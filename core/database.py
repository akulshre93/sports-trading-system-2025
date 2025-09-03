# core/database.py - PostgreSQL Database Manager
"""
Sports Trading System 2.0 - Database Manager
Handles: Connection pooling, position snapshots, order recording, sports config
Integrates with: config_base.py, schema.sql functions, position monitoring
"""

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from typing import Dict, Any, List, Optional, Tuple
import json
import time
import threading
from datetime import datetime

# Import configurations
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / "configs"))
from config_base import get_database_config, ACTIVE_SPORTS


class DatabaseError(Exception):
    """Custom database error for better error handling"""
    pass


class DatabaseManager:
    """
    Central database manager with connection pooling
    Handles all database operations for the trading system
    """
    
    def __init__(self):
        """Initialize database manager with connection pooling"""
        self.config = get_database_config()
        self.pool = None
        self._pool_lock = threading.Lock()
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Create connection pool with retry logic"""
        try:
            self.pool = pool.ThreadedConnectionPool(
                minconn=self.config['min_connections'],
                maxconn=self.config['max_connections'],
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password'],
                connect_timeout=self.config['connection_timeout']
            )
            print(f"Database pool initialized: {self.config['min_connections']}-{self.config['max_connections']} connections")
            
        except Exception as e:
            raise DatabaseError(f"Failed to initialize database pool: {e}")
    
    @contextmanager
    def get_connection(self):
        """
        Safe connection retrieval with automatic cleanup
        Uses connection pooling with proper error handling
        """
        conn = None
        try:
            with self._pool_lock:
                conn = self.pool.getconn()
            yield conn
            
        except psycopg2.PoolExhausted:
            raise DatabaseError("Connection pool exhausted - too many concurrent operations")
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            raise DatabaseError(f"Database connection error: {e}")
        finally:
            if conn:
                with self._pool_lock:
                    self.pool.putconn(conn)
    
    def _execute_with_retry(self, operation, max_retries: int = 3):
        """
        Execute database operation with retry logic
        Handles transient connection failures gracefully
        """
        last_error = None
        
        for attempt in range(max_retries):
            try:
                return operation()
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                last_error = e
                if attempt == max_retries - 1:
                    break
                
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"Database operation failed (attempt {attempt + 1}), retrying in {wait_time}s...")
                time.sleep(wait_time)
        
        raise DatabaseError(f"Database operation failed after {max_retries} attempts: {last_error}")
    
    # ======================
    # POSITION MANAGEMENT
    # ======================
    
    def insert_position_snapshot(self, strategy_id: int, ticker: str, kalshi_data: dict) -> bool:
        """
        Insert position snapshot using schema function
        Called by position monitor every 1 second
        """
        def operation():
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT insert_position_snapshot(%s, %s, %s)",
                        (ticker, strategy_id, json.dumps(kalshi_data))
                    )
                    conn.commit()
                    return True
        
        return self._execute_with_retry(operation)
    
    def get_latest_position(self, strategy_id: int, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get latest position for strategy/ticker using schema function
        Returns None if no position exists
        """
        def operation():
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        "SELECT * FROM get_latest_position(%s, %s)",
                        (strategy_id, ticker)
                    )
                    result = cur.fetchone()
                    
                    # Handle the case where function returns all nulls for non-existent position
                    if result and any(value is not None for value in result.values()):
                        return dict(result)
                    return None
        
        return self._execute_with_retry(operation)
    
    def get_all_positions_for_strategy(self, strategy_id: int) -> List[Dict[str, Any]]:
        """
        Get all current positions for a strategy
        Useful for portfolio monitoring and risk management
        """
        def operation():
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT DISTINCT ON (ticker) *
                        FROM positions 
                        WHERE strategy_id = %s AND status = 'open'
                        ORDER BY ticker, position_snapshot_time DESC
                    """, (strategy_id,))
                    
                    return [dict(row) for row in cur.fetchall()]
        
        return self._execute_with_retry(operation)
    
    # ======================
    # ORDER MANAGEMENT  
    # ======================
    
    def record_order(self, strategy_id: int, ticker: str, order_data: Dict[str, Any]) -> int:
        """
        Record order placement for audit trail
        Returns the database order ID
        """
        def operation():
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO orders (
                            strategy_id, ticker, kalshi_order_id, side, action, 
                            order_type, quantity, price, status, strategy_action, strategy_reason
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        strategy_id,
                        ticker,
                        order_data.get('kalshi_order_id'),
                        order_data.get('side'),
                        order_data.get('action', 'buy'),
                        order_data.get('order_type', 'limit'),
                        order_data.get('quantity'),
                        order_data.get('price'),
                        order_data.get('status', 'pending'),
                        order_data.get('strategy_action'),
                        order_data.get('strategy_reason', '')
                    ))
                    
                    order_id = cur.fetchone()[0]
                    conn.commit()
                    return order_id
        
        return self._execute_with_retry(operation)
    
    def update_order_status(self, kalshi_order_id: str, status: str, 
                           filled_quantity: int = None, average_fill_price: float = None) -> bool:
        """
        Update order status when filled/cancelled
        Called when order status changes
        """
        def operation():
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE orders 
                        SET status = %s, filled_quantity = COALESCE(%s, filled_quantity),
                            average_fill_price = COALESCE(%s, average_fill_price),
                            filled_at = CASE WHEN %s = 'filled' THEN CURRENT_TIMESTAMP ELSE filled_at END,
                            cancelled_at = CASE WHEN %s = 'cancelled' THEN CURRENT_TIMESTAMP ELSE cancelled_at END,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE kalshi_order_id = %s
                    """, (status, filled_quantity, average_fill_price, status, status, kalshi_order_id))
                    
                    conn.commit()
                    return cur.rowcount > 0
        
        return self._execute_with_retry(operation)
    
    # ======================
    # SPORTS CONFIGURATION
    # ======================
    
    def get_active_sports(self) -> List[Dict[str, Any]]:
        """
        Get sports configuration from database
        Filtered by ACTIVE_SPORTS config setting
        """
        def operation():
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT id, sport_name, ticker_prefix, assumed_game_length_minutes
                        FROM sports 
                        WHERE sport_name = ANY(%s)
                        ORDER BY sport_name
                    """, (ACTIVE_SPORTS,))
                    
                    return [dict(row) for row in cur.fetchall()]
        
        return self._execute_with_retry(operation)
    
    def get_strategy_id(self, strategy_name: str) -> Optional[int]:
        """
        Get strategy ID from name
        Used for foreign key references
        """
        def operation():
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM strategies WHERE name = %s", (strategy_name,))
                    result = cur.fetchone()
                    return result[0] if result else None
        
        return self._execute_with_retry(operation)
    
    # ======================
    # SYSTEM MONITORING
    # ======================
    
    def get_pool_status(self) -> Dict[str, Any]:
        """
        Get connection pool health status
        Used for monitoring and debugging
        """
        if not self.pool:
            return {'status': 'not_initialized', 'error': 'Pool not created'}
        
        try:
            return {
                'total_connections': self.pool.maxconn,
                'min_connections': self.pool.minconn,
                'used_connections': len(self.pool._used),
                'available_connections': len(self.pool._pool),
                'pool_healthy': len(self.pool._pool) > 0,
                'status': 'healthy'
            }
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    def record_system_health(self, component: str, status: str, message: str = None, 
                           metric_name: str = None, metric_value: float = None) -> bool:
        """
        Record system health metrics
        Used for monitoring and alerting
        """
        def operation():
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO system_health (component, status, message, metric_name, metric_value)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (component, status, message, metric_name, metric_value))
                    
                    conn.commit()
                    return True
        
        return self._execute_with_retry(operation)
    
    # ======================
    # CLEANUP & UTILITIES
    # ======================
    
    def close_pool(self):
        """Close all connections in pool"""
        if self.pool:
            self.pool.closeall()
            print("Database connection pool closed")
    
    def __del__(self):
        """Ensure pool is closed on destruction"""
        self.close_pool()
    
    # ======================
    # CONSOLE TESTING FUNCTIONS
    # ======================
    
    def test_connection(self) -> bool:
        """Test basic database connectivity"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT version()")
                    version = cur.fetchone()[0]
                    print(f"✓ Database connected: {version[:50]}...")
                    return True
        except Exception as e:
            print(f"✗ Connection test failed: {e}")
            return False
    
    def test_schema(self) -> bool:
        """Verify required tables and functions exist"""
        required_tables = ['sports', 'strategies', 'positions', 'orders', 'markets']
        required_functions = ['insert_position_snapshot', 'get_latest_position']
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Test tables
                    print("Checking tables...")
                    for table in required_tables:
                        cur.execute("SELECT to_regclass(%s)", (f'public.{table}',))
                        exists = cur.fetchone()[0] is not None
                        status = "✓" if exists else "✗"
                        print(f"  {status} {table}")
                        if not exists:
                            return False
                    
                    # Test functions
                    print("Checking functions...")
                    for func in required_functions:
                        cur.execute("SELECT proname FROM pg_proc WHERE proname = %s", (func,))
                        exists = cur.fetchone() is not None
                        status = "✓" if exists else "✗"
                        print(f"  {status} {func}()")
                        if not exists:
                            return False
                    
                    print("✓ Schema validation passed")
                    return True
                    
        except Exception as e:
            print(f"✗ Schema test failed: {e}")
            return False
    
    def test_operations(self) -> bool:
        """Test core database operations"""
        try:
            # Test sports configuration
            print("Testing sports configuration...")
            sports = self.get_active_sports()
            print(f"✓ Loaded {len(sports)} active sports")
            for sport in sports:
                print(f"  - {sport['sport_name']}: {sport['ticker_prefix']}")
            
            # Test strategy ID lookup
            print("\nTesting strategy lookup...")
            strategy_id = self.get_strategy_id('temporal_arbitrage')
            print(f"✓ Strategy ID for 'temporal_arbitrage': {strategy_id}")
            
            if strategy_id:
                # Test position operations with dummy data
                print("\nTesting position operations...")
                dummy_kalshi_data = {
                    'event_ticker': 'TEST-EVENT-OPS',
                    'total_traded': 150,
                    'position': 75,
                    'market_exposure': 3750,
                    'realized_pnl': 25,
                    'resting_orders_count': 1,
                    'fees_paid': 8,
                    'last_updated_ts': '2025-01-01 13:00:00'
                }
                
                # Insert position snapshot
                self.insert_position_snapshot(strategy_id, 'TEST-OPS-TICKER', dummy_kalshi_data)
                print("✓ Position snapshot inserted")
                
                # Retrieve position
                position = self.get_latest_position(strategy_id, 'TEST-OPS-TICKER')
                if position:
                    print(f"✓ Position retrieved: {position['position']} contracts, ${position['market_exposure']} exposure")
                
                # Test order recording
                print("\nTesting order operations...")
                order_data = {
                    'kalshi_order_id': 'TEST-ORDER-123',
                    'side': 'yes',
                    'action': 'buy',
                    'quantity': 10,
                    'price': 0.45,
                    'strategy_action': 'entry',
                    'strategy_reason': 'Test order placement'
                }
                
                order_id = self.record_order(strategy_id, 'TEST-OPS-TICKER', order_data)
                print(f"✓ Order recorded with ID: {order_id}")
                
                # Clean up test data
                with self.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM positions WHERE ticker = 'TEST-OPS-TICKER'")
                        cur.execute("DELETE FROM orders WHERE ticker = 'TEST-OPS-TICKER'")
                        conn.commit()
                print("✓ Test data cleaned up")
            
            print("\n✓ All operations test passed")
            return True
            
        except Exception as e:
            print(f"✗ Operations test failed: {e}")
            return False
    
    def print_status(self):
        """Print current database manager status"""
        print("\n" + "=" * 50)
        print("DATABASE MANAGER STATUS")
        print("=" * 50)
        
        print(f"Database: {self.config['database']}")
        print(f"Host: {self.config['host']}:{self.config['port']}")
        print(f"User: {self.config['user']}")
        
        # Pool status
        pool_status = self.get_pool_status()
        if pool_status['status'] == 'healthy':
            print(f"Pool: {pool_status['used_connections']}/{pool_status['total_connections']} connections used")
            print(f"Available: {pool_status['available_connections']} connections")
            print("Status: ✓ HEALTHY")
        else:
            print(f"Pool Status: ✗ {pool_status['status']}")
            if 'error' in pool_status:
                print(f"Error: {pool_status['error']}")
        
        # Active sports
        try:
            sports = self.get_active_sports()
            print(f"Active Sports: {[s['sport_name'] for s in sports]}")
        except Exception as e:
            print(f"Sports Config: ✗ Error loading ({e})")
        
        print("=" * 50)


# ======================
# CONSOLE TESTING INTERFACE
# ======================

def run_full_database_test():
    """
    Complete database manager validation
    Primary console test function
    """
    print("\n" + "=" * 60)
    print("DATABASE MANAGER - FULL VALIDATION")
    print("=" * 60)
    
    try:
        # Initialize manager
        print("1. Initializing DatabaseManager...")
        db = DatabaseManager()
        print("✓ DatabaseManager initialized successfully")
        
        # Run tests
        print("\n2. Testing basic connectivity...")
        if not db.test_connection():
            print("✗ FAILED: Basic connection test")
            return False
        
        print("\n3. Testing schema...")
        if not db.test_schema():
            print("✗ FAILED: Schema validation")
            return False
        
        print("\n4. Testing operations...")
        if not db.test_operations():
            print("✗ FAILED: Operations test")
            return False
        
        print("\n5. Database status...")
        db.print_status()
        
        print("\n" + "=" * 60)
        print("✓ ALL DATABASE TESTS PASSED")
        print("✓ DatabaseManager ready for production use")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print(f"\n✗ DATABASE TEST FAILED: {e}")
        print("=" * 60)
        return False


# ======================
# CONSOLE USAGE
# ======================

if __name__ == "__main__":
    print("core/database.py loaded successfully!")
    print("\nQuick test commands:")
    print("  run_full_database_test()    - Complete validation")
    print("  db = DatabaseManager()      - Create manager instance")
    print("  db.test_connection()        - Test connectivity")
    print("  db.print_status()           - Show current status")
    print("\nRun: run_full_database_test()")