"""
SPORTS TRADING SYSTEM 2.0 - POSITION MONITOR
Centralized 1-second position polling service

Architecture:
- Single background thread polls Kalshi API every 1 second
- Stores position snapshots in PostgreSQL database
- Strategies read positions from database (not API directly)
- Error resilient with comprehensive logging
"""

import threading
import time
from datetime import datetime
from typing import Dict, List, Optional
from cryptography.hazmat.primitives import serialization

# FIXED VERSION:
from core.api_client import ExchangeClient
from core.database import DatabaseManager
from configs.config_base import get_kalshi_config, get_system_config
import logging

class PositionMonitor:
    """Centralized position monitoring service with 1-second polling"""
    
    def __init__(self):
        # Initialize components
        self.logger = logging.getLogger('position_monitor')
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(handler)
        self.config = get_system_config()
        
        # API client initialization
        self._init_api_client()
        
        # Database manager
        self.database = DatabaseManager()
        
        # Threading control
        self.is_running = False
        self.monitor_thread = None
        
        # Configuration - Override to 1 second for real-time monitoring
        self.polling_interval = 1.0  # Force 1-second for temporal arbitrage needs
        
        # Statistics
        self.poll_count = 0
        self.error_count = 0
        self.last_poll_time = None
        self.last_position_count = 0
        
        self.logger.info("Position monitor initialized")
    
    def _init_api_client(self):
        """Initialize API client with proper key loading"""
        try:
            kalshi_config = get_kalshi_config()
            
            # Load private key from file
            with open(kalshi_config['private_key_path'], 'rb') as f:
                private_key = serialization.load_pem_private_key(
                    f.read(),
                    password=None
                )
            
            # Initialize API client
            self.api_client = ExchangeClient(
                exchange_api_base=kalshi_config['api_host'],
                key_id=kalshi_config['key_id'],
                private_key=private_key
            )
            
            self.logger.info("API client initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize API client: {e}")
            raise
    
    def start_monitoring(self):
        """Start background position monitoring"""
        if self.is_running:
            self.logger.warning("Position monitoring already running")
            return
        
        self.is_running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        
        self.logger.info(f"Position monitoring started - {self.polling_interval}s polling interval")
    
    def stop_monitoring(self):
        """Gracefully stop position monitoring"""
        if not self.is_running:
            self.logger.warning("Position monitoring not running")
            return
        
        self.logger.info("Stopping position monitoring...")
        self.is_running = False
        
        if self.monitor_thread:
            self.monitor_thread.join(timeout=3.0)
            
        self.logger.info(f"Position monitoring stopped. Final stats: {self.poll_count} polls, {self.error_count} errors")
    
    def _monitor_loop(self):
        """Main monitoring loop - runs in background thread"""
        self.logger.info("Position monitoring loop started")
        
        while self.is_running:
            start_time = time.time()
            
            try:
                # Poll positions from Kalshi API
                self._poll_positions()
                
                self.poll_count += 1
                self.last_poll_time = datetime.now()
                
                # Log progress every minute
                if self.poll_count % 60 == 0:
                    self.logger.info(
                        f"Position monitoring: {self.poll_count} polls completed, "
                        f"{self.last_position_count} positions tracked, "
                        f"{self.error_count} errors"
                    )
                
            except Exception as e:
                self.error_count += 1
                self.logger.error(f"Position poll failed (#{self.error_count}): {e}")
                
                # If too many consecutive errors, log warning but continue
                if self.error_count % 10 == 0:
                    self.logger.warning(f"High error count: {self.error_count} errors total")
            
            # Maintain precise polling interval
            elapsed = time.time() - start_time
            sleep_time = max(0, self.polling_interval - elapsed)
            time.sleep(sleep_time)
        
        self.logger.info("Position monitoring loop ended")
    
    def _poll_positions(self):
        """Single position polling cycle"""
        # Get positions from Kalshi API
        positions_response = self.api_client.get_positions()
        
        if not positions_response:
            self.logger.warning("No positions response from API")
            return
        
        # Extract market positions (the granular ticker-level data)
        market_positions = positions_response.get('market_positions', [])
        
        # Filter for active positions (non-zero quantities)
        active_positions = [pos for pos in market_positions if pos.get('position', 0) != 0]
        
        self.last_position_count = len(active_positions)
        
        # Log detailed info every 10 polls or when positions change
        if self.poll_count % 10 == 0 or len(active_positions) != getattr(self, '_last_logged_count', 0):
            self.logger.debug(f"Poll #{self.poll_count}: {len(active_positions)} active positions")
            self._last_logged_count = len(active_positions)
        
        # Process each active position
        for position in active_positions:
            self._process_position(position)
    
    def _process_position(self, position_data):
        """Process single position and store in database"""
        try:
            ticker = position_data.get('ticker', '')
            quantity = position_data.get('position', 0)
            
            if not ticker:
                self.logger.warning("Position missing ticker, skipping")
                return
            
            # Determine strategy ID (simplified approach for now)
            strategy_id = self._determine_strategy_id(ticker)
            
            # Store position snapshot in database
            success = self.database.insert_position_snapshot(
                strategy_id=strategy_id,
                ticker=ticker,
                kalshi_data=position_data
            )
            
            if success:
                self.logger.debug(f"Position snapshot: {ticker} qty={quantity} strategy={strategy_id}")
            else:
                self.logger.warning(f"Failed to store position snapshot: {ticker}")
                
        except Exception as e:
            self.logger.error(f"Error processing position {position_data}: {e}")
    
    def _determine_strategy_id(self, ticker):
        """Determine which strategy owns this position"""
        # Simple heuristic for now - can be enhanced later
        # Strategy ID 1 = temporal_arbitrage
        # Strategy ID 2 = fat_tail_statistical
        
        # For baseball games, default to temporal arbitrage
        if 'KXMLBGAME' in ticker:
            return 1
        else:
            return 2
    
    def get_monitoring_status(self):
        """Get current monitoring status"""
        return {
            'is_running': self.is_running,
            'polling_interval': self.polling_interval,
            'poll_count': self.poll_count,
            'error_count': self.error_count,
            'last_poll_time': self.last_poll_time,
            'last_position_count': self.last_position_count,
            'thread_alive': self.monitor_thread.is_alive() if self.monitor_thread else False
        }


# =============================================================================
# CONSOLE TESTING FUNCTIONS
# =============================================================================

def test_component_integration():
    """Test that all dependencies work together"""
    print("\n" + "="*60)
    print("TESTING: Component integration")
    print("="*60)
    
    try:
        monitor = PositionMonitor()
        
        # Test API client
        positions = monitor.api_client.get_positions()
        if positions and 'market_positions' in positions:
            print("✅ API client initialized successfully")
        else:
            print("❌ API client failed to get positions")
            return False
        
        # Test database
        from datetime import datetime
        test_data = {
            'ticker': 'TEST-INTEGRATION',
            'position': 99,
            'market_exposure': 1000,
            'timestamp': datetime.now().isoformat()
        }
        
        success = monitor.database.insert_position_snapshot(1, 'TEST-INTEGRATION', test_data)
        if success:
            print("✅ Database manager initialized successfully")
        else:
            print("❌ Database manager failed")
            return False
        
        # Test logger
        monitor.logger.info("Test log message")
        print("✅ Logger initialized successfully")
        
        print("✅ Configuration loaded successfully")
        return True
        
    except Exception as e:
        print(f"❌ Component integration test failed: {e}")
        return False


def test_single_poll():
    """Test one position polling cycle"""
    print("\n" + "="*60)
    print("TESTING: Single position poll")
    print("="*60)
    
    try:
        monitor = PositionMonitor()
        
        print("Executing single poll...")
        monitor._poll_positions()
        
        print(f"✅ Single poll completed")
        print(f"   Last position count: {monitor.last_position_count}")
        print(f"   Poll count: {monitor.poll_count}")
        print(f"   Error count: {monitor.error_count}")
        
        return True
        
    except Exception as e:
        print(f"❌ Single poll test failed: {e}")
        return False


def test_data_flow():
    """Test complete data flow: API → Database"""
    print("\n" + "="*60)
    print("TESTING: Data flow validation")
    print("="*60)
    
    try:
        monitor = PositionMonitor()
        
        # Get positions from API
        positions = monitor.api_client.get_positions()
        market_positions = positions.get('market_positions', [])
        active_positions = [pos for pos in market_positions if pos.get('position', 0) != 0]
        
        print(f"API returned {len(active_positions)} active positions")
        
        if active_positions:
            # Process first position
            test_position = active_positions[0]
            ticker = test_position.get('ticker')
            quantity = test_position.get('position')
            
            print(f"Processing: {ticker} qty={quantity}")
            
            # Store in database
            monitor._process_position(test_position)
            
            # Verify in database
            strategy_id = monitor._determine_strategy_id(ticker)
            db_position = monitor.database.get_latest_position(strategy_id, ticker)
            if db_position and db_position.get('position') == quantity:
                print("✅ Data flow validation passed - API → Database working")
                return True
            else:
                print("❌ Data flow validation failed - database mismatch")
                return False
        else:
            print("⚠️  No active positions to test data flow")
            return True
        
    except Exception as e:
        print(f"❌ Data flow test failed: {e}")
        return False


def test_polling_timing(test_duration=5):
    """Test 1-second polling timing precision"""
    print("\n" + "="*60)
    print(f"TESTING: Polling timing for {test_duration} seconds")
    print("="*60)
    
    try:
        monitor = PositionMonitor()
        
        start_times = []
        intervals = []
        
        print("Starting timed polling test...")
        
        for i in range(test_duration):
            start = time.time()
            start_times.append(datetime.now())
            
            # Simulate position poll work
            monitor._poll_positions()
            
            # Calculate timing
            if i > 0:
                interval = (start_times[-1] - start_times[-2]).total_seconds()
                intervals.append(interval)
                print(f"Poll {i+1}: {interval:.3f}s")
            else:
                print(f"Poll {i+1}: Initial")
            
            # Maintain 1-second interval
            elapsed = time.time() - start
            sleep_time = max(0, 1.0 - elapsed)
            time.sleep(sleep_time)
        
        if intervals:
            avg_interval = sum(intervals) / len(intervals)
            print(f"\nAverage interval: {avg_interval:.3f}s (target: 1.000s)")
            
            # Check if timing is reasonable (within 50ms tolerance)
            if 0.95 <= avg_interval <= 1.05:
                print("✅ Timing test passed - consistent 1-second polling")
                return True
            else:
                print("❌ Timing test failed - intervals inconsistent")
                return False
        else:
            print("❌ No intervals to analyze")
            return False
            
    except Exception as e:
        print(f"❌ Timing test failed: {e}")
        return False


def test_background_monitoring(test_duration=3):
    """Test background monitoring thread"""
    print("\n" + "="*60)
    print(f"TESTING: Background monitoring for {test_duration} seconds")
    print("="*60)
    
    try:
        monitor = PositionMonitor()
        
        # Start monitoring
        print("Starting background monitoring...")
        monitor.start_monitoring()
        
        # Check initial status
        status = monitor.get_monitoring_status()
        print(f"Thread running: {status['is_running']}, Poll count: {status['poll_count']}")
        
        # Wait and check progress
        time.sleep(test_duration)
        
        status = monitor.get_monitoring_status()
        print(f"After {test_duration}s - Thread running: {status['is_running']}, Poll count: {status['poll_count']}")
        
        # Stop monitoring
        monitor.stop_monitoring()
        
        final_status = monitor.get_monitoring_status()
        print(f"Thread stopped: {not final_status['is_running']}, Final poll count: {final_status['poll_count']}")
        
        if final_status['poll_count'] >= test_duration - 1:  # Allow for timing variation
            print("✅ Background monitoring test passed")
            return True
        else:
            print(f"❌ Background monitoring test failed - insufficient polls")
            return False
            
    except Exception as e:
        print(f"❌ Background monitoring test failed: {e}")
        return False


def test_error_handling():
    """Test error handling and resilience"""
    print("\n" + "="*60)
    print("TESTING: Error handling and resilience")
    print("="*60)
    
    try:
        monitor = PositionMonitor()
        
        print("Testing position processing error handling...")
        
        # Test with malformed position data
        bad_position = {'invalid': 'data'}
        monitor._process_position(bad_position)
        print("✅ Handled malformed position data without crashing")
        
        # Test strategy ID determination
        strategy_id = monitor._determine_strategy_id('KXMLBGAME-TEST')
        print(f"✅ Strategy ID determination working: {strategy_id}")
        
        # Test monitoring status retrieval
        status = monitor.get_monitoring_status()
        print(f"✅ Status retrieval working: {len(status)} fields")
        
        return True
        
    except Exception as e:
        print(f"❌ Error handling test failed: {e}")
        return False


def print_monitor_status():
    """Print current position monitor status"""
    print("\n" + "="*60)
    print("POSITION MONITOR STATUS")
    print("="*60)
    
    try:
        monitor = PositionMonitor()
        status = monitor.get_monitoring_status()
        
        print(f"Running: {status['is_running']}")
        print(f"Polling Interval: {status['polling_interval']} seconds")
        print(f"Total Polls: {status['poll_count']}")
        print(f"Total Errors: {status['error_count']}")
        print(f"Last Position Count: {status['last_position_count']}")
        print(f"Last Poll: {status['last_poll_time']}")
        print(f"Thread Status: {'Alive' if status['thread_alive'] else 'Stopped'}")
        
        # Show current positions from API
        positions = monitor.api_client.get_positions()
        market_positions = positions.get('market_positions', []) if positions else []
        active_positions = [pos for pos in market_positions if pos.get('position', 0) != 0]
        
        print(f"\nCurrent Active Positions: {len(active_positions)}")
        for pos in active_positions[:5]:  # Show first 5
            ticker = pos.get('ticker', 'Unknown')
            quantity = pos.get('position', 0)
            exposure = pos.get('market_exposure', 0)
            print(f"  {ticker}: qty={quantity}, exposure={exposure}")
            
    except Exception as e:
        print(f"❌ Status check failed: {e}")


def run_full_position_monitor_test():
    """Complete validation of position monitoring system"""
    print("\n" + "="*80)
    print("COMPREHENSIVE POSITION MONITOR TEST")
    print("="*80)
    
    test_results = []
    
    # Run all tests
    test_results.append(("Component Integration", test_component_integration()))
    test_results.append(("Single Poll", test_single_poll()))
    test_results.append(("Data Flow", test_data_flow()))
    test_results.append(("Timing Precision", test_polling_timing(3)))
    test_results.append(("Background Threading", test_background_monitoring(3)))
    test_results.append(("Error Handling", test_error_handling()))
    
    # Print summary
    print("\n" + "="*60)
    print("TEST RESULTS SUMMARY")
    print("="*60)
    
    passed = 0
    for test_name, result in test_results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name:>20}: {status}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{len(test_results)} tests passed")
    
    # Print final status
    print_monitor_status()
    
    return passed == len(test_results)


# Usage example
if __name__ == "__main__":
    print("SPORTS TRADING SYSTEM 2.0 - POSITION MONITOR")
    print("=" * 60)
    print("\nUsage Examples:")
    print("\nfrom core.position_monitor import PositionMonitor")
    print("\n# Create and start monitoring")
    print("monitor = PositionMonitor()")
    print("monitor.start_monitoring()  # Begins 1-second background polling")
    print("\n# Check status")
    print("status = monitor.get_monitoring_status()")
    print("\n# Stop monitoring")
    print("monitor.stop_monitoring()")
    print("\n" + "="*60)
    
    # Run comprehensive test
    run_full_position_monitor_test()