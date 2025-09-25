# market_data/market_cache.py - Fast In-Memory Market Data Cache
"""
Sports Trading System 2.0 - Market Data Cache
Handles: Thread-safe price storage, sub-millisecond lookups, strategy data access
Integrates with: DatabaseManager, MarketDataCollector, strategy modules
"""

import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict, deque
import statistics

# Import core components
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from core.database import DatabaseManager
from configs.config_base import get_system_config


class MarketCacheError(Exception):
    """Custom exception for market cache errors"""
    pass


class MarketPrice:
    """
    Individual market price data structure
    Optimized for fast access and momentum calculations
    """
    
    def __init__(self, ticker: str):
        self.ticker = ticker
        self.current_price = None
        self.last_updated = None
        self.price_history = deque(maxlen=50)  # Last 50 price snapshots
        self.is_active = False
        self.consecutive_failures = 0
        
        # Cached calculations for performance
        self._cached_momentum = None
        self._cached_momentum_time = None
        self._cached_spread = None
        self._cached_spread_time = None
    
    def update_price(self, price_data: Dict[str, Any]):
        """Update current price and add to history"""
        try:
            # Store current price
            self.current_price = {
                'ticker': price_data['ticker'],
                'timestamp': price_data['timestamp'],
                'yes_bid': price_data.get('yes_bid'),
                'yes_ask': price_data.get('yes_ask'),
                'no_bid': price_data.get('no_bid'), 
                'no_ask': price_data.get('no_ask'),
                'yes_volume': price_data.get('yes_volume', 0),
                'no_volume': price_data.get('no_volume', 0),
                'total_volume': price_data.get('total_volume', 0),
                'relative_min_to_game_start': price_data.get('relative_min_to_game_start')
            }
            
            # Add to history
            self.price_history.append(self.current_price.copy())
            
            # Update metadata
            self.last_updated = price_data['timestamp']
            self.is_active = True
            self.consecutive_failures = 0
            
            # Clear cached calculations
            self._cached_momentum = None
            self._cached_spread = None
            
        except Exception as e:
            raise MarketCacheError(f"Failed to update price for {self.ticker}: {e}")
    
    def get_mid_price(self, side: str = 'yes') -> Optional[float]:
        """Get mid-price for yes or no side - FIXED: returns float"""
        if not self.current_price:
            return None
        
        if side == 'yes':
            bid = self.current_price.get('yes_bid')
            ask = self.current_price.get('yes_ask')
        else:
            bid = self.current_price.get('no_bid')
            ask = self.current_price.get('no_ask')
        
        if bid is not None and ask is not None:
            return float((bid + ask) / 2 / 100)  # FIXED: explicit float conversion
        elif bid is not None:
            return float(bid / 100)  # FIXED: explicit float conversion
        elif ask is not None:
            return float(ask / 100)  # FIXED: explicit float conversion
        else:
            return None
    
    def calculate_momentum(self, lookback_seconds: int = 60) -> Optional[float]:
        """
        Calculate price momentum over specified time period
        Returns change in mid-price as percentage
        """
        # Check cache (valid for 5 seconds)
        now = time.time()
        if (self._cached_momentum is not None and 
            self._cached_momentum_time and 
            now - self._cached_momentum_time < 5):
            return self._cached_momentum
        
        if len(self.price_history) < 2:
            return None
        
        current_time = self.current_price['timestamp']
        cutoff_time = current_time - timedelta(seconds=lookback_seconds)
        
        # Find prices within lookback period
        recent_prices = [
            p for p in self.price_history 
            if p['timestamp'] >= cutoff_time
        ]
        
        if len(recent_prices) < 2:
            return None
        
        # Calculate momentum using mid-prices
        first_price = self._calculate_mid_price(recent_prices[0])
        last_price = self._calculate_mid_price(recent_prices[-1])
        
        if first_price and last_price and first_price > 0:
            momentum = ((last_price - first_price) / first_price) * 100
            
            # Cache the result
            self._cached_momentum = momentum
            self._cached_momentum_time = now
            
            return momentum
        
        return None
    
    def _calculate_mid_price(self, price_data: Dict) -> Optional[float]:
        """Helper to calculate mid-price from price data"""
        yes_bid = price_data.get('yes_bid')
        yes_ask = price_data.get('yes_ask')
        
        if yes_bid is not None and yes_ask is not None:
            return (yes_bid + yes_ask) / 2 / 100
        elif yes_bid is not None:
            return yes_bid / 100
        elif yes_ask is not None:
            return yes_ask / 100
        else:
            return None
    
    def get_spread(self, side: str = 'yes') -> Optional[float]:
        """Get bid-ask spread for specified side"""
        # Check cache
        now = time.time()
        if (self._cached_spread is not None and 
            self._cached_spread_time and 
            now - self._cached_spread_time < 5):
            return self._cached_spread
        
        if not self.current_price:
            return None
        
        if side == 'yes':
            bid = self.current_price.get('yes_bid')
            ask = self.current_price.get('yes_ask')
        else:
            bid = self.current_price.get('no_bid')
            ask = self.current_price.get('no_ask')
        
        if bid is not None and ask is not None and ask > bid:
            spread = (ask - bid) / 100  # Convert to dollars
            
            # Cache the result
            self._cached_spread = spread
            self._cached_spread_time = now
            
            return spread
        
        return None
    
    def get_volume_info(self) -> Dict[str, int]:
        """Get current volume information"""
        if not self.current_price:
            return {'yes_volume': 0, 'no_volume': 0, 'total_volume': 0}
        
        return {
            'yes_volume': self.current_price.get('yes_volume', 0),
            'no_volume': self.current_price.get('no_volume', 0),
            'total_volume': self.current_price.get('total_volume', 0)
        }
    
    def is_stale(self, max_age_seconds: int = 300) -> bool:
        """Check if price data is stale (older than max_age)"""
        if not self.last_updated:
            return True
        
        # Handle timezone-naive timestamps from database
        if self.last_updated.tzinfo is None:
            # Database timestamp is naive, make it UTC-aware
            last_updated_utc = self.last_updated.replace(tzinfo=timezone.utc)
        else:
            last_updated_utc = self.last_updated
        
        age = (datetime.now(timezone.utc) - last_updated_utc).total_seconds()
        return age > max_age_seconds


class MarketCache:
    """
    High-performance in-memory cache for market data
    Provides sub-millisecond price lookups for trading strategies
    """
    
    def __init__(self):
        """Initialize market cache with thread safety"""
        
        try:
            from core.logger import get_component_logger
            self.logger = get_component_logger('market_data')
        except ImportError:
            import logging
            self.logger = logging.getLogger('market_data')
        
        self.database = DatabaseManager()
        self.system_config = get_system_config()
        
        # Thread-safe market data storage
        self._markets: Dict[str, MarketPrice] = {}
        self._markets_lock = threading.RWLock() if hasattr(threading, 'RWLock') else threading.Lock()
        
        # Update control
        self._update_thread = None
        self._update_active = False
        self._shutdown_event = threading.Event()
        
        # Performance tracking
        self.stats = {
            'cache_hits': 0,
            'cache_misses': 0,
            'last_update_time': None,
            'update_count': 0,
            'active_markets_count': 0,
            'stale_markets_count': 0
        }
        
        self.logger.info("MarketCache initialized")
    
    # ======================
    # CORE CACHE OPERATIONS
    # ======================
    
    def get_current_price(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get current price data for ticker
        Optimized for sub-millisecond response time
        """
        try:
            with self._markets_lock:
                market = self._markets.get(ticker)
                if market and market.current_price:
                    self.stats['cache_hits'] += 1
                    return market.current_price.copy()
                else:
                    self.stats['cache_misses'] += 1
                    return None
                    
        except Exception as e:
            self.logger.error(f"Failed to get price for {ticker}: {e}")
            return None
    
    def get_mid_price(self, ticker: str, side: str = 'yes') -> Optional[float]:
        """Get mid-price for ticker/side combination - FIXED: returns float"""
        try:
            with self._markets_lock:
                market = self._markets.get(ticker)
                if market:
                    self.stats['cache_hits'] += 1
                    mid_price = market.get_mid_price(side)
                    # FIXED: Convert Decimal to float consistently
                    return float(mid_price) if mid_price is not None else None
                else:
                    self.stats['cache_misses'] += 1
                    return None
                    
        except Exception as e:
            self.logger.error(f"Failed to get mid price for {ticker}: {e}")
            return None
                    
        except Exception as e:
            self.logger.error(f"Failed to get mid price for {ticker}: {e}")
            return None
    
    def get_price_history(self, ticker: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent price history for ticker"""
        try:
            with self._markets_lock:
                market = self._markets.get(ticker)
                if market:
                    self.stats['cache_hits'] += 1
                    return list(market.price_history)[-limit:]
                else:
                    self.stats['cache_misses'] += 1
                    return []
                    
        except Exception as e:
            self.logger.error(f"Failed to get price history for {ticker}: {e}")
            return []
    
    def get_momentum(self, ticker: str, lookback_seconds: int = 60) -> Optional[float]:
        """Get price momentum for ticker"""
        try:
            with self._markets_lock:
                market = self._markets.get(ticker)
                if market:
                    self.stats['cache_hits'] += 1
                    return market.calculate_momentum(lookback_seconds)
                else:
                    self.stats['cache_misses'] += 1
                    return None
                    
        except Exception as e:
            self.logger.error(f"Failed to get momentum for {ticker}: {e}")
            return None
    
    def is_market_active(self, ticker: str, max_age_seconds: int = 300) -> bool:
        """Check if market has recent price data"""
        try:
            with self._markets_lock:
                market = self._markets.get(ticker)
                if market:
                    return market.is_active and not market.is_stale(max_age_seconds)
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to check market status for {ticker}: {e}")
            return False
    
    def get_all_active_tickers(self) -> List[str]:
        """Get list of all active market tickers"""
        try:
            with self._markets_lock:
                return [
                    ticker for ticker, market in self._markets.items()
                    if market.is_active and not market.is_stale()
                ]
                
        except Exception as e:
            self.logger.error(f"Failed to get active tickers: {e}")
            return []
    
    # ======================
    # CACHE UPDATE OPERATIONS
    # ======================
    
    def update_cache(self) -> int:
        """
        Update cache from database
        Returns number of markets updated
        """
        try:
            # Get recent market data from database
            with self.database.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT DISTINCT ON (ticker) 
                            ticker, sport_id, yes_bid, yes_ask, no_bid, no_ask,
                            yes_volume, no_volume, total_volume, relative_min_to_game_start, timestamp
                        FROM market_data_history 
                        WHERE timestamp > NOW() - INTERVAL '1 hour'
                        ORDER BY ticker, timestamp DESC
                    """)
                    
                    recent_data = cur.fetchall()
            
            updated_count = 0
            
            with self._markets_lock:
                for row in recent_data:
                    ticker = row[0]
                    
                    # Create market if doesn't exist
                    if ticker not in self._markets:
                        self._markets[ticker] = MarketPrice(ticker)
                    
                    # Update market data
                    price_data = {
                        'ticker': row[0],
                        'sport_id': row[1],
                        'yes_bid': row[2],
                        'yes_ask': row[3],
                        'no_bid': row[4],
                        'no_ask': row[5],
                        'yes_volume': row[6] or 0,
                        'no_volume': row[7] or 0,
                        'total_volume': row[8] or 0,
                        'relative_min_to_game_start': row[9],
                        'timestamp': row[10].replace(tzinfo=timezone.utc) if row[10] and row[10].tzinfo is None else row[10]
                    }
                    
                    self._markets[ticker].update_price(price_data)
                    updated_count += 1
            
            # Update stats
            self.stats['last_update_time'] = datetime.now(timezone.utc)
            self.stats['update_count'] += 1
            self.stats['active_markets_count'] = len([
                m for m in self._markets.values() 
                if m.is_active and not m.is_stale()
            ])
            self.stats['stale_markets_count'] = len([
                m for m in self._markets.values() 
                if m.is_stale()
            ])
            
            self.logger.debug(f"Cache updated: {updated_count} markets, {self.stats['active_markets_count']} active")
            return updated_count
            
        except Exception as e:
            self.logger.error(f"Cache update failed: {e}")
            return 0
    
    def start_auto_update(self, interval_seconds: int = 5):
        """Start automatic cache updates"""
        if self._update_active:
            self.logger.warning("Auto-update already active")
            return
        
        try:
            # Initial cache load
            self.update_cache()
            
            # Start update thread
            self._update_active = True
            self._update_thread = threading.Thread(target=self._update_loop, args=(interval_seconds,), daemon=True)
            self._update_thread.start()
            
            self.logger.info(f"Auto-update started with {interval_seconds}s interval")
            
        except Exception as e:
            self.logger.error(f"Failed to start auto-update: {e}")
            self._update_active = False
    
    def stop_auto_update(self):
        """Stop automatic cache updates"""
        if not self._update_active:
            return
        
        self.logger.info("Stopping auto-update...")
        
        self._update_active = False
        self._shutdown_event.set()
        
        if self._update_thread and self._update_thread.is_alive():
            self._update_thread.join(timeout=10)
        
        self.logger.info("Auto-update stopped")
    
    def _update_loop(self, interval_seconds: int):
        """Background update loop"""
        self.logger.info(f"Starting auto-update loop ({interval_seconds}s interval)")
        
        while self._update_active and not self._shutdown_event.is_set():
            try:
                loop_start = time.time()
                
                # Update cache
                updated_count = self.update_cache()
                
                # Calculate next update time
                elapsed = time.time() - loop_start
                sleep_time = max(0, interval_seconds - elapsed)
                
                if updated_count > 0:
                    self.logger.debug(f"Updated {updated_count} markets in {elapsed:.2f}s")
                
                # Sleep until next update
                if sleep_time > 0:
                    if self._shutdown_event.wait(sleep_time):
                        break
                
            except Exception as e:
                self.logger.error(f"Update loop error: {e}")
                if self._shutdown_event.wait(interval_seconds):
                    break
        
        self.logger.info("Auto-update loop stopped")
    
    # ======================
    # MONITORING & DIAGNOSTICS
    # ======================
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics"""
        with self._markets_lock:
            total_markets = len(self._markets)
            
            # Calculate hit rate
            total_requests = self.stats['cache_hits'] + self.stats['cache_misses']
            hit_rate = (self.stats['cache_hits'] / total_requests * 100) if total_requests > 0 else 0
            
            return {
                'total_markets': total_markets,
                'active_markets': self.stats['active_markets_count'],
                'stale_markets': self.stats['stale_markets_count'],
                'cache_hit_rate': hit_rate,
                'total_requests': total_requests,
                'cache_hits': self.stats['cache_hits'],
                'cache_misses': self.stats['cache_misses'],
                'last_update': self.stats['last_update_time'],
                'update_count': self.stats['update_count'],
                'auto_update_active': self._update_active
            }
    
    def print_cache_status(self):
        """Print detailed cache status"""
        stats = self.get_cache_stats()
        
        print("\n" + "=" * 50)
        print("MARKET CACHE STATUS")
        print("=" * 50)
        
        print(f"Markets: {stats['total_markets']} total, {stats['active_markets']} active, {stats['stale_markets']} stale")
        print(f"Cache Hit Rate: {stats['cache_hit_rate']:.1f}% ({stats['cache_hits']}/{stats['total_requests']} requests)")
        print(f"Updates: {stats['update_count']} total")
        print(f"Auto-Update: {'ON' if stats['auto_update_active'] else 'OFF'}")
        
        if stats['last_update']:
            print(f"Last Update: {stats['last_update'].strftime('%Y-%m-%d %H:%M:%S UTC')}")
        
        # Show sample of active markets
        active_tickers = self.get_all_active_tickers()[:5]
        if active_tickers:
            print(f"Sample Active Markets:")
            for ticker in active_tickers:
                price = self.get_mid_price(ticker)
                momentum = self.get_momentum(ticker)
                print(f"  {ticker}: ${price:.3f}" + (f" ({momentum:+.1f}%)" if momentum else ""))
        
        print("=" * 50)
    
    def __del__(self):
        """Cleanup on destruction"""
        self.stop_auto_update()


# ======================
# CONSOLE TESTING INTERFACE
# ======================

def run_cache_initialization_test():
    """Test cache initialization and basic functionality"""
    print("\n" + "=" * 60)
    print("MARKET CACHE - INITIALIZATION TEST")
    print("=" * 60)
    
    try:
        cache = MarketCache()
        print("✓ Cache initialized")
        
        # Test initial update
        print("\nTesting cache update...")
        updated_count = cache.update_cache()
        print(f"✓ Updated {updated_count} markets")
        
        # Test basic stats
        stats = cache.get_cache_stats()
        print(f"✓ Cache stats: {stats['total_markets']} markets, {stats['active_markets']} active")
        
        return cache
        
    except Exception as e:
        print(f"✗ Cache initialization failed: {e}")
        return None

def run_cache_performance_test(cache: MarketCache = None):
    """Test cache lookup performance"""
    print("\n" + "=" * 60)
    print("MARKET CACHE - PERFORMANCE TEST")
    print("=" * 60)
    
    if not cache:
        cache = MarketCache()
        cache.update_cache()
    
    try:
        active_tickers = cache.get_all_active_tickers()
        if not active_tickers:
            print("✗ No active markets for performance test")
            return False
        
        # Test lookup speed
        test_ticker = active_tickers[0]
        print(f"Testing lookup speed for {test_ticker}...")
        
        start_time = time.perf_counter()
        for _ in range(1000):
            price = cache.get_current_price(test_ticker)
        end_time = time.perf_counter()
        
        avg_time_ms = (end_time - start_time) / 1000 * 1000
        print(f"✓ Average lookup time: {avg_time_ms:.3f}ms (1000 lookups)")
        
        # Test different lookup methods
        print(f"\nTesting different methods for {test_ticker}:")
        mid_price = cache.get_mid_price(test_ticker)
        momentum = cache.get_momentum(test_ticker)
        history = cache.get_price_history(test_ticker, limit=5)
        
        print(f"  Mid price: ${mid_price:.3f}" if mid_price else "  Mid price: None")
        print(f"  Momentum: {momentum:+.2f}%" if momentum else "  Momentum: None")
        print(f"  History: {len(history)} snapshots")
        
        return True
        
    except Exception as e:
        print(f"✗ Performance test failed: {e}")
        return False

def run_full_cache_test():
    """Complete market cache validation"""
    print("\n" + "=" * 60)
    print("MARKET CACHE - FULL VALIDATION")
    print("=" * 60)
    
    # Test initialization
    cache = run_cache_initialization_test()
    if not cache:
        return False
    
    # Test performance
    if not run_cache_performance_test(cache):
        return False
    
    # Show final status
    print("\nFinal cache status:")
    cache.print_cache_status()
    
    print("\n" + "=" * 60)
    print("✓ ALL MARKET CACHE TESTS PASSED")
    print("✓ Ready for strategy integration")
    print("=" * 60)
    
    return cache

# ======================
# CONSOLE USAGE
# ======================

if __name__ == "__main__":
    print("market_data/market_cache.py loaded successfully!")
    print("\nQuick test commands:")
    print("  run_cache_initialization_test()  - Test basic cache functionality")
    print("  run_cache_performance_test()     - Test lookup performance")  
    print("  run_full_cache_test()            - Complete validation")
    print("  cache.start_auto_update()        - Start automatic updates")
    print("  cache.print_cache_status()       - Show cache status")
    print("\nRun: run_full_cache_test()")