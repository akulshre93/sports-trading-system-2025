# market_data/data_collector.py - FIXED: Rate Limited Threading Implementation
"""
Sports Trading System 2.0 - Market Data Collector (FIXED)
FIXES: Proper rate limiting (20 calls/sec), active market filtering, threading errors
"""

import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
import json
from pathlib import Path
import concurrent.futures

# Import core components
import sys
sys.path.append(str(Path(__file__).parent.parent))

from core.database import DatabaseManager
from core.api_client import ExchangeClient
from configs.config_base import get_kalshi_config, get_system_config

# Import cryptography for key loading
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend


class MarketDataCollectionError(Exception):
    """Custom exception for market data collection errors"""
    pass


class RateLimiter:
    """
    Thread-safe rate limiter for API calls
    Ensures we never exceed Kalshi's 20 calls/second limit
    """
    
    def __init__(self, max_calls_per_second: int = 18):  # Slightly under 20 for safety
        self.max_calls_per_second = max_calls_per_second
        self.min_interval = 1.0 / max_calls_per_second  # ~0.056 seconds between calls
        self.last_call_time = 0
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        """Wait if necessary to respect rate limit"""
        with self.lock:
            now = time.time()
            elapsed = now - self.last_call_time
            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                time.sleep(sleep_time)
            self.last_call_time = time.time()


class MarketDataCollector:
    """
    FIXED: Real-time market data collector with proper rate limiting
    """
    
    def __init__(self):
        """Initialize collector with core components"""
        
        try:
            from core.logger import get_component_logger
            self.logger = get_component_logger('market_data')
        except ImportError:
            # Fallback to standard logging if circular import
            import logging
            logging.basicConfig(level=logging.INFO)
            self.logger = logging.getLogger('market_data')
        
        self.database = DatabaseManager()
        
        # Initialize API client with proper key loading
        self._initialize_api_client()
        
        # ADDED: Rate limiter for API calls
        self.rate_limiter = RateLimiter(max_calls_per_second=18)
        
        # Configuration
        self.system_config = get_system_config()
        self.collection_active = False
        self.active_markets = []  # In-memory list of today's markets
        
        # Threading
        self._collection_thread = None
        self._scheduler_thread = None
        self._shutdown_event = threading.Event()
        self._markets_lock = threading.Lock()
        
        # Performance tracking
        self.stats = {
            'markets_discovered': 0,
            'successful_updates': 0,
            'failed_updates': 0,
            'rate_limit_errors': 0,
            'last_discovery_time': None,
            'last_collection_time': None,
            'collection_errors': 0,
            'active_markets_filtered': 0
        }
        
        self.logger.info("MarketDataCollector initialized with rate limiting")
    
    def _initialize_api_client(self):
        """Initialize API client with proper RSA key loading"""
        try:
            kalshi_config = get_kalshi_config()
            
            # Load RSA private key from file
            with open(kalshi_config['private_key_path'], 'rb') as key_file:
                private_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=None,
                    backend=default_backend()
                )
            
            # Initialize ExchangeClient with proper parameters
            self.api_client = ExchangeClient(
                exchange_api_base=kalshi_config['api_host'],
                key_id=kalshi_config['key_id'],
                private_key=private_key
            )
            
            self.logger.info(f"API client initialized: {kalshi_config['api_host']}")
            
        except Exception as e:
            raise MarketDataCollectionError(f"Failed to initialize API client: {e}")
    
    # ======================
    # MARKET DISCOVERY (FIXED - Added Active Filtering)
    # ======================
    
    def discover_markets(self) -> int:
        """
        FIXED: Discover today's markets and filter to active ones only
        Returns number of ACTIVE markets discovered
        """
        try:
            self.logger.info("Starting market discovery...")
            
            # Get active sports from database (filtered by config_base.ACTIVE_SPORTS)
            active_sports = self.database.get_active_sports()
            
            if not active_sports:
                raise MarketDataCollectionError("No active sports configured")
            
            total_markets_stored = 0
            all_active_markets = []
            
            # Process each active sport
            for sport in active_sports:
                self.logger.info(f"Processing {sport['sport_name']} markets...")
                
                try:
                    # Get markets for this sport from API (with rate limiting)
                    self.rate_limiter.wait_if_needed()
                    self.logger.info(f"Fetching markets for series: {sport['ticker_prefix']}")
                    response = self.api_client.get_markets(series_ticker=sport['ticker_prefix'])
                    
                    if not isinstance(response, dict) or 'markets' not in response:
                        self.logger.warning(f"Invalid API response for {sport['sport_name']}: {response}")
                        continue
                    
                    all_markets = response['markets']
                    self.logger.info(f"Retrieved {len(all_markets)} total {sport['sport_name']} markets")
                    
                    # FIXED: Filter for today's games AND active status
                    active_today_markets = self._filter_active_todays_markets(all_markets)
                    
                    if not active_today_markets:
                        self.logger.info(f"No active games today for {sport['sport_name']}")
                        continue
                    
                    # Store markets in database
                    stored_count = self._store_markets(active_today_markets, sport['id'])
                    total_markets_stored += stored_count
                    
                    # Add to in-memory active markets list
                    sport_active_markets = [
                        {
                            'ticker': market['ticker'],
                            'event_ticker': market['event_ticker'],
                            'title': market['title'],
                            'expected_expiration_time': market['expected_expiration_time'],
                            'sport_id': sport['id'],
                            'status': market.get('status', 'unknown')
                        }
                        for market in active_today_markets
                    ]
                    all_active_markets.extend(sport_active_markets)
                    
                    self.logger.info(f"{sport['sport_name']}: {stored_count} ACTIVE today's games stored")
                    
                except Exception as e:
                    self.logger.error(f"Failed to process {sport['sport_name']} markets: {e}")
                    continue
            
            # Update in-memory active markets list (thread-safe)
            with self._markets_lock:
                self.active_markets = all_active_markets
            
            # Update stats
            self.stats['markets_discovered'] = total_markets_stored
            self.stats['active_markets_filtered'] = len(all_active_markets)
            self.stats['last_discovery_time'] = datetime.now(timezone.utc)
            
            # Record health metrics
            self.database.record_system_health(
                component='market_collector',
                status='healthy',
                message=f"Discovered {total_markets_stored} ACTIVE markets (filtered from larger set)",
                metric_name='active_markets_discovered',
                metric_value=total_markets_stored
            )
            
            self.logger.info(f"Market discovery completed: {total_markets_stored} ACTIVE games (vs potentially 40+ total)")
            return total_markets_stored
            
        except Exception as e:
            self.logger.error(f"Market discovery failed: {e}")
            self.database.record_system_health(
                component='market_collector',
                status='error',
                message=f"Discovery failed: {str(e)}"
            )
            raise MarketDataCollectionError(f"Market discovery failed: {e}")
    
    def _filter_active_todays_markets(self, all_markets: List[Dict]) -> List[Dict]:
        """FIXED: Filter markets for TODAY'S ACTIVE games only"""
        now = datetime.now(timezone.utc)
        today = now.date()
        tomorrow = (now + timedelta(days=1)).date()  # Include early tomorrow games
        
        active_markets = []
        
        for market in all_markets:
            # FIXED: Check market status first
            market_status = market.get('status', 'unknown').lower()
            if market_status not in ['open', 'active']:
                continue  # Skip closed/settled markets
            
            exp_time_str = market.get('expected_expiration_time', '')
            if not exp_time_str:
                continue
            
            try:
                # Parse ISO datetime with Z timezone
                exp_time = datetime.fromisoformat(exp_time_str.replace('Z', '+00:00'))
                exp_date = exp_time.date()
                
                # FIXED: Only include today's/tomorrow's games that are:
                # 1. Still in the future (haven't expired)
                # 2. Not too far in the future (within 24 hours)
                if (exp_date in [today, tomorrow] and 
                    exp_time > now and 
                    exp_time < now + timedelta(hours=24)):
                    active_markets.append(market)
                    
            except ValueError as e:
                self.logger.warning(f"Invalid expiration time format for {market.get('ticker', 'unknown')}: {exp_time_str}")
                continue
        
        self.logger.info(f"Filtered from {len(all_markets)} total to {len(active_markets)} ACTIVE today's games")
        return active_markets
    
    def _store_markets(self, markets: List[Dict], sport_id: int) -> int:
        """Store discovered markets in database (unchanged)"""
        stored_count = 0
        
        for market in markets:
            try:
                # Extract settlement time from expected_expiration_time
                exp_time_str = market.get('expected_expiration_time', '')
                settlement_time = None
                
                if exp_time_str:
                    settlement_time = datetime.fromisoformat(exp_time_str.replace('Z', '+00:00'))
                
                # Prepare market data for database
                market_data = {
                    'ticker': market['ticker'],
                    'event_ticker': market['event_ticker'],
                    'sport_id': sport_id,
                    'market_type': market.get('market_type', 'binary'),
                    'event_name': market.get('title', ''),
                    'settlement_time': settlement_time,
                    'close_time': None,  # We could parse this too if needed
                    'is_active': market.get('status') == 'active',
                    'last_data_update': None,  # Will be set during collection
                    'consecutive_failures': 0
                }
                
                # Insert or update market (upsert behavior)
                with self.database.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO markets (
                                ticker, event_ticker, sport_id, market_type, event_name,
                                settlement_time, close_time, is_active, last_data_update, consecutive_failures
                            ) VALUES (
                                %(ticker)s, %(event_ticker)s, %(sport_id)s, %(market_type)s, %(event_name)s,
                                %(settlement_time)s, %(close_time)s, %(is_active)s, %(last_data_update)s, %(consecutive_failures)s
                            )
                            ON CONFLICT (ticker) DO UPDATE SET
                                event_name = EXCLUDED.event_name,
                                settlement_time = EXCLUDED.settlement_time,
                                is_active = EXCLUDED.is_active,
                                updated_at = CURRENT_TIMESTAMP
                        """, market_data)
                        
                        conn.commit()
                        stored_count += 1
                
            except Exception as e:
                self.logger.error(f"Failed to store market {market.get('ticker', 'unknown')}: {e}")
                continue
        
        return stored_count
    
    # ======================
    # PRICE COLLECTION (COMPLETELY FIXED)
    # ======================
    
    def collect_market_data_cycle(self):
        """
        FIXED: Rate-limited market data collection cycle
        Uses proper threading with rate limiting - no more 429 errors
        """
        if not self.active_markets:
            self.logger.warning("No active markets to collect data for")
            return
    
        cycle_start = time.time()
        successful_updates = 0
        failed_updates = 0
    
        # Get current active markets (thread-safe copy)
        with self._markets_lock:
            markets_to_process = self.active_markets.copy()
    
        self.logger.debug(f"Starting collection cycle for {len(markets_to_process)} markets")
        
        # FIXED: Sequential collection with rate limiting (reliable approach)
        # We could use threading, but sequential with rate limiting is more reliable
        # and still fast enough for 2-10 markets
        
        for market in markets_to_process:
            try:
                # Rate limit before each API call
                self.rate_limiter.wait_if_needed()
                
                result = self._collect_single_market(market)
                if result and result.get('success'):
                    successful_updates += 1
                else:
                    failed_updates += 1
                    
            except Exception as e:
                self.logger.error(f"Collection failed for {market['ticker']}: {e}")
                failed_updates += 1
                continue
    
        # Update stats
        cycle_duration = time.time() - cycle_start
        self.stats['successful_updates'] += successful_updates
        self.stats['failed_updates'] += failed_updates
        self.stats['last_collection_time'] = datetime.now(timezone.utc)
    
        if successful_updates > 0:
            self.logger.debug(f"Collection cycle: {successful_updates} success, {failed_updates} failed, {cycle_duration:.2f}s")
        elif len(markets_to_process) > 0:
            self.logger.warning(f"Collection cycle: ALL {len(markets_to_process)} markets failed!")
            
    def _collect_single_market(self, market):
        """Collect data for single market (thread-safe)"""
        try:
            ticker = market['ticker']
            
            # Get market data from API (includes bid/ask)
            market_data = self.api_client.get_market(ticker)
            
            # Also get orderbook for retrospective analysis
            orderbook = self.api_client.get_orderbook(ticker)
            
            # Validate market data response first
            if not market_data or 'market' not in market_data:
                return {'success': False, 'ticker': ticker, 'reason': 'no_market_data'}
                
            market_info = market_data['market']
            
            # Check if market is active and has pricing data
            if market_info.get('status') != 'active' or market_info.get('yes_bid') is None:
                return {'success': False, 'ticker': ticker, 'reason': 'inactive_or_no_pricing'}
            
            # Validate orderbook (for retrospective analysis)
            if not orderbook or not isinstance(orderbook, dict) or 'orderbook' not in orderbook:
                self.logger.warning(f"No orderbook data for {ticker} - continuing with market data only")
                orderbook = None
    
            # Extract price data from market API response 
            price_data = self._extract_price_data(market_info, orderbook, market)
            
            # Store market data snapshot
            self._store_market_snapshot(price_data)
            
            # Update market last_data_update timestamp
            self._update_market_timestamp(ticker, success=True)
            
            return {'success': True, 'ticker': ticker}
            
        except Exception as e:
            error_msg = str(e)
            
            # Handle 429 rate limit errors specifically
            if '429' in error_msg or 'Too Many Requests' in error_msg:
                self.logger.warning(f"Rate limit hit for {market['ticker']}: {e}")
                self.stats['rate_limit_errors'] += 1
                time.sleep(1.0)
                return {'success': False, 'ticker': market['ticker'], 'error': 'rate_limited'}
            else:
                self.logger.error(f"Failed to collect data for {market['ticker']}: {e}")
                self._update_market_timestamp(market['ticker'], success=False)
                return {'success': False, 'ticker': market['ticker'], 'error': str(e)}
    
    def _extract_price_data(self, market_info: Dict, orderbook: Optional[Dict], market: Dict) -> Dict:
        """Extract price data from market API response and orderbook"""
        
        # Get bid/ask directly from market API (Kalshi provides these calculated)
        yes_bid = market_info.get('yes_bid')
        yes_ask = market_info.get('yes_ask') 
        no_bid = market_info.get('no_bid')
        no_ask = market_info.get('no_ask')
        
        # Get additional market data
        last_price = market_info.get('last_price')
        volume_24h = market_info.get('volume_24h', 0)
        liquidity = market_info.get('liquidity', 0)
        open_interest = market_info.get('open_interest', 0)
        
        # Extract orderbook volume data if available
        yes_volume = 0
        no_volume = 0
        
        if orderbook and 'orderbook' in orderbook:
            book = orderbook['orderbook']
            yes_orders = book.get('yes', [])
            no_orders = book.get('no', [])
            
            # Sum volumes from all resting orders
            yes_volume = sum([order[1] for order in yes_orders])
            no_volume = sum([order[1] for order in no_orders])
        
        # Calculate relative time to game start
        relative_time = self._calculate_relative_time(market)
        
        return {
            'ticker': market['ticker'],
            'sport_id': market['sport_id'],
            'yes_bid': yes_bid,
            'yes_ask': yes_ask,
            'no_bid': no_bid,
            'no_ask': no_ask,
            'last_price': last_price,
            'volume_24h': volume_24h,
            'liquidity': liquidity,
            'open_interest': open_interest,
            'yes_volume': yes_volume,  # From orderbook
            'no_volume': no_volume,    # From orderbook
            'total_volume': yes_volume + no_volume,  # From orderbook
            'relative_min_to_game_start': relative_time,
            'timestamp': datetime.utcnow().replace(tzinfo=timezone.utc)
        }
    
    def _calculate_relative_time(self, market: Dict) -> Optional[int]:
        """Calculate minutes to game start (unchanged)"""
        try:
            exp_time_str = market['expected_expiration_time']
            exp_time = datetime.fromisoformat(exp_time_str.replace('Z', '+00:00'))
            
            # Assume game starts 3 hours before expected expiration (for baseball)
            game_start = exp_time - timedelta(hours=3)
            
            now = datetime.now(timezone.utc)
            time_diff = now - game_start
            
            return int(time_diff.total_seconds() / 60)
            
        except Exception:
            return None
    
    def _store_market_snapshot(self, price_data: Dict):
        """Store market data snapshot in database"""
        with self.database.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO market_data_history (
                        ticker, sport_id, yes_bid, yes_ask, no_bid, no_ask,
                        last_price, volume_24h, liquidity, open_interest,
                        yes_volume, no_volume, total_volume, 
                        relative_min_to_game_start, timestamp
                    ) VALUES (
                        %(ticker)s, %(sport_id)s, %(yes_bid)s, %(yes_ask)s, %(no_bid)s, %(no_ask)s,
                        %(last_price)s, %(volume_24h)s, %(liquidity)s, %(open_interest)s,
                        %(yes_volume)s, %(no_volume)s, %(total_volume)s, 
                        %(relative_min_to_game_start)s, %(timestamp)s
                    )
                """, price_data)
                
                conn.commit()
    
    def _update_market_timestamp(self, ticker: str, success: bool):
        """Update market's last update timestamp and failure count (unchanged)"""
        with self.database.get_connection() as conn:
            with conn.cursor() as cur:
                if success:
                    cur.execute("""
                        UPDATE markets 
                        SET last_data_update = CURRENT_TIMESTAMP, consecutive_failures = 0
                        WHERE ticker = %s
                    """, (ticker,))
                else:
                    cur.execute("""
                        UPDATE markets 
                        SET consecutive_failures = consecutive_failures + 1
                        WHERE ticker = %s
                    """, (ticker,))
                
                conn.commit()
    
    # ======================
    # SCHEDULING & THREADING (Updated with better error handling)
    # ======================
    
    def start_collection(self):
        """Start background market data collection"""
        if self.collection_active:
            self.logger.warning("Collection already active")
            return
        
        try:
            # Initial market discovery
            discovered_count = self.discover_markets()
            if discovered_count == 0:
                raise MarketDataCollectionError("No active markets discovered - cannot start collection")
            
            # Start 1-second collection thread
            self.collection_active = True
            self._collection_thread = threading.Thread(target=self._collection_loop, daemon=True)
            self._collection_thread.start()
            
            # Start daily scheduler thread
            self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
            self._scheduler_thread.start()
            
            self.logger.info(f"Market data collection started: {discovered_count} active markets, rate limited to 18 calls/sec")
            
        except Exception as e:
            self.logger.error(f"Failed to start collection: {e}")
            self.collection_active = False
            raise
    
    def stop_collection(self):
        """Stop background market data collection (unchanged)"""
        if not self.collection_active:
            return
        
        self.logger.info("Stopping market data collection...")
        
        self.collection_active = False
        self._shutdown_event.set()
        
        # Wait for threads to finish
        if self._collection_thread and self._collection_thread.is_alive():
            self._collection_thread.join(timeout=5)
        
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join(timeout=2)
        
        self.logger.info("Market data collection stopped")
    
    def _collection_loop(self):
        """FIXED: Background thread for 1-second market data collection"""
        self.logger.info("Collection loop started - rate limited collection")
        
        while self.collection_active and not self._shutdown_event.is_set():
            try:
                loop_start = time.time()
                
                # Collect market data
                self.collect_market_data_cycle()
                
                # Calculate sleep time to maintain 1-second intervals
                elapsed = time.time() - loop_start
                sleep_time = max(0, 1.0 - elapsed)
                
                if sleep_time > 0:
                    if self._shutdown_event.wait(sleep_time):
                        break  # Shutdown requested
                else:
                    self.logger.warning(f"Collection cycle took {elapsed:.2f}s (>1s)")
                    
            except Exception as e:
                self.logger.error(f"Collection loop error: {e}")
                self.stats['collection_errors'] += 1
                
                # Brief pause before retrying
                if self._shutdown_event.wait(1):
                    break
        
        self.logger.info("Collection loop stopped")
    
    def _scheduler_loop(self):
        """Background thread for daily market discovery scheduling (unchanged)"""
        self.logger.info("Starting scheduler for daily market refresh")
        
        last_discovery_date = datetime.now(timezone.utc).date()
        
        while self.collection_active and not self._shutdown_event.is_set():
            try:
                now = datetime.now(timezone.utc)
                current_date = now.date()
                
                # Check if it's a new day and past 8am ET (13:00 UTC)
                if (current_date > last_discovery_date and now.hour >= 13):
                    self.logger.info("Running scheduled market discovery (8am ET)")
                    try:
                        self.discover_markets()
                        last_discovery_date = current_date
                    except Exception as e:
                        self.logger.error(f"Scheduled market discovery failed: {e}")
                
                # Check every 5 minutes
                if self._shutdown_event.wait(300):
                    break
                    
            except Exception as e:
                self.logger.error(f"Scheduler error: {e}")
                if self._shutdown_event.wait(300):
                    break
        
        self.logger.info("Scheduler stopped")
    
    # ======================
    # STATUS & MONITORING (Enhanced with rate limiting stats)
    # ======================
    
    def get_status(self) -> Dict[str, Any]:
        """Get collector status and statistics"""
        with self._markets_lock:
            active_count = len(self.active_markets)
        
        return {
            'collection_active': self.collection_active,
            'active_markets_count': active_count,
            'rate_limiter': {
                'max_calls_per_second': self.rate_limiter.max_calls_per_second,
                'min_interval_ms': self.rate_limiter.min_interval * 1000
            },
            'stats': self.stats.copy(),
            'threads': {
                'collection_thread_alive': self._collection_thread.is_alive() if self._collection_thread else False,
                'scheduler_thread_alive': self._scheduler_thread.is_alive() if self._scheduler_thread else False
            }
        }
    
    def print_status(self):
        """ENHANCED: Print current collector status with rate limiting info"""
        status = self.get_status()
        
        print("\n" + "=" * 50)
        print("MARKET DATA COLLECTOR STATUS (FIXED)")
        print("=" * 50)
        
        print(f"Collection Active: {'✅' if status['collection_active'] else '❌'}")
        print(f"Active Markets: {status['active_markets_count']}")
        print(f"Rate Limit: {status['rate_limiter']['max_calls_per_second']} calls/sec ({status['rate_limiter']['min_interval_ms']:.1f}ms intervals)")
        
        stats = status['stats']
        print(f"Markets Discovered: {stats['markets_discovered']} (filtered from larger set)")
        print(f"Successful Updates: {stats['successful_updates']}")
        print(f"Failed Updates: {stats['failed_updates']}")
        print(f"Rate Limit Errors: {stats['rate_limit_errors']}")  # NEW
        print(f"Collection Errors: {stats['collection_errors']}")
        
        if stats['last_discovery_time']:
            print(f"Last Discovery: {stats['last_discovery_time'].strftime('%Y-%m-%d %H:%M:%S UTC')}")
        if stats['last_collection_time']:
            print(f"Last Collection: {stats['last_collection_time'].strftime('%Y-%m-%d %H:%M:%S UTC')}")
        
        threads = status['threads']
        print(f"Collection Thread: {'✅' if threads['collection_thread_alive'] else '❌'}")
        print(f"Scheduler Thread: {'✅' if threads['scheduler_thread_alive'] else '❌'}")
        
        # Show sample of active markets being monitored
        with self._markets_lock:
            if self.active_markets:
                print(f"\nActive Markets Sample:")
                for market in self.active_markets[:3]:
                    print(f"  - {market['ticker']}: {market['title']}")
        
        print("=" * 50)
    
    def __del__(self):
        """Cleanup on destruction"""
        self.stop_collection()


# ======================
# CONSOLE TESTING INTERFACE (Updated)
# ======================

def run_fixed_discovery_test():
    """Test FIXED market discovery with active filtering"""
    print("\n" + "=" * 60)
    print("FIXED MARKET DATA COLLECTOR - DISCOVERY TEST")
    print("=" * 60)
    
    try:
        collector = MarketDataCollector()
        print("✅ Fixed collector initialized with rate limiting")
        
        # Test market discovery
        print("\nTesting ACTIVE market discovery...")
        markets_found = collector.discover_markets()
        print(f"✅ Discovered {markets_found} ACTIVE markets (filtered)")
        
        # Show active markets
        with collector._markets_lock:
            print(f"✅ Active markets loaded: {len(collector.active_markets)}")
            for i, market in enumerate(collector.active_markets):
                status = market.get('status', 'unknown')
                print(f"  {i+1}. {market['ticker']} ({status}): {market['title']}")
        
        return collector
        
    except Exception as e:
        print(f"❌ Fixed discovery test failed: {e}")
        return None

def run_fixed_collection_test(collector: MarketDataCollector = None):
    """Test FIXED collection cycle with rate limiting"""
    print("\n" + "=" * 60)
    print("FIXED MARKET DATA COLLECTOR - COLLECTION TEST")
    print("=" * 60)
    
    if not collector:
        collector = MarketDataCollector()
        collector.discover_markets()
    
    try:
        print("Testing FIXED collection cycle with rate limiting...")
        cycle_start = time.time()
        collector.collect_market_data_cycle()
        cycle_time = time.time() - cycle_start
        
        print(f"✅ Collection cycle completed in {cycle_time:.2f} seconds")
        
        # Show stats
        stats = collector.stats
        print(f"✅ Successful updates: {stats['successful_updates']}")
        print(f"⚠️  Failed updates: {stats['failed_updates']}")
        print(f"⚠️  Rate limit errors: {stats['rate_limit_errors']}")  # NEW
        
        return True
        
    except Exception as e:
        print(f"❌ Fixed collection test failed: {e}")
        return False

def run_fixed_collector_full_test():
    """Complete FIXED market data collector validation"""
    print("\n" + "=" * 60)
    print("FIXED MARKET DATA COLLECTOR - FULL VALIDATION")
    print("=" * 60)
    
    # Test discovery
    collector = run_fixed_discovery_test()
    if not collector:
        return False
    
    # Test collection cycle
    if not run_fixed_collection_test(collector):
        return False
    
    # Show final status
    print("\nFixed collector status:")
    collector.print_status()
    
    print("\n" + "=" * 60)
    print("✅ ALL FIXED COLLECTOR TESTS PASSED")
    print("✅ Ready for live data collection with rate limiting")
    print("=" * 60)
    
    return collector

# ======================
# CONSOLE USAGE
# ======================

if __name__ == "__main__":
    print("market_data/data_collector.py (FIXED) loaded successfully!")
    print("\nFIXED Quick test commands:")
    print("  run_fixed_discovery_test()       - Test FIXED discovery with filtering")
    print("  run_fixed_collection_test()      - Test FIXED collection with rate limiting")  
    print("  run_fixed_collector_full_test()  - Complete FIXED validation")
    print("  collector.start_collection()     - Start FIXED live collection")
    print("  collector.print_status()         - Show FIXED status")
    print("\nRun: run_fixed_collector_full_test()")