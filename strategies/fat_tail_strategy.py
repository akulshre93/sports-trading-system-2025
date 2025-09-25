# strategies/fat_tail_strategy.py - Fat Tail Statistical Arbitrage Strategy
"""
Fat Tail Statistical Strategy - Single File Implementation
Strategy: BACKING 32-46¢ underdogs with systematic edge
Updated logic: Dynamic allocation, limit orders before 5min to game start, market orders after
"""

import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any
import json

# Import core components
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from market_data.market_cache import MarketCache
from core.position_monitor import PositionMonitor
from core.database import DatabaseManager
import logging
from core.order_executor import OrderExecutor


class FatTailStrategy:
    """
    Fat Tail Statistical Arbitrage Strategy
    BACKING 32-46¢ underdogs with systematic 2.3% edge
    """
    
    def __init__(self, daily_portfolio_allocation: float):
        """Initialize strategy with daily portfolio allocation"""
        
        self.logger = logging.getLogger('fat_tail_statistical')
        self.database = DatabaseManager()
        self.strategy_id = self.database.get_strategy_id('fat_tail_statistical')
        self.order_executor = OrderExecutor()
        
        # Core components
        self.market_cache = MarketCache()
        self.position_monitor = PositionMonitor()
        
        # Strategy configuration (updated based on your new logic)
        self.config = {
            # Core strategy parameters
            'price_range_min': 0.32,           # 32¢ minimum underdog price
            'price_range_max': 0.48,           # 48¢ maximum underdog price  
            'strategy_type': 'BACKING',        # BACKING underdogs
            
            # Portfolio allocation
            'daily_portfolio_allocation': daily_portfolio_allocation,
            'max_position_pct_per_market': 0.10,  # 5% max per market (risk management)
            
            # Order timing logic (NEW - based on your description)
            'limit_order_cutoff_minutes': 5,   # Switch to market orders 5 min before game start
            'limit_order_refresh_seconds': 30, # Refresh limit orders every 30 seconds
            
            # Exit criteria
            'stop_loss_threshold': 0.14,       # Exit if price hits 23¢
            'take_profit_threshold': 0.80,     # Consider profit taking above 80¢
            
            # Systematic edge validation
            'min_expected_edge': 0.005,        # 2.3% minimum edge
            'systematic_win_rate': 0.501,      # Historical 46.1% vs 43.8% implied
        }
        
        # Runtime state
        self.daily_allocation_per_market = {}  # ticker -> allocated dollars
        self.pending_orders = {}               # ticker -> order_id
        self.active_positions = {}             # ticker -> position_info
        self.last_scan_time = None
        
        # Threading
        self.is_running = False
        self.scan_thread = None
        self.shutdown_event = threading.Event()
        
        # Performance tracking
        self.stats = {
            'opportunities_found': 0,
            'orders_placed': 0,
            'positions_entered': 0,
            'positions_exited': 0,
            'scan_cycles': 0,
            'last_allocation_calculation': None
        }
        
        self.logger.info(f"Fat Tail Strategy initialized with ${daily_portfolio_allocation:.2f} daily allocation")
        
    def _get_order_priority_and_timeout(self, order_action: str, order_type: str) -> Tuple[int, int]:
        """Get priority and timeout for different order types"""
        if order_action == 'exit':
            return 2, 60  # High priority exits, 60 seconds
        elif order_type == 'market':
            return 4, 30  # Medium priority market orders, 30 seconds  
        else:  # limit orders
            return 6, 300000  # Lower priority limit orders, 5 minutes
    
    # ======================
    # DAILY SETUP & ALLOCATION
    # ======================
    
    def calculate_daily_allocations(self) -> int:
        """
        Calculate daily position allocations based on number of in-range markets
        Called once per day at market open
        Returns number of markets in range
        """
        try:
            self.logger.info("Calculating daily market allocations...")
            
            # Get all active markets from cache
            active_tickers = self.market_cache.get_all_active_tickers()
            
            if not active_tickers:
                self.logger.warning("No active markets found for allocation calculation")
                return 0
            
            # Filter for markets in our price range
            in_range_markets = []
            
            for ticker in active_tickers:
                mid_price = self.market_cache.get_mid_price(ticker, 'yes')
                if mid_price and self.config['price_range_min'] <= mid_price <= self.config['price_range_max']:
                    in_range_markets.append(ticker)
            
            num_markets = len(in_range_markets)
            if num_markets == 0:
                self.logger.warning("No markets in price range (32-46¢)")
                return 0
            
            # Calculate allocation per market
            total_allocation = self.config['daily_portfolio_allocation']
            max_per_market = total_allocation * self.config['max_position_pct_per_market']
            
            # Divide equally but cap at max per market
            equal_allocation = total_allocation / num_markets
            allocation_per_market = min(equal_allocation, max_per_market)
            
            # Store allocations
            self.daily_allocation_per_market = {
                ticker: allocation_per_market for ticker in in_range_markets
            }
            
            self.stats['last_allocation_calculation'] = datetime.now(timezone.utc)
            
            self.logger.info(f"Daily allocation: {num_markets} markets, ${allocation_per_market:.2f} each (max ${max_per_market:.2f})")
            
            return num_markets
            
        except Exception as e:
            self.logger.error(f"Failed to calculate daily allocations: {e}")
            return 0
    
    # ======================
    # OPPORTUNITY SCANNING
    # ======================
    
    def scan_for_opportunities(self):
        """
        Scan all allocated markets for entry opportunities
        Called every second during trading hours
        """
        try:
            current_time = datetime.now(timezone.utc)
            opportunities_found = 0
            
            for ticker, allocated_amount in self.daily_allocation_per_market.items():
                
                # Skip if we already have position or pending order
                if self._has_existing_exposure(ticker):
                    continue
                
                # Get current market data
                current_price = self.market_cache.get_mid_price(ticker, 'yes')
                if not current_price:
                    continue
                
                # Check if still in target range
                if not (self.config['price_range_min'] <= current_price <= self.config['price_range_max']):
                    continue
                
                # Calculate time to game start
                minutes_to_game = self._get_minutes_to_game_start(ticker)
                if minutes_to_game is None:
                    continue
                
                # Check if we should place order
                if self._should_place_order(ticker, current_price, minutes_to_game):
                    self._place_entry_order(ticker, current_price, minutes_to_game, allocated_amount)
                    opportunities_found += 1
            
            self.stats['opportunities_found'] += opportunities_found
            self.stats['scan_cycles'] += 1
            self.last_scan_time = current_time
            
            if opportunities_found > 0:
                self.logger.info(f"Scan cycle: {opportunities_found} opportunities found")
            
        except Exception as e:
            self.logger.error(f"Opportunity scan failed: {e}")
    
    def _has_existing_exposure(self, ticker: str) -> bool:
        """Check if we already have position or pending order for ticker"""
        
        # Check pending orders
        if ticker in self.pending_orders:
            return True
        
        # Check active positions
        current_position = self.database.get_latest_position(self.strategy_id, ticker)
        if current_position and current_position.get('position', 0) != 0:
            return True
        
        return False
    
    def _get_minutes_to_game_start(self, ticker: str) -> Optional[int]:
        """Get minutes until game starts (negative = game hasn't started, positive = game in progress)"""
        try:
            # Get current market data which includes timing
            current_price_data = self.market_cache.get_current_price(ticker)
            
            if not current_price_data:
                self.logger.warning(f"No market data available for {ticker}")
                return None
            
            # Extract relative time to game start
            relative_time = current_price_data.get('relative_min_to_game_start')
            
            if relative_time is None:
                self.logger.warning(f"No timing data available for {ticker}")
                return None
            
            return relative_time
            
        except Exception as e:
            self.logger.error(f"Failed to get game timing for {ticker}: {e}")
            return None
    
    def _should_place_order(self, ticker: str, current_price: float, minutes_to_game: int) -> bool:
        """Determine if we should place an order for this opportunity"""
        
        # Basic opportunity criteria
        if not (self.config['price_range_min'] <= current_price <= self.config['price_range_max']):
            return False
        
        # Don't trade after game has started
        if minutes_to_game > 0:  # Game already started
            return False
        
        # Calculate expected edge (simplified)
        implied_prob = current_price
        our_edge = self.config['systematic_win_rate'] - implied_prob
        
        if our_edge < self.config['min_expected_edge']:
            return False
        
        return True
    
    # ======================
    # ORDER PLACEMENT
    # ======================
    
    def _place_entry_order(self, ticker: str, current_price: float, minutes_to_game: int, allocation: float):
        """
        Place entry order based on timing and market conditions
        Logic: Limit orders before 5 min to game start, market orders after
        """
        try:
            # Determine order type and price based on timing
            if abs(minutes_to_game) > self.config['limit_order_cutoff_minutes']:
                # More than 5 minutes until game start - use limit order at best bid
                order_type = 'limit'
                order_price = self._get_best_bid_price(ticker)
                
                if not order_price:
                    self.logger.warning(f"No bid price available for {ticker}, skipping")
                    return
                    
            else:
                # Within 5 minutes of game start - use market order
                order_type = 'market'
                order_price = current_price  # Will be ignored for market orders
            
            # Calculate quantity based on allocation
            quantity = int(allocation / current_price)
            if quantity == 0:
                self.logger.warning(f"Allocation too small for {ticker}: ${allocation:.2f} at ${current_price:.2f}")
                return
            
            # Prepare order data
            order_data = {
                'ticker': ticker,
                'side': 'yes',  # BACKING the underdog
                'action': 'buy',
                'order_type': order_type,
                'quantity': quantity,
                'price': order_price,
                'strategy_action': 'entry',
                'strategy_reason': f'Fat tail BACKING {current_price:.2f} underdog, edge={self.config["min_expected_edge"]:.1%}'
            }
            
            # Get priority and timeout for this order
            priority, timeout_seconds = self._get_order_priority_and_timeout(order_data['strategy_action'], order_type)
            
            # Submit to order executor
            order_request = {
                'strategy_id': self.strategy_id,
                'ticker': order_data['ticker'],
                'side': order_data['side'],
                'action': order_data['action'],
                'order_type': order_data['order_type'],
                'quantity': order_data['quantity'],
                'price': order_data['price'],
                'priority': priority,
                'timeout_seconds': timeout_seconds,
                'strategy_action': order_data['strategy_action'],
                'strategy_reason': order_data['strategy_reason']
            }
            
            try:
                request_uuid = self.order_executor.submit_order_request(order_request)
                self.pending_orders[ticker] = request_uuid
                self.stats['orders_placed'] += 1
                
                self.logger.info(f"Entry order submitted: {ticker} {order_type} {quantity} @ {order_price:.3f} (UUID: {request_uuid})")
                
            except Exception as e:
                self.logger.error(f"Failed to submit entry order for {ticker}: {e}")
                return
            
            # Schedule order refresh for limit orders
            if order_type == 'limit':
                self._schedule_order_refresh(ticker)
            
        except Exception as e:
            self.logger.error(f"Failed to place entry order for {ticker}: {e}")
    
    def _get_best_bid_price(self, ticker: str) -> Optional[float]:
        """Get current best bid price for limit orders"""
        try:
            current_price_data = self.market_cache.get_current_price(ticker)
            if current_price_data:
                yes_bid = current_price_data.get('yes_bid')
                if yes_bid:
                    return yes_bid / 100  # Convert cents to dollars
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to get bid price for {ticker}: {e}")
            return None
    
    def _schedule_order_refresh(self, ticker: str):
        """Schedule limit order refresh in 30 seconds"""
        def refresh_order():
            time.sleep(self.config['limit_order_refresh_seconds'])
            if ticker in self.pending_orders and self.is_running:
                try:
                    # Cancel existing pending order
                    old_request_uuid = self.pending_orders[ticker]
                    self.order_executor.cancel_order_request(old_request_uuid, "limit_order_refresh")
                    
                    # Remove from pending orders - will be re-added if new order placed
                    del self.pending_orders[ticker]
                    
                    self.logger.info(f"Refreshed limit order for {ticker} (cancelled {old_request_uuid})")
                    
                except Exception as e:
                    self.logger.error(f"Failed to refresh limit order for {ticker}: {e}")
        
        refresh_thread = threading.Thread(target=refresh_order, daemon=True)
        refresh_thread.start()
    
    # ======================
    # POSITION MONITORING
    # ======================
    
    def monitor_existing_positions(self):
        """
        Monitor existing positions for exit criteria
        Called every second during trading hours
        """
        try:
            # Get all current positions for this strategy
            strategy_positions = self.database.get_all_positions_for_strategy(self.strategy_id)
            
            positions_monitored = 0
            
            for position in strategy_positions:
                ticker = position['ticker']
                current_position_size = position['position']
                
                if current_position_size == 0:
                    continue  # Skip closed positions
                
                # Get current market price
                current_price = self.market_cache.get_mid_price(ticker, 'yes')
                if not current_price:
                    continue
                
                # Check exit criteria
                should_exit, exit_reason = self._check_exit_criteria(position, current_price)
                
                if should_exit:
                    self._place_exit_order(ticker, current_position_size, current_price, exit_reason)
                
                positions_monitored += 1
            
            if positions_monitored > 0:
                self.logger.debug(f"Monitored {positions_monitored} positions")
            
        except Exception as e:
            self.logger.error(f"Position monitoring failed: {e}")
    
    def _check_exit_criteria(self, position: Dict, current_price: float) -> Tuple[bool, str]:
        """Check if position should be exited"""
        
        # Stop loss check
        if current_price <= self.config['stop_loss_threshold']:
            return True, f"Stop loss triggered at ${current_price:.2f}"
        
        # Profit taking check
        if current_price >= self.config['take_profit_threshold']:
            return True, f"Profit taking at ${current_price:.2f}"
        
        # Game timing check - exit when game starts (placeholder)
        minutes_to_game = self._get_minutes_to_game_start(position['ticker'])
        if minutes_to_game and minutes_to_game > 0:
            return True, "Game started - exit position"
        
        return False, ""
    
    def _place_exit_order(self, ticker: str, position_size: int, current_price: float, exit_reason: str):
        """Place exit order for existing position"""
        try:
            # Always use market orders for exits
            order_data = {
                'ticker': ticker,
                'side': 'yes',  # Selling our yes position
                'action': 'sell',
                'order_type': 'market',
                'quantity': abs(position_size),
                'price': current_price,  # Ignored for market orders
                'strategy_action': 'exit',
                'strategy_reason': exit_reason
            }
            
            # Get priority and timeout for exit order (high priority)
            priority, timeout_seconds = self._get_order_priority_and_timeout('exit', 'market')
            
            # Submit exit order to order executor  
            order_request = {
                'strategy_id': self.strategy_id,
                'ticker': order_data['ticker'],
                'side': order_data['side'],
                'action': order_data['action'],
                'order_type': order_data['order_type'],
                'quantity': order_data['quantity'],
                'price': order_data['price'],
                'priority': priority,
                'timeout_seconds': timeout_seconds,
                'strategy_action': order_data['strategy_action'],
                'strategy_reason': order_data['strategy_reason']
            }
            
            try:
                request_uuid = self.order_executor.submit_order_request(order_request)
                
                # Remove from pending orders if this was an entry position
                if ticker in self.pending_orders:
                    del self.pending_orders[ticker]
                
                self.stats['positions_exited'] += 1
                
                self.logger.info(f"Exit order submitted: {ticker} market {abs(position_size)} (UUID: {request_uuid})")
                
            except Exception as e:
                self.logger.error(f"Failed to submit exit order for {ticker}: {e}")
            
        except Exception as e:
            self.logger.error(f"Failed to place exit order for {ticker}: {e}")
    
    # ======================
    # MAIN STRATEGY LOOP
    # ======================
    
    def start_strategy(self):
        """Start the strategy - daily allocation calculation + continuous monitoring"""
        if self.is_running:
            self.logger.warning("Strategy already running")
            return
        
        try:
            # Calculate daily allocations
            markets_found = self.calculate_daily_allocations()
            if markets_found == 0:
                self.logger.error("No markets found for allocation - strategy cannot start")
                return
            
            # Start market cache auto-update
            self.market_cache.start_auto_update(interval_seconds=1)
            # Start order executor background processing
            self.order_executor.start_processing()
            
            # Start the main strategy loop
            self.is_running = True
            self.scan_thread = threading.Thread(target=self._strategy_loop, daemon=True)
            self.scan_thread.start()
            
            self.logger.info(f"Fat Tail Strategy started - monitoring {markets_found} markets")
            
        except Exception as e:
            self.logger.error(f"Failed to start strategy: {e}")
            self.is_running = False
    
    def stop_strategy(self):
        """Stop the strategy gracefully"""
        if not self.is_running:
            return
        
        self.logger.info("Stopping Fat Tail Strategy...")
        
        self.is_running = False
        self.shutdown_event.set()
        
        # Stop market cache
        self.market_cache.stop_auto_update()
        # Stop order executor
        self.order_executor.stop_processing()
        
        # Wait for strategy thread
        if self.scan_thread and self.scan_thread.is_alive():
            self.scan_thread.join(timeout=5)
        
        self.logger.info("Fat Tail Strategy stopped")
    
    def _strategy_loop(self):
        """Main strategy loop - runs every second"""
        self.logger.info("Fat Tail Strategy loop started - 1-second scanning cycle")
        
        while self.is_running and not self.shutdown_event.is_set():
            try:
                loop_start = time.time()
                
                # Scan for new opportunities
                self.scan_for_opportunities()
                
                # Monitor existing positions
                self.monitor_existing_positions()
                
                # Maintain 1-second loop timing
                elapsed = time.time() - loop_start
                sleep_time = max(0, 1.0 - elapsed)
                
                if sleep_time > 0:
                    if self.shutdown_event.wait(sleep_time):
                        break
                else:
                    self.logger.warning(f"Strategy loop took {elapsed:.2f}s (>1s)")
                
            except Exception as e:
                self.logger.error(f"Strategy loop error: {e}")
                if self.shutdown_event.wait(1):
                    break
        
        self.logger.info("Fat Tail Strategy loop ended")
    
    # ======================
    # STATUS & MONITORING
    # ======================
    
    def get_strategy_status(self) -> Dict[str, Any]:
        """Get current strategy status"""
        return {
            'is_running': self.is_running,
            'daily_allocation': self.config['daily_portfolio_allocation'],
            'markets_allocated': len(self.daily_allocation_per_market),
            'pending_orders': len(self.pending_orders),
            'active_positions': len(self.active_positions),
            'stats': self.stats.copy(),
            'last_scan_time': self.last_scan_time,
            'config': self.config.copy()
        }
    
    def print_strategy_status(self):
        """Print detailed strategy status"""
        status = self.get_strategy_status()
        
        print("\n" + "=" * 60)
        print("FAT TAIL STRATEGY STATUS")
        print("=" * 60)
        
        print(f"Running: {'✓' if status['is_running'] else '✗'}")
        print(f"Daily Allocation: ${status['daily_allocation']:.2f}")
        print(f"Markets in Range: {status['markets_allocated']}")
        print(f"Pending Orders: {status['pending_orders']}")
        print(f"Active Positions: {status['active_positions']}")
        
        print("\nPerformance Stats:")
        stats = status['stats']
        print(f"  Opportunities Found: {stats['opportunities_found']}")
        print(f"  Orders Placed: {stats['orders_placed']}")
        print(f"  Positions Entered: {stats['positions_entered']}")
        print(f"  Positions Exited: {stats['positions_exited']}")
        print(f"  Scan Cycles: {stats['scan_cycles']}")
        
        if status['last_scan_time']:
            print(f"Last Scan: {status['last_scan_time'].strftime('%H:%M:%S UTC')}")
        
        print("=" * 60)


# ======================
# CONSOLE TESTING
# ======================

def run_strategy_test(allocation: float = 1000.0, test_duration: int = 30):
    """Test the fat tail strategy"""
    print("\n" + "=" * 60)
    print(f"FAT TAIL STRATEGY TEST - ${allocation:.2f} allocation, {test_duration}s")
    print("=" * 60)
    
    try:
        strategy = FatTailStrategy(daily_portfolio_allocation=allocation)
        
        # Test allocation calculation
        print("\nTesting daily allocation calculation...")
        markets_found = strategy.calculate_daily_allocations()
        print(f"✓ Found {markets_found} markets in range")
        
        if markets_found == 0:
            print("✗ No markets available for testing")
            return False
        
        # Show allocations
        print("\nMarket allocations:")
        for ticker, amount in list(strategy.daily_allocation_per_market.items())[:3]:
            print(f"  {ticker}: ${amount:.2f}")
        
        # Test opportunity scanning
        print(f"\nTesting opportunity scanning for {test_duration} seconds...")
        strategy.is_running = True
        
        for i in range(test_duration):
            strategy.scan_for_opportunities()
            strategy.monitor_existing_positions()
            time.sleep(1)
            
            if i % 10 == 9:  # Progress update every 10 seconds
                print(f"  ... {i+1}/{test_duration} seconds")
        
        strategy.is_running = False
        
        # Show final status
        print("\nFinal strategy status:")
        strategy.print_strategy_status()
        
        print("\n✓ Fat Tail Strategy test completed successfully")
        return True
        
    except Exception as e:
        print(f"✗ Fat Tail Strategy test failed: {e}")
        return False


if __name__ == "__main__":
    print("strategies/fat_tail_strategy.py loaded successfully!")
    print("\nQuick test commands:")
    print("  run_strategy_test()              - Test strategy with default allocation")
    print("  run_strategy_test(2000.0, 60)    - Test with $2000 allocation, 60 seconds")
    print("  strategy.start_strategy()        - Start live strategy")
    print("  strategy.print_strategy_status() - Show current status")
    print("\nRun: run_strategy_test()")