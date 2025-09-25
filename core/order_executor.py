# core/order_executor.py - Central Order Coordination System
"""
Sports Trading System 2.0 - Order Executor
Handles: Database queue processing, Kalshi API integration, order lifecycle management
Features: Priority processing, timeout management, conflict detection, strategy interface
"""

import threading
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
import json
import logging

# Core component imports
from core.database import DatabaseManager
from core.api_client import ExchangeClient
from configs.config_base import get_kalshi_config, get_system_config

# Cryptography for API client initialization  
from cryptography.hazmat.primitives import serialization


class OrderExecutorError(Exception):
    """Custom exception for order executor errors"""
    pass


class OrderRequest:
    """Data structure for order requests with validation"""
    
    def __init__(self, order_data: Dict[str, Any]):
        # Required fields
        self.request_uuid = order_data.get('request_uuid')
        self.strategy_id = order_data['strategy_id']
        self.ticker = order_data['ticker']
        self.side = order_data['side']  # 'yes', 'no'
        self.action = order_data['action']  # 'buy', 'sell'
        self.order_type = order_data['order_type']  # 'limit', 'market'
        self.quantity = order_data['quantity']
        self.price = order_data.get('price')  # None for market orders
        
        # Strategy context
        self.strategy_action = order_data['strategy_action']
        self.strategy_reason = order_data['strategy_reason']
        self.priority = order_data.get('priority', 5)
        
        # Timeout handling
        self.timeout_seconds = order_data.get('timeout_seconds', 300)  # Default 5 minutes
        self.use_kalshi_expiration = order_data.get('use_kalshi_expiration', True)
        
        # Status tracking
        self.status = order_data.get('status', 'pending')
        self.created_at = order_data.get('created_at')
        self.processed_at = order_data.get('processed_at')
        self.placed_at = order_data.get('placed_at')
        self.kalshi_order_id = order_data.get('kalshi_order_id')
        
        # Validate required fields
        self._validate()
    
    def _validate(self):
        """Validate order request data"""
        if self.side not in ['yes', 'no']:
            raise OrderExecutorError(f"Invalid side: {self.side}")
        
        if self.action not in ['buy', 'sell']:
            raise OrderExecutorError(f"Invalid action: {self.action}")
        
        if self.order_type not in ['limit', 'market']:
            raise OrderExecutorError(f"Invalid order type: {self.order_type}")
        
        if self.order_type == 'limit' and self.price is None:
            raise OrderExecutorError("Limit orders require price")
        
        if self.quantity <= 0:
            raise OrderExecutorError("Quantity must be positive")
        
        if self.priority < 1 or self.priority > 10:
            raise OrderExecutorError("Priority must be between 1-10")


class OrderConflictChecker:
    """Handles conflict detection for order requests"""
    
    def __init__(self, database: DatabaseManager, logger: logging.Logger):
        self.database = database
        self.logger = logger
    
    def check_conflicts(self, order_request: OrderRequest) -> Tuple[bool, str, Dict]:
        """
        Check for conflicts with existing positions and pending orders
        Returns: (has_conflict, conflict_reason, conflict_details)
        """
        conflicts = []
        conflict_details = {}
        
        # Check existing positions
        position_conflict = self._check_position_conflict(order_request)
        if position_conflict:
            conflicts.append("existing_position")
            conflict_details['position'] = position_conflict
        
        # Check pending orders
        pending_conflict = self._check_pending_order_conflict(order_request)
        if pending_conflict:
            conflicts.append("pending_order")
            conflict_details['pending_order'] = pending_conflict
        
        # Check order size limits (basic risk management)
        size_conflict = self._check_order_size_limits(order_request)
        if size_conflict:
            conflicts.append("size_limit")
            conflict_details['size_limit'] = size_conflict
        
        has_conflict = len(conflicts) > 0
        conflict_reason = "; ".join(conflicts) if conflicts else ""
        
        return has_conflict, conflict_reason, conflict_details
    
    def _check_position_conflict(self, order_request: OrderRequest) -> Optional[Dict]:
        """Check if order conflicts with existing position"""
        try:
            current_position = self.database.get_latest_position(
                order_request.strategy_id, 
                order_request.ticker
            )
            
            if not current_position or current_position.get('position', 0) == 0:
                return None  # No existing position
            
            current_size = current_position['position']
            
            # Check for conflicting directions
            if order_request.action == 'buy' and current_size < 0:
                return {
                    'type': 'direction_conflict',
                    'current_position': current_size,
                    'proposed_action': order_request.action,
                    'details': 'Buying when short position exists'
                }
            
            if order_request.action == 'sell' and current_size > 0:
                # This is actually fine - selling existing position
                pass
            
            if order_request.action == 'sell' and abs(order_request.quantity) > abs(current_size):
                return {
                    'type': 'oversell_attempt', 
                    'current_position': current_size,
                    'proposed_quantity': order_request.quantity,
                    'details': 'Attempting to sell more than current position'
                }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Position conflict check failed for {order_request.ticker}: {e}")
            return {
                'type': 'check_error',
                'error': str(e),
                'details': 'Unable to verify position conflict'
            }
    
    def _check_pending_order_conflict(self, order_request: OrderRequest) -> Optional[Dict]:
        """Check if order conflicts with pending orders"""
        try:
            with self.database.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                    SELECT COUNT(*), array_agg(request_uuid) as order_ids
                    FROM order_requests 
                    WHERE strategy_id = %s AND ticker = %s 
                    AND status IN ('pending', 'processing', 'placed')
                    AND side = %s AND action = %s
                    AND request_uuid != %s
                """, (
                    order_request.strategy_id,
                    order_request.ticker,
                    order_request.side,
                    order_request.action,
                    order_request.request_uuid
                ))
                    
                    result = cur.fetchone()
                    pending_count = result[0] if result else 0
                    
                    if pending_count > 0:
                        return {
                            'type': 'duplicate_order',
                            'pending_count': pending_count,
                            'existing_orders': result[1],
                            'details': f'{pending_count} similar orders already pending'
                        }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Pending order conflict check failed for {order_request.ticker}: {e}")
            return {
                'type': 'check_error',
                'error': str(e),
                'details': 'Unable to verify pending order conflicts'
            }
    
    def _check_order_size_limits(self, order_request: OrderRequest) -> Optional[Dict]:
        """Check basic order size limits"""
        system_config = get_system_config()
        
        # Estimate order value (rough check)
        estimated_price = order_request.price if order_request.price else 0.50  # Default mid-price
        estimated_value = order_request.quantity * estimated_price
        
        max_position_value = system_config.get('max_position_value', 500)
        
        if estimated_value > max_position_value:
            return {
                'type': 'size_limit_exceeded',
                'estimated_value': estimated_value,
                'max_allowed': max_position_value,
                'details': f'Order value ${estimated_value:.2f} exceeds limit ${max_position_value:.2f}'
            }
        
        return None


class OrderExecutor:
    """
    Central Order Executor for multi-strategy coordination
    Handles complete order lifecycle from strategy submission to completion
    """
    
    def __init__(self):
        """Initialize order executor with core components"""
        
        # Logging (standard logging to avoid circular imports)
        self.logger = logging.getLogger('order_executor')
        self.logger.setLevel(logging.INFO)
        
        # Core components
        self.database = DatabaseManager()
        self.conflict_checker = OrderConflictChecker(self.database, self.logger)
        
        # Initialize API client
        self._initialize_api_client()
        
        # Configuration
        self.system_config = get_system_config()
        
        # Processing control
        self.is_running = False
        self.processing_thread = None
        self.timeout_thread = None
        self.status_monitor_thread = None
        self.shutdown_event = threading.Event()
        
        # Thread safety
        self._processing_lock = threading.Lock()
        
        # Performance tracking
        self.stats = {
            'requests_submitted': 0,
            'orders_placed': 0,
            'orders_filled': 0,
            'orders_cancelled': 0,
            'orders_rejected': 0,
            'orders_timeout': 0,
            'conflicts_detected': 0,
            'processing_errors': 0,
            'priority_overrides': 0,
            'last_processing_time': None
        }
        
        self.logger.info("Order Executor initialized")
    
    def _initialize_api_client(self):
        """Initialize Kalshi API client for order placement"""
        try:
            kalshi_config = get_kalshi_config()
            
            # Load private key
            with open(kalshi_config['private_key_path'], 'rb') as f:
                private_key = serialization.load_pem_private_key(f.read(), password=None)
            
            # Initialize ExchangeClient
            self.api_client = ExchangeClient(
                exchange_api_base=kalshi_config['api_host'],
                key_id=kalshi_config['key_id'],
                private_key=private_key
            )
            
            self.logger.info(f"API client initialized: {kalshi_config['api_host']}")
            
        except Exception as e:
            raise OrderExecutorError(f"Failed to initialize API client: {e}")
    
    # ======================
    # STRATEGY INTERFACE
    # ======================
    
    def submit_order_request(self, order_data: Dict[str, Any]) -> str:
        """
        Submit order request from strategy
        Returns: request_uuid for tracking
        """
        try:
            # Generate unique request ID
            request_uuid = str(uuid.uuid4())
            order_data['request_uuid'] = request_uuid
            
            # Validate order request
            order_request = OrderRequest(order_data)
            
            # Store in database using schema function
            with self.database.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT submit_order_request(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        order_request.strategy_id,
                        order_request.ticker,
                        order_request.side,
                        order_request.action,
                        order_request.order_type,
                        order_request.quantity,
                        order_request.price,
                        order_request.strategy_action,
                        order_request.strategy_reason
                    ))
                    
                    db_request_uuid = cur.fetchone()[0]
                    
                    # Update with additional fields not in schema function
                    cur.execute("""
                        UPDATE order_requests 
                        SET priority = %s, request_uuid = %s
                        WHERE request_uuid = %s
                    """, (order_request.priority, request_uuid, db_request_uuid))
                    
                    conn.commit()
            
            self.stats['requests_submitted'] += 1
            
            self.logger.info(f"Order request submitted: {request_uuid} - {order_request.ticker} {order_request.action} {order_request.quantity} @ {order_request.price}")
            
            return request_uuid
            
        except Exception as e:
            self.logger.error(f"Failed to submit order request: {e}")
            raise OrderExecutorError(f"Order submission failed: {e}")
    
    def cancel_order_request(self, request_uuid: str, reason: str = "strategy_cancelled") -> bool:
        """Cancel pending order request"""
        try:
            with self.database.get_connection() as conn:
                with conn.cursor() as cur:
                    # Get current order info
                    cur.execute("""
                        SELECT status, kalshi_order_id, ticker
                        FROM order_requests 
                        WHERE request_uuid = %s
                    """, (request_uuid,))
                    
                    result = cur.fetchone()
                    if not result:
                        self.logger.warning(f"Order request not found: {request_uuid}")
                        return False
                    
                    status, kalshi_order_id, ticker = result
                    
                    # Handle based on current status
                    if status == 'pending':
                        # Just update database - order not yet placed with Kalshi
                        cur.execute("""
                            UPDATE order_requests 
                            SET status = 'cancelled', cancellation_reason = %s, cancelled_at = CURRENT_TIMESTAMP
                            WHERE request_uuid = %s
                        """, (reason, request_uuid))
                        
                    elif status == 'placed' and kalshi_order_id:
                        # Cancel with Kalshi API
                        try:
                            cancel_response = self.api_client.cancel_order(kalshi_order_id)
                            
                            cur.execute("""
                                UPDATE order_requests 
                                SET status = 'cancelled', cancellation_reason = %s, cancelled_at = CURRENT_TIMESTAMP
                                WHERE request_uuid = %s
                            """, (reason, request_uuid))
                            
                            self.logger.info(f"Cancelled Kalshi order: {kalshi_order_id} for {ticker}")
                            
                        except Exception as api_error:
                            self.logger.error(f"Failed to cancel Kalshi order {kalshi_order_id}: {api_error}")
                            return False
                    
                    else:
                        self.logger.warning(f"Cannot cancel order in status: {status}")
                        return False
                    
                    conn.commit()
                    self.stats['orders_cancelled'] += 1
                    
                    return True
            
        except Exception as e:
            self.logger.error(f"Failed to cancel order request {request_uuid}: {e}")
            return False
    
    def get_order_status(self, request_uuid: str) -> Optional[Dict[str, Any]]:
        """Get current status of order request"""
        try:
            with self.database.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT status, kalshi_order_id, placed_at, filled_at, cancelled_at, 
                               rejection_reason, cancellation_reason, conflicts_with_existing_position,
                               conflicts_with_pending_order, conflict_details
                        FROM order_requests 
                        WHERE request_uuid = %s
                    """, (request_uuid,))
                    
                    result = cur.fetchone()
                    if not result:
                        return None
                    
                    return {
                        'request_uuid': request_uuid,
                        'status': result[0],
                        'kalshi_order_id': result[1],
                        'placed_at': result[2],
                        'filled_at': result[3],
                        'cancelled_at': result[4],
                        'rejection_reason': result[5],
                        'cancellation_reason': result[6],
                        'conflicts_with_existing_position': result[7],
                        'conflicts_with_pending_order': result[8],
                        'conflict_details': result[9]
                    }
                    
        except Exception as e:
            self.logger.error(f"Failed to get order status {request_uuid}: {e}")
            return None
    
    # ======================
    # ORDER PROCESSING
    # ======================
    
    def process_pending_orders(self) -> int:
        """
        Process pending orders from database queue
        Returns number of orders processed
        """
        processed_count = 0
        
        with self._processing_lock:
            try:
                # Get next pending order (priority-based)
                with self.database.get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT request_uuid, strategy_id, ticker, side, action, order_type, 
                                   quantity, price, strategy_action, strategy_reason, priority,
                                   created_at
                            FROM order_requests 
                            WHERE status = 'pending'
                            ORDER BY priority ASC, created_at ASC
                            LIMIT 5
                        """)
                        
                        pending_orders = cur.fetchall()
                
                for order_row in pending_orders:
                    try:
                        # Convert to OrderRequest object
                        order_data = {
                            'request_uuid': order_row[0],
                            'strategy_id': order_row[1],
                            'ticker': order_row[2],
                            'side': order_row[3],
                            'action': order_row[4],
                            'order_type': order_row[5],
                            'quantity': order_row[6],
                            'price': order_row[7],
                            'strategy_action': order_row[8],
                            'strategy_reason': order_row[9],
                            'priority': order_row[10],
                            'created_at': order_row[11],
                            'status': 'pending'
                        }
                        
                        order_request = OrderRequest(order_data)
                        
                        # Process the order
                        success = self._process_single_order(order_request)
                        if success:
                            processed_count += 1
                            
                    except Exception as e:
                        self.logger.error(f"Failed to process order {order_row[0]}: {e}")
                        self._mark_order_rejected(order_row[0], f"Processing error: {e}")
                        continue
                
                if processed_count > 0:
                    self.stats['last_processing_time'] = datetime.now(timezone.utc)
                    
            except Exception as e:
                self.logger.error(f"Order processing failed: {e}")
                self.stats['processing_errors'] += 1
        
        return processed_count
    
    def _process_single_order(self, order_request: OrderRequest) -> bool:
        """Process a single order request"""
        try:
            # FIXED: Immediate atomic status update to prevent race conditions
            with self.database.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE order_requests 
                        SET status = 'processing', processed_at = CURRENT_TIMESTAMP
                        WHERE request_uuid = %s
                    """, (order_request.request_uuid,))
                    conn.commit()  # Force immediate commit
            
            # Check for conflicts
            has_conflict, conflict_reason, conflict_details = self.conflict_checker.check_conflicts(order_request)
            
            if has_conflict:
                # Try priority override for pending order conflicts only
                if 'pending_order' in conflict_reason and order_request.priority <= 4:  # Only high priority orders can override
                    override_success, override_reason = self._handle_priority_override(order_request)
                    if override_success:
                        self.logger.info(f"Priority override successful for {order_request.ticker}: {override_reason}")
                        # Continue processing - conflict resolved
                    else:
                        self.logger.warning(f"Priority override failed for {order_request.ticker}: {override_reason}")
                        self._mark_order_rejected(order_request.request_uuid, f"Conflicts: {conflict_reason} (override failed: {override_reason})", conflict_details)
                        self.stats['conflicts_detected'] += 1
                        return False
                else:
                    self.logger.warning(f"Order conflicts detected for {order_request.ticker}: {conflict_reason}")
                    self._mark_order_rejected(order_request.request_uuid, f"Conflicts: {conflict_reason}", conflict_details)
                    self.stats['conflicts_detected'] += 1
                    return False
            
            # Place order with Kalshi
            kalshi_order_id = self._place_order_with_kalshi(order_request)
            
            if kalshi_order_id:
                # Update database with successful placement
                self._mark_order_placed(order_request.request_uuid, kalshi_order_id)
                self.stats['orders_placed'] += 1
                
                self.logger.info(f"Order placed successfully: {order_request.ticker} - Kalshi ID: {kalshi_order_id}")
                return True
            else:
                self._mark_order_rejected(order_request.request_uuid, "Kalshi API placement failed")
                self.stats['orders_rejected'] += 1
                return False
                
        except Exception as e:
            self.logger.error(f"Single order processing failed for {order_request.request_uuid}: {e}")
            self._mark_order_rejected(order_request.request_uuid, f"Processing exception: {e}")
            return False
    
    def _place_order_with_kalshi(self, order_request: OrderRequest) -> Optional[str]:
        """Place order with Kalshi API"""
        try:
            # Calculate expiration if using Kalshi timeout
            expiration_ts = None
            if order_request.use_kalshi_expiration and order_request.timeout_seconds:
                expiration_ts = int(time.time()) + order_request.timeout_seconds
            
            # Generate client order ID
            import uuid
            client_order_id = f"executor_{order_request.strategy_id}_{str(uuid.uuid4())[:8]}"
            
            # Prepare order parameters
            order_params = {
                'ticker': order_request.ticker,
                'client_order_id': client_order_id,
                'side': order_request.side,
                'action': order_request.action,
                'count': order_request.quantity,
                'type': order_request.order_type
            }
            
            # Add price and expiration based on order type
            if order_request.order_type == 'limit':
                if order_request.side == 'yes':
                    order_params['yes_price'] = int(order_request.price * 100)  # Convert to cents
                else:
                    order_params['no_price'] = int(order_request.price * 100)
                
                if expiration_ts:
                    order_params['expiration_ts'] = expiration_ts
            
            # Place order with Kalshi
            response = self.api_client.create_order(**order_params)
            
            if response and 'order' in response:
                kalshi_order_id = response['order']['order_id']
                self.logger.debug(f"Kalshi order placed: {kalshi_order_id} for {order_request.ticker}")
                return kalshi_order_id
            else:
                self.logger.error(f"Invalid Kalshi response for {order_request.ticker}: {response}")
                return None
                
        except Exception as e:
            self.logger.error(f"Kalshi order placement failed for {order_request.ticker}: {e}")
            return None
    
    def _handle_priority_override(self, new_order: OrderRequest) -> Tuple[bool, str]:
        """Handle priority-based conflict resolution"""
        try:
            with self.database.get_connection() as conn:
                with conn.cursor() as cur:
                    # Find lower priority conflicting orders
                    cur.execute("""
                        SELECT request_uuid, priority, strategy_action
                        FROM order_requests 
                        WHERE strategy_id = %s AND ticker = %s 
                        AND status IN ('pending', 'processing') 
                        AND priority > %s
                        AND side = %s AND action = %s
                    """, (
                        new_order.strategy_id, new_order.ticker, 
                        new_order.priority, new_order.side, new_order.action
                    ))
                    
                    lower_priority_orders = cur.fetchall()
                    
                    if lower_priority_orders:
                        # Cancel lower priority orders
                        cancelled_count = 0
                        for uuid, old_priority, old_action in lower_priority_orders:
                            if self.cancel_order_request(uuid, f"priority_override_P{new_order.priority}_over_P{old_priority}"):
                                cancelled_count += 1
                        
                        self.logger.info(f"Priority override: P{new_order.priority} cancelled {cancelled_count} P>{new_order.priority} orders for {new_order.ticker}")
                        self.stats['priority_overrides'] += cancelled_count
                        return True, f"Cancelled {cancelled_count} lower priority orders"
                    
                    return False, "No lower priority orders to override"
                    
        except Exception as e:
            self.logger.error(f"Priority override check failed: {e}")
            return False, f"Override check failed: {e}"
    
    # ======================
    # STATUS MANAGEMENT
    # ======================
    
    def _update_order_status(self, request_uuid: str, new_status: str):
        """Update order status in database"""
        try:
            with self.database.get_connection() as conn:
                with conn.cursor() as cur:
                    if new_status == 'processing':
                        cur.execute("""
                            UPDATE order_requests 
                            SET status = %s, processed_at = CURRENT_TIMESTAMP
                            WHERE request_uuid = %s
                        """, (new_status, request_uuid))
                    else:
                        cur.execute("""
                            UPDATE order_requests 
                            SET status = %s, updated_at = CURRENT_TIMESTAMP
                            WHERE request_uuid = %s
                        """, (new_status, request_uuid))
                    
                    conn.commit()
                    
        except Exception as e:
            self.logger.error(f"Failed to update order status {request_uuid} to {new_status}: {e}")
    
    def _mark_order_placed(self, request_uuid: str, kalshi_order_id: str):
        """Mark order as successfully placed"""
        try:
            with self.database.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE order_requests 
                        SET status = 'placed', kalshi_order_id = %s, placed_at = CURRENT_TIMESTAMP
                        WHERE request_uuid = %s
                    """, (kalshi_order_id, request_uuid))
                    
                    conn.commit()
                    
        except Exception as e:
            self.logger.error(f"Failed to mark order placed {request_uuid}: {e}")
    
    def _mark_order_rejected(self, request_uuid: str, rejection_reason: str, conflict_details: Dict = None):
        """Mark order as rejected with reason"""
        try:
            with self.database.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE order_requests 
                        SET status = 'rejected', rejection_reason = %s, 
                            conflicts_with_existing_position = %s,
                            conflicts_with_pending_order = %s,
                            conflict_details = %s,
                            processed_at = CURRENT_TIMESTAMP
                        WHERE request_uuid = %s
                    """, (
                        rejection_reason,
                        conflict_details and 'existing_position' in conflict_details,
                        conflict_details and 'pending_order' in conflict_details,
                        json.dumps(conflict_details) if conflict_details else None,
                        request_uuid
                    ))
                    
                    conn.commit()
                    
        except Exception as e:
            self.logger.error(f"Failed to mark order rejected {request_uuid}: {e}")
    
    # ======================
    # BACKGROUND MONITORING
    # ======================
    
    def monitor_order_timeouts(self):
        """Monitor and cancel orders that exceed timeout"""
        try:
            with self.database.get_connection() as conn:
                with conn.cursor() as cur:
                    # Find orders that should timeout (client-side backup)
                    cur.execute("""
                        SELECT request_uuid, ticker, kalshi_order_id
                        FROM order_requests 
                        WHERE status = 'placed' 
                        AND placed_at < NOW() - INTERVAL '10 minutes'
                        AND kalshi_order_id IS NOT NULL
                    """)
                    
                    timeout_orders = cur.fetchall()
            
            for request_uuid, ticker, kalshi_order_id in timeout_orders:
                try:
                    # Check order status with Kalshi first
                    order_status = self.api_client.get_order(kalshi_order_id)
                    
                    if order_status and order_status.get('order', {}).get('status') == 'resting':
                        # Order still resting - cancel it
                        self.api_client.cancel_order(kalshi_order_id)
                        
                        # Update database
                        with self.database.get_connection() as conn:
                            with conn.cursor() as cur:
                                cur.execute("""
                                    UPDATE order_requests 
                                    SET status = 'cancelled', cancellation_reason = 'timeout',
                                        cancelled_at = CURRENT_TIMESTAMP
                                    WHERE request_uuid = %s
                                """, (request_uuid,))
                                conn.commit()
                        
                        self.stats['orders_timeout'] += 1
                        self.logger.info(f"Timeout cancelled: {ticker} - {kalshi_order_id}")
                        
                except Exception as e:
                    self.logger.error(f"Timeout handling failed for {request_uuid}: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Timeout monitoring failed: {e}")
    
    def monitor_order_status_updates(self):
        """Monitor Kalshi orders for fill/cancel updates"""
        try:
            with self.database.get_connection() as conn:
                with conn.cursor() as cur:
                    # Get placed orders that need status checking
                    cur.execute("""
                        SELECT request_uuid, kalshi_order_id, ticker
                        FROM order_requests 
                        WHERE status = 'placed' AND kalshi_order_id IS NOT NULL
                        ORDER BY placed_at DESC
                        LIMIT 20
                    """)
                    
                    active_orders = cur.fetchall()
            
            for request_uuid, kalshi_order_id, ticker in active_orders:
                try:
                    # Check status with Kalshi
                    order_status = self.api_client.get_order(kalshi_order_id)
                    
                    if not order_status or 'order' not in order_status:
                        continue
                    
                    kalshi_order = order_status['order']
                    status = kalshi_order.get('status')
                    
                    # Update database based on Kalshi status
                    if status == 'filled':
                        with self.database.get_connection() as conn:
                            with conn.cursor() as cur:
                                cur.execute("""
                                    UPDATE order_requests 
                                    SET status = 'filled', filled_at = CURRENT_TIMESTAMP
                                    WHERE request_uuid = %s AND status != 'filled'
                                """, (request_uuid,))
                                conn.commit()
                        
                        if cur.rowcount > 0:
                            self.stats['orders_filled'] += 1
                            self.logger.info(f"Order filled: {ticker} - {kalshi_order_id}")
                    
                    elif status == 'canceled':
                        with self.database.get_connection() as conn:
                            with conn.cursor() as cur:
                                cur.execute("""
                                    UPDATE order_requests 
                                    SET status = 'cancelled', cancellation_reason = 'kalshi_cancelled',
                                        cancelled_at = CURRENT_TIMESTAMP
                                    WHERE request_uuid = %s AND status = 'placed'
                                """, (request_uuid,))
                                conn.commit()
                        
                        if cur.rowcount > 0:
                            self.stats['orders_cancelled'] += 1
                            self.logger.info(f"Order cancelled (Kalshi): {ticker} - {kalshi_order_id}")
                    
                except Exception as e:
                    self.logger.error(f"Status update failed for {kalshi_order_id}: {e}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Status monitoring failed: {e}")
    
    # ======================
    # LIFECYCLE MANAGEMENT
    # ======================
    
    def start_processing(self):
        """Start background order processing"""
        if self.is_running:
            self.logger.warning("Order processing already running")
            return
        
        try:
            self.is_running = True
            
            # Start main processing thread (every 2 seconds)
            self.processing_thread = threading.Thread(
                target=self._processing_loop, 
                args=(2,), 
                daemon=True
            )
            self.processing_thread.start()
            
            # Start timeout monitoring thread (every 30 seconds)
            self.timeout_thread = threading.Thread(
                target=self._timeout_loop,
                args=(30,),
                daemon=True
            )
            self.timeout_thread.start()
            
            # Start status monitoring thread (every 10 seconds)
            self.status_monitor_thread = threading.Thread(
                target=self._status_monitor_loop,
                args=(10,),
                daemon=True
            )
            self.status_monitor_thread.start()
            
            self.logger.info("Order Executor processing started - 2s processing, 30s timeouts, 10s status updates")
            
        except Exception as e:
            self.logger.error(f"Failed to start order processing: {e}")
            self.is_running = False
            raise
    
    def stop_processing(self):
        """Stop background order processing"""
        if not self.is_running:
            return
        
        self.logger.info("Stopping Order Executor...")
        
        self.is_running = False
        self.shutdown_event.set()
        
        # Wait for threads to finish
        for thread, name in [
            (self.processing_thread, "processing"),
            (self.timeout_thread, "timeout"), 
            (self.status_monitor_thread, "status_monitor")
        ]:
            if thread and thread.is_alive():
                thread.join(timeout=5)
                if thread.is_alive():
                    self.logger.warning(f"{name} thread did not stop cleanly")
        
        self.logger.info("Order Executor stopped")
    
    def _processing_loop(self, interval_seconds: int):
        """Main processing loop"""
        self.logger.info(f"Order processing loop started ({interval_seconds}s interval)")
        
        while self.is_running and not self.shutdown_event.is_set():
            try:
                loop_start = time.time()
                
                # Process pending orders
                processed = self.process_pending_orders()
                
                if processed > 0:
                    self.logger.debug(f"Processed {processed} orders")
                
                # Maintain timing
                elapsed = time.time() - loop_start
                sleep_time = max(0, interval_seconds - elapsed)
                
                if sleep_time > 0:
                    if self.shutdown_event.wait(sleep_time):
                        break
                        
            except Exception as e:
                self.logger.error(f"Processing loop error: {e}")
                if self.shutdown_event.wait(interval_seconds):
                    break
        
        self.logger.info("Order processing loop ended")
    
    def _timeout_loop(self, interval_seconds: int):
        """Timeout monitoring loop"""
        self.logger.info(f"Timeout monitoring loop started ({interval_seconds}s interval)")
        
        while self.is_running and not self.shutdown_event.is_set():
            try:
                self.monitor_order_timeouts()
                
                if self.shutdown_event.wait(interval_seconds):
                    break
                    
            except Exception as e:
                self.logger.error(f"Timeout loop error: {e}")
                if self.shutdown_event.wait(interval_seconds):
                    break
        
        self.logger.info("Timeout monitoring loop ended")
    
    def _status_monitor_loop(self, interval_seconds: int):
        """Status monitoring loop"""
        self.logger.info(f"Status monitoring loop started ({interval_seconds}s interval)")
        
        while self.is_running and not self.shutdown_event.is_set():
            try:
                self.monitor_order_status_updates()
                
                if self.shutdown_event.wait(interval_seconds):
                    break
                    
            except Exception as e:
                self.logger.error(f"Status monitor loop error: {e}")
                if self.shutdown_event.wait(interval_seconds):
                    break
        
        self.logger.info("Status monitoring loop ended")
    
    # ======================
    # STATUS & MONITORING
    # ======================
    
    def get_executor_status(self) -> Dict[str, Any]:
        """Get current executor status and statistics"""
        return {
            'is_running': self.is_running,
            'threads': {
                'processing_alive': self.processing_thread.is_alive() if self.processing_thread else False,
                'timeout_alive': self.timeout_thread.is_alive() if self.timeout_thread else False,
                'status_monitor_alive': self.status_monitor_thread.is_alive() if self.status_monitor_thread else False
            },
            'stats': self.stats.copy()
        }
    
    def print_executor_status(self):
        """Print detailed executor status"""
        status = self.get_executor_status()
        
        print("\n" + "=" * 60)
        print("ORDER EXECUTOR STATUS")
        print("=" * 60)
        
        print(f"Running: {'✅' if status['is_running'] else '❌'}")
        
        threads = status['threads']
        print(f"Processing Thread: {'✅' if threads['processing_alive'] else '❌'}")
        print(f"Timeout Thread: {'✅' if threads['timeout_alive'] else '❌'}")
        print(f"Status Monitor: {'✅' if threads['status_monitor_alive'] else '❌'}")
        
        print("\nPerformance Statistics:")
        stats = status['stats']
        print(f"  Requests Submitted: {stats['requests_submitted']}")
        print(f"  Orders Placed: {stats['orders_placed']}")
        print(f"  Orders Filled: {stats['orders_filled']}")
        print(f"  Orders Cancelled: {stats['orders_cancelled']}")
        print(f"  Orders Rejected: {stats['orders_rejected']}")
        print(f"  Orders Timeout: {stats['orders_timeout']}")
        print(f"  Conflicts Detected: {stats['conflicts_detected']}")
        print(f"  Processing Errors: {stats['processing_errors']}")
        
        if stats['last_processing_time']:
            print(f"Last Processing: {stats['last_processing_time'].strftime('%H:%M:%S UTC')}")
        
        print("=" * 60)
    
    def __del__(self):
        """Cleanup on destruction"""
        self.stop_processing()


# ======================
# CONSOLE TESTING FUNCTIONS
# ======================

def test_order_executor_initialization():
    """Test executor initialization and basic functionality"""
    print("\n" + "=" * 60)
    print("ORDER EXECUTOR - INITIALIZATION TEST")
    print("=" * 60)
    
    try:
        executor = OrderExecutor()
        print("✅ Order Executor initialized")
        
        # Test API client
        balance = executor.api_client.get_balance()
        print(f"✅ API integration: Balance ${balance['balance'] / 100:.2f}")
        
        # Test database
        strategy_id = executor.database.get_strategy_id('fat_tail_statistical')
        print(f"✅ Database integration: Strategy ID {strategy_id}")
        
        return executor
        
    except Exception as e:
        print(f"❌ Initialization failed: {e}")
        return None

def test_order_submission_and_processing(executor: OrderExecutor = None):
    """Test complete order submission and processing flow"""
    print("\n" + "=" * 60) 
    print("ORDER EXECUTOR - ORDER SUBMISSION TEST")
    print("=" * 60)
    
    if not executor:
        executor = OrderExecutor()
    
    try:
        # Submit test order request
        test_order = {
            'strategy_id': 2,  # fat_tail_statistical
            'ticker': 'TEST-EXECUTOR-FLOW',
            'side': 'yes',
            'action': 'buy',
            'order_type': 'limit',
            'quantity': 5,
            'price': 0.35,
            'priority': 3,
            'timeout_seconds': 300,
            'strategy_action': 'test_entry',
            'strategy_reason': 'Testing order executor flow'
        }
        
        request_uuid = executor.submit_order_request(test_order)
        print(f"✅ Order submitted: {request_uuid}")
        
        # Check initial status
        status = executor.get_order_status(request_uuid)
        print(f"✅ Initial status: {status['status']}")
        
        # Test single processing cycle
        processed = executor.process_pending_orders()
        print(f"✅ Processing cycle: {processed} orders processed")
        
        # Check final status
        final_status = executor.get_order_status(request_uuid)
        print(f"✅ Final status: {final_status['status']}")
        
        if final_status['rejection_reason']:
            print(f"   Rejection reason: {final_status['rejection_reason']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Order submission test failed: {e}")
        return False

def test_conflict_detection(executor: OrderExecutor = None):
    """Test conflict detection logic"""
    print("\n" + "=" * 60)
    print("ORDER EXECUTOR - CONFLICT DETECTION TEST") 
    print("=" * 60)
    
    if not executor:
        executor = OrderExecutor()
    
    try:
        # Create duplicate order requests
        base_order = {
            'strategy_id': 2,
            'ticker': 'TEST-CONFLICT-DETECTION',
            'side': 'yes',
            'action': 'buy',
            'order_type': 'limit',
            'quantity': 10,
            'price': 0.40,
            'strategy_action': 'test_conflict',
            'strategy_reason': 'Testing conflict detection'
        }
        
        # Submit first order
        uuid1 = executor.submit_order_request(base_order.copy())
        print(f"✅ First order submitted: {uuid1}")
        
        # Submit identical order (should conflict)
        uuid2 = executor.submit_order_request(base_order.copy())
        print(f"✅ Second order submitted: {uuid2}")
        
        # Process orders
        processed = executor.process_pending_orders()
        print(f"✅ Processing completed: {processed} orders processed")
        
        # Check results
        status1 = executor.get_order_status(uuid1)
        status2 = executor.get_order_status(uuid2)
        
        print(f"Order 1 status: {status1['status']}")
        print(f"Order 2 status: {status2['status']}")
        
        if status2['status'] == 'rejected':
            print(f"✅ Conflict detected: {status2['rejection_reason']}")
            return True
        else:
            print("❌ Conflict detection failed - duplicate order not rejected")
            return False
            
    except Exception as e:
        print(f"❌ Conflict detection test failed: {e}")
        return False

def run_full_order_executor_test():
    """Complete order executor validation"""
    print("\n" + "=" * 80)
    print("ORDER EXECUTOR - COMPREHENSIVE TEST SUITE")
    print("=" * 80)
    
    test_results = []
    
    # Test 1: Initialization
    executor = test_order_executor_initialization()
    test_results.append(("Initialization", executor is not None))
    
    if not executor:
        print("\n❌ Cannot continue - initialization failed")
        return False
    
    # Test 2: Order submission and processing
    test_results.append(("Order Submission", test_order_submission_and_processing(executor)))
    
    # Test 3: Conflict detection  
    test_results.append(("Conflict Detection", test_conflict_detection(executor)))
    
    # Print summary
    print("\n" + "=" * 60)
    print("TEST RESULTS SUMMARY")
    print("=" * 60)
    
    passed = 0
    for test_name, result in test_results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name:>20}: {status}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{len(test_results)} tests passed")
    
    # Print final status
    executor.print_executor_status()
    
    print(f"\n{'='*60}")
    if passed == len(test_results):
        print("✅ ORDER EXECUTOR READY FOR INTEGRATION")
    else:
        print("❌ ORDER EXECUTOR NEEDS FIXES")
    print("="*60)
    
    return passed == len(test_results), executor


# ======================
# CONSOLE USAGE
# ======================

if __name__ == "__main__":
    print("core/order_executor.py loaded successfully!")
    print("\nQuick test commands:")
    print("  test_order_executor_initialization()  - Test basic setup")
    print("  test_order_submission_and_processing() - Test order flow")
    print("  test_conflict_detection()             - Test conflict logic")
    print("  run_full_order_executor_test()        - Complete validation")
    print("  executor.start_processing()           - Start background processing")
    print("  executor.print_executor_status()      - Show current status")
    print("\nRun: run_full_order_executor_test()")