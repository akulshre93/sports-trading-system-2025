-- =====================================================
-- SPORTS TRADING SYSTEM 2.0 - DATABASE SCHEMA
-- Multi-strategy position and order tracking
-- =====================================================

-- =====================================================
-- REFERENCE TABLES
-- =====================================================

-- Sports reference for game timing calculations
CREATE TABLE sports (
    id SERIAL PRIMARY KEY,
    sport_name VARCHAR(50) UNIQUE NOT NULL, -- 'baseball', 'basketball', etc.
    ticker_prefix VARCHAR(20) NOT NULL, -- 'KXMLBGAME', 'KXNBAGAME', etc.
    assumed_game_length_minutes INTEGER NOT NULL, -- 180 for baseball, 150 for basketball
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Markets table - tracks all markets we're monitoring
CREATE TABLE markets (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(100) UNIQUE NOT NULL,
    event_ticker VARCHAR(100) NOT NULL,
    sport_id INTEGER REFERENCES sports(id),
    market_type VARCHAR(50) NOT NULL, -- 'binary', 'categorical', etc.
    event_name VARCHAR(200),
    settlement_time TIMESTAMP, -- Kalshi's estimated settlement time
    close_time TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Strategies reference table (for foreign keys only - config in code)
CREATE TABLE strategies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL, -- 'temporal_arbitrage', 'fat_tail_statistical'
    display_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- MARKET DATA HISTORY
-- =====================================================

-- Historical market data for analysis (memory intensive - implement later)
CREATE TABLE market_data_history (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(100) NOT NULL,
    sport_id INTEGER REFERENCES sports(id),
    
    -- Price data
    yes_bid DECIMAL(4,2),
    yes_ask DECIMAL(4,2),
    no_bid DECIMAL(4,2), 
    no_ask DECIMAL(4,2),
    last_trade_price DECIMAL(4,2),
    last_trade_side VARCHAR(3), -- 'yes', 'no'
    
    -- Volume data
    yes_volume INTEGER DEFAULT 0,
    no_volume INTEGER DEFAULT 0,
    total_volume INTEGER DEFAULT 0,
    
    -- Timing context
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    relative_min_to_game_start INTEGER, -- Positive = game running, Negative = game starts in X min
    
    -- Performance indexes
    INDEX idx_market_data_ticker_time (ticker, timestamp),
    INDEX idx_market_data_relative_time (relative_min_to_game_start),
    INDEX idx_market_data_sport_time (sport_id, timestamp)
);

-- =====================================================
-- TRADING ACTIVITY
-- =====================================================

-- Current positions - real-time position tracking
CREATE TABLE positions (
    id SERIAL PRIMARY KEY,
    position_uuid UUID UNIQUE DEFAULT gen_random_uuid(), -- Our unique identifier
    strategy_id INTEGER REFERENCES strategies(id),
    ticker VARCHAR(100) NOT NULL,
    event_ticker VARCHAR(100),
    
    -- Kalshi API fields (direct mapping)
    total_traded INTEGER DEFAULT 0,
    total_traded_dollars_cents INTEGER DEFAULT 0, -- Store as cents to avoid decimal issues
    position INTEGER DEFAULT 0, -- Current position (+ long, - short, 0 flat)
    market_exposure INTEGER DEFAULT 0,
    market_exposure_dollars_cents INTEGER DEFAULT 0,
    realized_pnl INTEGER DEFAULT 0, -- In cents
    realized_pnl_dollars_cents INTEGER DEFAULT 0,
    resting_orders_count INTEGER DEFAULT 0,
    fees_paid INTEGER DEFAULT 0, -- In cents
    fees_paid_dollars_cents INTEGER DEFAULT 0,
    kalshi_last_updated TIMESTAMP,
    
    -- Our additional tracking fields
    entry_price DECIMAL(4,2), -- Our average entry price
    entry_time TIMESTAMP,
    exit_price DECIMAL(4,2), -- Average exit price (if closed)
    exit_time TIMESTAMP,
    status VARCHAR(20) DEFAULT 'open', -- 'open', 'closed', 'partial'
    stop_loss_price DECIMAL(4,2),
    take_profit_price DECIMAL(4,2),
    
    -- Strategy context (inherited from orders)
    strategy_action VARCHAR(50), -- 'entry', 'exit', 'stop_loss', 'take_profit'
    strategy_reason TEXT, -- Free text from strategy (e.g., 'momentum_detected', 'statistical_fade')
    confidence_score DECIMAL(3,2), -- Strategy confidence 0.00-1.00
    notes TEXT,
    
    -- Audit trail
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes for fast queries
    INDEX idx_positions_strategy_ticker (strategy_id, ticker),
    INDEX idx_positions_status (status),
    INDEX idx_positions_entry_time (entry_time),
    
    -- Ensure one position per strategy per ticker
    UNIQUE(strategy_id, ticker)
);

-- All orders placed (both filled and unfilled)
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    order_uuid UUID UNIQUE DEFAULT gen_random_uuid(),
    position_id INTEGER REFERENCES positions(id), -- Links to position (NULL if position not created yet)
    strategy_id INTEGER REFERENCES strategies(id),
    ticker VARCHAR(100) NOT NULL,
    
    -- Order details
    kalshi_order_id VARCHAR(100), -- Kalshi's order ID
    side VARCHAR(10) NOT NULL, -- 'yes', 'no'
    action VARCHAR(10) NOT NULL, -- 'buy', 'sell'
    order_type VARCHAR(20) DEFAULT 'limit', -- 'limit', 'market'
    quantity INTEGER NOT NULL,
    price DECIMAL(4,2), -- Order price
    
    -- Order status tracking
    status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'filled', 'cancelled', 'rejected'
    filled_quantity INTEGER DEFAULT 0,
    average_fill_price DECIMAL(4,2),
    fees_paid_cents INTEGER DEFAULT 0,
    
    -- Timing
    placed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    filled_at TIMESTAMP,
    cancelled_at TIMESTAMP,
    
    -- Strategy context (captured at order creation)
    strategy_action VARCHAR(50) NOT NULL, -- 'entry', 'exit', 'stop_loss', 'take_profit'
    strategy_reason TEXT NOT NULL, -- Free text from strategy executor
    
    -- Indexes
    INDEX idx_orders_strategy_ticker (strategy_id, ticker),
    INDEX idx_orders_status (status),
    INDEX idx_orders_kalshi_id (kalshi_order_id),
    INDEX idx_orders_placed_at (placed_at)
);

-- =====================================================
-- PERFORMANCE TRACKING
-- =====================================================

-- Daily performance summary by strategy
CREATE TABLE daily_performance (
    id SERIAL PRIMARY KEY,
    strategy_id INTEGER REFERENCES strategies(id),
    trade_date DATE NOT NULL,
    
    -- P&L metrics
    realized_pnl_cents INTEGER DEFAULT 0,
    unrealized_pnl_cents INTEGER DEFAULT 0,
    total_pnl_cents INTEGER DEFAULT 0,
    fees_paid_cents INTEGER DEFAULT 0,
    net_pnl_cents INTEGER DEFAULT 0, -- total_pnl - fees
    
    -- Activity metrics
    positions_opened INTEGER DEFAULT 0,
    positions_closed INTEGER DEFAULT 0,
    orders_placed INTEGER DEFAULT 0,
    orders_filled INTEGER DEFAULT 0,
    
    -- Portfolio metrics
    starting_balance_cents INTEGER,
    ending_balance_cents INTEGER,
    max_exposure_cents INTEGER, -- Maximum exposure during the day
    
    -- Performance ratios (calculated fields)
    win_rate DECIMAL(5,2), -- Percentage of winning positions
    avg_win_cents INTEGER,
    avg_loss_cents INTEGER,
    profit_factor DECIMAL(5,2), -- Total wins / Total losses
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure one record per strategy per day
    UNIQUE(strategy_id, trade_date)
);

-- =====================================================
-- SYSTEM MONITORING
-- =====================================================

-- System health and operational metrics
CREATE TABLE system_health (
    id SERIAL PRIMARY KEY,
    component VARCHAR(50) NOT NULL, -- 'main', 'temporal_arbitrage', 'fat_tail', etc.
    status VARCHAR(20) NOT NULL, -- 'healthy', 'warning', 'error'
    message TEXT,
    metric_name VARCHAR(50),
    metric_value DECIMAL(10,2),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_health_component_time (component, timestamp),
    INDEX idx_health_status (status)
);

-- Market data collection health
CREATE TABLE data_collection_health (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL, -- 'kalshi_api', 'market_cache'
    markets_monitored INTEGER DEFAULT 0,
    last_update_time TIMESTAMP,
    update_frequency_seconds INTEGER,
    error_count INTEGER DEFAULT 0,
    success_rate DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- VIEWS FOR COMMON QUERIES
-- =====================================================

-- Current portfolio summary
CREATE VIEW portfolio_summary AS
SELECT 
    s.name as strategy_name,
    COUNT(p.id) as open_positions,
    SUM(p.market_exposure) as total_exposure_cents,
    SUM(p.realized_pnl) as total_realized_pnl_cents,
    SUM(p.fees_paid) as total_fees_paid_cents,
    AVG(p.confidence_score) as avg_confidence
FROM strategies s
LEFT JOIN positions p ON s.id = p.strategy_id AND p.status = 'open'
GROUP BY s.id, s.name;

-- Recent performance by strategy
CREATE VIEW recent_performance AS
SELECT 
    s.name as strategy_name,
    dp.trade_date,
    dp.net_pnl_cents,
    dp.positions_opened,
    dp.positions_closed,
    dp.win_rate,
    dp.profit_factor
FROM strategies s
JOIN daily_performance dp ON s.id = dp.strategy_id
WHERE dp.trade_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY dp.trade_date DESC, s.name;

-- Active orders summary
CREATE VIEW active_orders AS
SELECT 
    s.name as strategy_name,
    o.ticker,
    o.side,
    o.order_type,
    o.quantity,
    o.price,
    o.status,
    o.strategy_action,
    o.strategy_reason,
    o.placed_at
FROM strategies s
JOIN orders o ON s.id = o.strategy_id
WHERE o.status IN ('pending', 'partially_filled')
ORDER BY o.placed_at DESC;

-- Game timing context view
CREATE VIEW game_timing_context AS
SELECT 
    m.ticker,
    m.event_name,
    s.sport_name,
    m.settlement_time,
    (m.settlement_time - INTERVAL '1 minute' * s.assumed_game_length_minutes) as estimated_game_start,
    EXTRACT(EPOCH FROM (
        NOW() - (m.settlement_time - INTERVAL '1 minute' * s.assumed_game_length_minutes)
    ))/60 as current_relative_min_to_game_start
FROM markets m
JOIN sports s ON m.sport_id = s.id
WHERE m.is_active = true;

-- =====================================================
-- FUNCTIONS FOR COMMON OPERATIONS
-- =====================================================

-- Function to calculate relative minutes to game start
CREATE OR REPLACE FUNCTION calculate_relative_game_time(
    p_settlement_time TIMESTAMP,
    p_game_length_minutes INTEGER
) RETURNS INTEGER AS $$
BEGIN
    RETURN EXTRACT(EPOCH FROM (
        NOW() - (p_settlement_time - INTERVAL '1 minute' * p_game_length_minutes)
    ))/60;
END;
$$ LANGUAGE plpgsql;

-- Function to update position from Kalshi API data
CREATE OR REPLACE FUNCTION update_position_from_kalshi(
    p_ticker VARCHAR(100),
    p_strategy_id INTEGER,
    p_kalshi_data JSONB
) RETURNS VOID AS $$
BEGIN
    INSERT INTO positions (
        strategy_id, ticker, event_ticker,
        total_traded, position, market_exposure, realized_pnl, 
        resting_orders_count, fees_paid, kalshi_last_updated
    ) VALUES (
        p_strategy_id,
        p_ticker,
        (p_kalshi_data->>'event_ticker'),
        (p_kalshi_data->>'total_traded')::INTEGER,
        (p_kalshi_data->>'position')::INTEGER,
        (p_kalshi_data->>'market_exposure')::INTEGER,
        (p_kalshi_data->>'realized_pnl')::INTEGER,
        (p_kalshi_data->>'resting_orders_count')::INTEGER,
        (p_kalshi_data->>'fees_paid')::INTEGER,
        (p_kalshi_data->>'last_updated_ts')::TIMESTAMP
    )
    ON CONFLICT (strategy_id, ticker) 
    DO UPDATE SET
        total_traded = EXCLUDED.total_traded,
        position = EXCLUDED.position,
        market_exposure = EXCLUDED.market_exposure,
        realized_pnl = EXCLUDED.realized_pnl,
        resting_orders_count = EXCLUDED.resting_orders_count,
        fees_paid = EXCLUDED.fees_paid,
        kalshi_last_updated = EXCLUDED.kalshi_last_updated,
        updated_at = CURRENT_TIMESTAMP;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- INITIAL DATA
-- =====================================================

-- Insert sports data
INSERT INTO sports (sport_name, ticker_prefix, assumed_game_length_minutes) VALUES
('baseball', 'KXMLBGAME', 180),
('basketball', 'KXNBAGAME', 150),
('football', 'KXNFLGAME', 210);

-- Insert initial strategies (reference only - config in code)
INSERT INTO strategies (name, display_name) VALUES
('temporal_arbitrage', 'Temporal Arbitrage'),
('fat_tail_statistical', 'Fat Tail Statistical');