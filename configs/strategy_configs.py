# configs/strategy_configs.py - Strategy-Specific Parameters
"""
Optimized strategy parameter configuration
Focuses purely on trading strategy parameters, not system architecture
"""

from typing import Dict, Any, List, Tuple

# ======================
# TEMPORAL ARBITRAGE STRATEGY
# Proven 61x improvement over baseline
# ======================

TEMPORAL_CONFIG = {
    # Core momentum detection (THE GAME CHANGER - 38x improvement)
    'momentum_threshold': 0.035,           # 3.5¢ threshold
    'min_price_threshold': 0.25,           # 25¢ minimum entry price
    'momentum_no_upper_limit_above': 0.75, # No upper limit above 75¢
    
    # Profit/loss management
    'profit_target_spread': 0.03,          # 3¢ profit target
    'stop_loss_spread': 0.03,              # 3¢ stop loss (critical - tighter kills performance)
    
    # Position management
    'max_incomplete_pairs_per_game': 2,    # Maximum 2 incomplete pairs per game
    'position_size_dollars': 10,           # $10 per contract
    'max_incomplete_time_seconds': 1800,   # 30 minutes force completion
    'max_total_pair_cost': 1.05,           # Skip invalid data points > $1.05
    
    # Advanced momentum detection
    'max_momentum_jump_per_minute': 0.20,  # Skip >20¢ jumps in 1 minute
    'enable_game_start_monitoring': True,  # Game proximity monitoring
    'game_start_proximity_minutes': 150,   # Minutes into game before close conditions
    'close_probability_min': 0.35,         # 35% lower bound for close range
    'close_probability_max': 0.65          # 65% upper bound for close range
}

# Derived parameters
TEMPORAL_CONFIG['profit_target'] = 1.0 - TEMPORAL_CONFIG['profit_target_spread']  # 0.97
TEMPORAL_CONFIG['stop_loss'] = 1.0 + TEMPORAL_CONFIG['stop_loss_spread']          # 1.03

# ======================
# FAT TAIL STATISTICAL STRATEGY  
# Validated: 9.01% ROI improvement, 2.3pp systematic edge
# Strategy: BACKING 32-46¢ underdogs in minutes -120 to +4
# ======================

FAT_TAIL_CONFIG = {
    # Core strategy parameters (UPDATED: -120 minutes as requested)
    'strategy_type': 'BACKING',            # BACKING underdogs (not FADING favorites)
    'price_range_min': 0.32,               # 32¢ minimum underdog price
    'price_range_max': 0.46,               # 46¢ maximum underdog price  
    'trading_window_start': -120,          # UPDATED: -120 minutes (was -20)
    'trading_window_end': 4,               # +4 minutes into game
    
    # Validated performance metrics
    'systematic_edge': 0.023,              # 2.3 percentage point edge
    'validated_roi': 0.0901,               # 9.01% ROI improvement  
    'sample_size': 1424,                   # Validation sample size
    'win_rate': 0.461,                     # 46.1% vs 43.8% implied
    
    # Position sizing & risk
    'position_size_pct': 0.0625,           # 6.25% of allocation per position
    'max_daily_positions': 8,              # Maximum 8 positions per day
    'stop_loss_level': 0.23,               # 23¢ absolute stop loss
    'max_daily_loss_dollars': 50,          # $50 daily loss limit
    'min_expected_edge': 0.02,             # 2% minimum edge threshold
    
    # Execution parameters
    'order_timeout_seconds': 30,           # 30 second order timeout
    'position_check_interval': 1,          # 1 second monitoring interval
    'exit_order_type': 'market',           # Market orders for exits
    'entry_price_type': 'exact_ask',       # Use exact ask price for entries
    'execution_stagger_seconds': 2,        # 2 second stagger between orders
    
    # Logging & monitoring
    'enable_detailed_logging': True,       # Comprehensive logging
    'log_scanner_details': True,           # Log scanner decisions
    'log_position_updates': True           # Log position updates
}

# ======================
# VALIDATION FRAMEWORK
# ======================

def validate_strategy_config(strategy_name: str, config: Dict[str, Any], validations: List[Tuple]) -> Tuple[List[str], List[str]]:
    """Generic validation function for any strategy configuration"""
    errors = []
    warnings = []
    
    for field, validation_type, *params in validations:
        value = config.get(field)
        
        if validation_type == 'required' and value is None:
            errors.append(f"{strategy_name}: {field} is required")
        elif validation_type == 'positive' and (value is None or value <= 0):
            errors.append(f"{strategy_name}: {field} must be positive")
        elif validation_type == 'range' and value is not None:
            min_val, max_val = params
            if not (min_val <= value <= max_val):
                errors.append(f"{strategy_name}: {field} must be between {min_val} and {max_val}")
        elif validation_type == 'greater_than' and value is not None:
            threshold = params[0]
            if value <= threshold:
                errors.append(f"{strategy_name}: {field} must be greater than {threshold}")
        elif validation_type == 'optimal_check' and value != params[0]:
            optimal_val = params[0]
            warnings.append(f"{strategy_name}: {field} not at proven optimal ({value} vs {optimal_val})")
        elif validation_type == 'consistency_check':
            other_field, comparison = params
            other_value = config.get(other_field)
            if value is not None and other_value is not None:
                if comparison == 'less_than' and value >= other_value:
                    errors.append(f"{strategy_name}: {field} must be less than {other_field}")
    
    return errors, warnings

def validate_temporal_config() -> Tuple[List[str], List[str]]:
    """Validate temporal arbitrage configuration"""
    validations = [
        # Core parameters
        ('momentum_threshold', 'optimal_check', 0.035),  # Proven optimal
        ('min_price_threshold', 'range', 0.1, 0.5),
        ('momentum_no_upper_limit_above', 'range', 0.5, 1.0),
        
        # Profit/loss management  
        ('profit_target_spread', 'positive'),
        ('stop_loss_spread', 'positive'),
        
        # Position management
        ('max_incomplete_pairs_per_game', 'range', 1, 5),
        ('position_size_dollars', 'positive'),
        ('max_incomplete_time_seconds', 'range', 300, 3600),
        ('max_total_pair_cost', 'range', 1.0, 1.2),
        
        # Consistency checks
        ('min_price_threshold', 'consistency_check', 'momentum_no_upper_limit_above', 'less_than')
    ]
    
    return validate_strategy_config('Temporal', TEMPORAL_CONFIG, validations)

def validate_fat_tail_config() -> Tuple[List[str], List[str]]:
    """Validate fat tail strategy configuration"""
    validations = [
        # Core strategy
        ('strategy_type', 'required'),
        ('price_range_min', 'range', 0.1, 0.8),
        ('price_range_max', 'range', 0.1, 0.8), 
        ('trading_window_start', 'range', -300, 0),  # -5 hours to game start
        ('trading_window_end', 'range', 0, 60),      # Game start to +1 hour
        
        # Position sizing
        ('position_size_pct', 'range', 0.01, 0.2),   # 1% to 20%
        ('max_daily_positions', 'range', 1, 20),
        ('stop_loss_level', 'range', 0.1, 0.5),
        ('max_daily_loss_dollars', 'positive'),
        
        # Execution
        ('order_timeout_seconds', 'range', 10, 300),
        ('position_check_interval', 'range', 0.5, 10),
        
        # Consistency checks
        ('price_range_min', 'consistency_check', 'price_range_max', 'less_than'),
        ('trading_window_start', 'consistency_check', 'trading_window_end', 'less_than')
    ]
    
    return validate_strategy_config('Fat Tail', FAT_TAIL_CONFIG, validations)

def validate_all_strategies() -> Tuple[List[str], List[str]]:
    """Validate all strategy configurations"""
    all_errors = []
    all_warnings = []
    
    # Validate each strategy
    temporal_errors, temporal_warnings = validate_temporal_config()
    fat_tail_errors, fat_tail_warnings = validate_fat_tail_config()
    
    all_errors.extend(temporal_errors + fat_tail_errors)
    all_warnings.extend(temporal_warnings + fat_tail_warnings)
    
    return all_errors, all_warnings

# ======================
# CONFIGURATION ACCESS
# ======================

def get_temporal_config() -> Dict[str, Any]:
    """Get temporal arbitrage configuration"""
    return TEMPORAL_CONFIG.copy()

def get_fat_tail_config() -> Dict[str, Any]:
    """Get fat tail strategy configuration"""
    return FAT_TAIL_CONFIG.copy()

def get_strategy_config(strategy_name: str) -> Dict[str, Any]:
    """Get configuration for any strategy by name"""
    configs = {
        'temporal_arbitrage': TEMPORAL_CONFIG,
        'fat_tail_statistical': FAT_TAIL_CONFIG,
        'temporal': TEMPORAL_CONFIG,  # Aliases
        'fat_tail': FAT_TAIL_CONFIG
    }
    
    config = configs.get(strategy_name.lower())
    if config is None:
        raise ValueError(f"Unknown strategy: {strategy_name}")
    
    return config.copy()

# ======================
# CONSOLE TESTING
# ======================

def print_strategy_config(strategy_name: str, config: Dict[str, Any]):
    """Print strategy configuration summary"""
    print(f"\n=== {strategy_name.upper()} STRATEGY ===")
    
    # Group related parameters
    core_params = []
    risk_params = []
    execution_params = []
    other_params = []
    
    for key, value in config.items():
        if any(term in key for term in ['threshold', 'range', 'window', 'strategy_type']):
            core_params.append((key, value))
        elif any(term in key for term in ['stop_loss', 'daily_loss', 'position_size', 'max_daily']):
            risk_params.append((key, value))
        elif any(term in key for term in ['timeout', 'interval', 'order_type', 'execution']):
            execution_params.append((key, value))
        else:
            other_params.append((key, value))
    
    # Print grouped parameters
    if core_params:
        print("Core Parameters:")
        for key, value in core_params:
            print(f"  {key}: {value}")
    
    if risk_params:
        print("Risk Management:")
        for key, value in risk_params:
            print(f"  {key}: {value}")
    
    if execution_params:
        print("Execution:")
        for key, value in execution_params:
            print(f"  {key}: {value}")

def print_all_strategies():
    """Print all strategy configurations"""
    print("=" * 70)
    print("STRATEGY CONFIGURATIONS")
    print("=" * 70)
    
    print_strategy_config("Temporal Arbitrage", TEMPORAL_CONFIG)
    print_strategy_config("Fat Tail Statistical", FAT_TAIL_CONFIG)
    
    print(f"\n=== KEY HIGHLIGHTS ===")
    print(f"Temporal: {TEMPORAL_CONFIG['momentum_threshold']*100:.1f}¢ momentum threshold (61x improvement)")
    print(f"Fat Tail: BACKING {FAT_TAIL_CONFIG['price_range_min']*100:.0f}-{FAT_TAIL_CONFIG['price_range_max']*100:.0f}¢ underdogs, {FAT_TAIL_CONFIG['trading_window_start']} to +{FAT_TAIL_CONFIG['trading_window_end']} min")
    print(f"Fat Tail: {FAT_TAIL_CONFIG['systematic_edge']*100:.1f}% systematic edge, {FAT_TAIL_CONFIG['validated_roi']*100:.1f}% ROI improvement")
    
    print("=" * 70)

def run_strategy_validation():
    """Run complete strategy validation - primary console test function"""
    print("\n" + "=" * 50)
    print("STRATEGY CONFIGURATION VALIDATION")
    print("=" * 50)
    
    errors, warnings = validate_all_strategies()
    
    if not errors and not warnings:
        print("✓ All strategy validations passed!")
        return True
    
    if errors:
        print("ERRORS:")
        for i, error in enumerate(errors, 1):
            print(f"  {i}. {error}")
    
    if warnings:
        print("WARNINGS:")
        for i, warning in enumerate(warnings, 1):
            print(f"  {i}. {warning}")
    
    print("=" * 50)
    return len(errors) == 0

# ======================
# CONSOLE TESTING
# ======================

if __name__ == "__main__":
    print("strategy_configs.py loaded successfully!")
    print("\nQuick test commands:")
    print("  run_strategy_validation()     - Validate all strategy parameters")
    print("  print_all_strategies()        - Show all strategy configurations")
    print("  get_temporal_config()         - Get temporal arbitrage settings")
    print("  get_fat_tail_config()         - Get fat tail strategy settings")
    print(f"\nKey Update: Fat tail window now {FAT_TAIL_CONFIG['trading_window_start']} to +{FAT_TAIL_CONFIG['trading_window_end']} minutes")
    print("\nRun: run_strategy_validation()")

# ======================
# ARCHITECTURE NOTES FOR FUTURE IMPLEMENTATION
# ======================

"""
REMOVED FROM THIS FILE (belongs elsewhere in new architecture):

1. PORTFOLIO ALLOCATION - Should be in main.py or strategy_coordinator.py:
   - Strategy allocation percentages (30% temporal, 70% fat tail)
   - Daily balance refresh logic
   - Account balance caching

2. CROSS-STRATEGY COORDINATION - Should be in strategy_coordinator.py:
   - Multi-strategy position sizing coordination
   - Shared risk limits across strategies
   - Strategy conflict resolution

3. SYSTEM INTEGRATION - Should be in core infrastructure:
   - Database integration for strategy tracking
   - Shared logging coordination
   - System-wide performance tracking

4. PORTFOLIO MANAGEMENT - Should be in risk_management/portfolio_manager.py:
   - Overall portfolio exposure limits
   - Cross-strategy risk correlation
   - Account balance allocation logic

Current file focuses purely on strategy-specific trading parameters.
"""