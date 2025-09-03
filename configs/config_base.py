# configs/config_base.py - Core System Infrastructure Configuration
"""
Core configuration for Sports Trading System 2.0
Handles: API settings, database connection, system parameters, logging
Efficient design with database-driven sports config
"""

import os
from pathlib import Path
from typing import Dict, Any, Tuple, List
import logging

# ======================
# PROJECT STRUCTURE
# ======================

try:
    BASE_DIR = Path(__file__).parent.parent
except NameError:
    BASE_DIR = Path(os.getcwd())

# Create required directories
for directory in ['logs', 'keys', 'databases']:
    (BASE_DIR / directory).mkdir(exist_ok=True)

LOGS_DIR = BASE_DIR / "logs"
KEYS_DIR = BASE_DIR / "keys"

# ======================
# KALSHI API CONFIGURATION
# ======================

KALSHI_CONFIG = {
    'key_id': "5051b51e-6fa6-4f5a-815b-9ea863c1c55f",
    'private_key_path': KEYS_DIR / "kalshi_private_key.pem",
    'live_host': "https://api.elections.kalshi.com/trade-api/v2",  # ← UPDATED
    'demo_host': "https://demo-api.kalshi.co/trade-api/v2",        # ← May need updating too
    'is_demo': False,
    'rate_limit_per_second': 10,
    'request_timeout': 30,
    'max_retries': 3
}

def get_kalshi_api_host() -> str:
    """Get appropriate API host"""
    return KALSHI_CONFIG['demo_host'] if KALSHI_CONFIG['is_demo'] else KALSHI_CONFIG['live_host']

# ======================
# DATABASE CONFIGURATION
# ======================

DATABASE_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'sports_trading_system_2025',
    'user': 'trader',
    'password': 'SecureTrading2025!',
    'schema': 'public',
    'min_connections': 2,
    'max_connections': 10,
    'connection_timeout': 30
}

def get_database_url(include_password: bool = True) -> str:
    """Generate PostgreSQL connection URL"""
    config = DATABASE_CONFIG
    password = config['password'] if include_password else '***'
    return f"postgresql://{config['user']}:{password}@{config['host']}:{config['port']}/{config['database']}"

# ======================
# SYSTEM CONFIGURATION
# ======================

SYSTEM_CONFIG = {
    # Timing intervals
    'check_interval': 30,
    'position_monitor_interval': 60,
    'health_check_interval': 300,
    'market_data_update_interval': 10,
    
    # Risk management (shared across strategies)
    'daily_loss_limit': 100,
    'max_position_value': 500,
    'max_daily_trades': 50,
    'portfolio_max_exposure': 0.95,
    
    # Operation modes
    'enable_live_trading': True,
    'enable_position_monitoring': True,
    'enable_risk_monitoring': True,
    'verbose_logging': True,
    
    # Market filtering
    'min_market_volume': 50,
    'max_spread_threshold': 0.15,
    'min_time_to_close': 30,
    
    # Data management
    'price_history_length': 50,
    'market_cache_expiry': 300
}

# ======================
# LOGGING CONFIGURATION
# ======================

LOGGING_CONFIG = {
    'LOG_LEVEL': 'INFO',                    # String format for logger compatibility
    'LOG_TO_FILE': True,
    'CONSOLE_LOGGING': True,                # Match logger field name
    'MAX_LOG_SIZE_MB': 50,                  # Match logger field name  
    'LOG_RETENTION_DAYS': 30,               # Match logger field name
    'LOG_DIRECTORY': str(LOGS_DIR)          # Match logger field name (not logs_directory)
}

# Log file paths
LOG_FILES = {
    'main_system': 'main_system.log',
    'temporal_arbitrage': 'temporal_arbitrage.log',
    'fat_tail_statistical': 'fat_tail_statistical.log',
    'api_client': 'api_client.log',
    'database': 'database.log',
    'risk_management': 'risk_management.log'
}

# ======================
# SPORTS CONFIGURATION (Database-Driven)
# ======================

# Only specify which sports are active - properties come from database
ACTIVE_SPORTS = ['baseball']  # Sport names that match database sports.sport_name

# Sports will be loaded from database sports table:
# - sport_name, ticker_prefix, assumed_game_length_minutes
# This config just controls which ones are actively monitored

# ======================
# VALIDATION & TESTING
# ======================

def validate_config_section(section_name: str, config: Dict, validations: List[Tuple]) -> Tuple[List[str], List[str]]:
    """Generic validation function - reduces code duplication"""
    errors = []
    warnings = []
    
    for field, validation_type, *params in validations:
        value = config.get(field)
        
        if validation_type == 'required' and not value:
            errors.append(f"{section_name}: {field} is required")
        elif validation_type == 'positive' and (value is None or value <= 0):
            errors.append(f"{section_name}: {field} must be positive")
        elif validation_type == 'range' and value is not None:
            min_val, max_val = params
            if not (min_val <= value <= max_val):
                errors.append(f"{section_name}: {field} must be between {min_val} and {max_val}")
        elif validation_type == 'file_exists' and not Path(value).exists():
            errors.append(f"{section_name}: {field} file not found: {value}")
        elif validation_type == 'warn_high' and value is not None:
            threshold = params[0]
            if value > threshold:
                warnings.append(f"{section_name}: {field} is high ({value} > {threshold})")
    
    return errors, warnings

def validate_all_config() -> Tuple[List[str], List[str]]:
    """Run all configuration validations"""
    all_errors = []
    all_warnings = []
    
    # API validations
    api_validations = [
        ('key_id', 'required'),
        ('private_key_path', 'file_exists'),
        ('rate_limit_per_second', 'range', 1, 10),
        ('request_timeout', 'positive'),
        ('max_retries', 'range', 1, 10)
    ]
    errors, warnings = validate_config_section('API', KALSHI_CONFIG, api_validations)
    all_errors.extend(errors)
    all_warnings.extend(warnings)
    
    # Database validations  
    db_validations = [
        ('host', 'required'),
        ('database', 'required'),
        ('user', 'required'),
        ('password', 'required'),
        ('port', 'range', 1, 65535),
        ('min_connections', 'positive'),
        ('max_connections', 'positive'),
        ('max_connections', 'warn_high', 20)
    ]
    errors, warnings = validate_config_section('Database', DATABASE_CONFIG, db_validations)
    all_errors.extend(errors)
    all_warnings.extend(warnings)
    
    # Additional database logic
    if DATABASE_CONFIG.get('min_connections', 0) >= DATABASE_CONFIG.get('max_connections', 1):
        all_errors.append("Database: min_connections must be less than max_connections")
    
    # System validations
    system_validations = [
        ('check_interval', 'positive'),
        ('position_monitor_interval', 'positive'),
        ('daily_loss_limit', 'positive'),
        ('max_position_value', 'positive'),
        ('max_daily_trades', 'positive'),
        ('portfolio_max_exposure', 'range', 0.1, 1.0),
        ('check_interval', 'warn_high', 300),  # > 5 minutes seems high
        ('position_monitor_interval', 'warn_high', 300)
    ]
    errors, warnings = validate_config_section('System', SYSTEM_CONFIG, system_validations)
    all_errors.extend(errors)
    all_warnings.extend(warnings)
    
    return all_errors, all_warnings

def test_config_section(section_name: str, config: Dict, show_sensitive: bool = False):
    """Generic test function for any config section"""
    print(f"\n=== {section_name.upper()} CONFIGURATION ===")
    
    for key, value in config.items():
        # Hide sensitive information unless requested
        if not show_sensitive and ('password' in key.lower() or 'key' in key.lower()):
            if isinstance(value, (str, Path)) and len(str(value)) > 10:
                display_value = str(value)[:8] + "***"
            else:
                display_value = "***"
        else:
            display_value = value
        
        # Special handling for file paths
        if isinstance(value, Path):
            exists = value.exists()
            status = "✓" if exists else "✗"
            print(f"{status} {key}: {display_value} ({'exists' if exists else 'missing'})")
        else:
            print(f"  {key}: {display_value}")

def print_full_configuration(show_sensitive: bool = False):
    """Print complete configuration summary"""
    print("=" * 70)
    print("SPORTS TRADING SYSTEM 2.0 - CONFIGURATION")
    print("=" * 70)
    
    # Test each section
    test_config_section("API", KALSHI_CONFIG, show_sensitive)
    test_config_section("Database", DATABASE_CONFIG, show_sensitive)
    test_config_section("System", SYSTEM_CONFIG)
    
    print(f"\n=== SPORTS & MARKETS ===")
    print(f"Active Sports: {', '.join(ACTIVE_SPORTS)} (properties from database)")
    
    print(f"\n=== DIRECTORIES ===")
    print(f"✓ Base: {BASE_DIR}")
    print(f"✓ Logs: {LOGS_DIR}")
    print(f"✓ Keys: {KEYS_DIR}")
    
    print("=" * 70)

def run_full_validation():
    """Complete configuration validation - primary console test function"""
    print("\n" + "=" * 50)
    print("CONFIGURATION VALIDATION")
    print("=" * 50)
    
    errors, warnings = validate_all_config()
    
    if not errors and not warnings:
        print("✓ All configuration validation passed!")
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
# ACCESSOR FUNCTIONS
# ======================

def get_kalshi_config() -> Dict[str, Any]:
    """Get complete Kalshi configuration"""
    config = KALSHI_CONFIG.copy()
    config['api_host'] = get_kalshi_api_host()
    config['private_key_path'] = str(config['private_key_path'])  # Convert Path to string
    return config

def get_database_config() -> Dict[str, Any]:
    """Get complete database configuration"""
    config = DATABASE_CONFIG.copy()
    config['connection_url'] = get_database_url(include_password=True)
    config['safe_connection_url'] = get_database_url(include_password=False)
    return config

def get_system_config() -> Dict[str, Any]:
    """Get complete system configuration"""
    return SYSTEM_CONFIG.copy()

def get_logging_config() -> Dict[str, Any]:
    """Get complete logging configuration"""
    config = LOGGING_CONFIG.copy()
    config['log_files'] = {name: str(LOGS_DIR / filename) for name, filename in LOG_FILES.items()}
    return config

# ======================
# CONSOLE TESTING
# ======================

if __name__ == "__main__":
    print("config_base.py loaded successfully!")
    print(f"Base directory: {BASE_DIR}")
    print("\nQuick test commands:")
    print("  run_full_validation()       - Validate all settings")
    print("  print_full_configuration()  - Show complete config") 
    print("  get_kalshi_config()         - Get API settings")
    print("  get_database_config()       - Get DB settings")
    print("\nRun: run_full_validation()")