# StockTradebyZ Development Guidelines

## Project Overview

**StockTradebyZ** is a comprehensive stock trading analysis system implementing "Z哥战法" (Z's Trading Strategies) with Python. The system provides automated stock selection, backtesting, data fetching, and analysis capabilities with plans for web interface integration.

### Core Purpose
- Automated stock selection using technical analysis strategies
- Historical data management and processing
- Backtesting and performance analysis
- Future web-based dashboard and LLM-powered analysis

## Project Structure Standards

### Directory Organization

```
StockTradebyZ/
├── .augment/                    # Development guidelines and rules
│   └── rules/
├── api/                         # Future FastAPI backend (planned)
├── backtest_result/             # Backtest output files (gitignored)
├── backtest_result_week/        # Weekly backtest results (gitignored)
├── data/                        # Stock CSV data files (gitignored)
├── data-backup/                 # Backup stock data (gitignored)
├── data-bb/                     # Alternative data source (gitignored)
├── database/                    # Database files and migrations
├── debug/                       # Debug tools and test data (gitignored)
├── docs/                        # Documentation files
├── logs/                        # All log files (gitignored)
├── selection_result/            # Stock selection results (gitignored)
├── Tasks/                       # Project planning and task management
│   ├── Done/                    # Completed tasks documentation
│   └── Todo/                    # Pending tasks and planning
└── test_scripts/                # Testing and validation scripts
```

### File Naming Conventions

#### Python Files
- **Core modules**: `PascalCase.py` (e.g., `Selector.py`)
- **Scripts**: `snake_case.py` (e.g., `fetch_kline.py`, `select_stock.py`)
- **Test files**: `test_*.py` (e.g., `test_backtest.py`)
- **Debug tools**: `debug_*.py` (e.g., `debug_bbi_selector.py`)

#### Data Files
- **Stock data**: `{stock_code}.csv` (e.g., `000001.csv`)
- **Market cap data**: `mktcap_{YYYYMMDD}.csv`
- **Selection results**: `selection_result_{YYYYMMDD}.csv`
- **Configuration**: `configs.json`, `appendix.json`

#### Log Files
- **All logs**: Stored in `/logs/` directory
- **Module logs**: `{module_name}.log` (e.g., `fetch.log`, `backtest.log`)
- **Timestamped logs**: `{operation}_log_{YYYYMMDD_HHMMSS}.txt`

#### Batch Files
- **Execution scripts**: `{action}_{target}.bat` (e.g., `run_stock_selection.bat`)
- **Setup scripts**: `setup_{tool}.bat` (e.g., `setup_mootdx.bat`)

## Code Organization Principles

### Module Responsibilities

#### Core Modules
- **`Selector.py`**: Strategy implementations (BBIKDJSelector, PeakKDJSelector, etc.)
- **`fetch_kline.py`**: Data acquisition from multiple sources (AkShare, Tushare, Mootdx)
- **`select_stock.py`**: Stock selection execution and result processing
- **`backtest.py`**: Strategy backtesting and performance analysis
- **`merge_data.py`**: Data merging and consolidation utilities

#### Support Modules
- **`backtest_report.py`**: Report generation for backtest results
- **`get_stockname.py`**: Stock name resolution and caching
- **Clean scripts**: Data quality control and cleanup utilities

### Class and Function Design

#### Strategy Classes
```python
class StrategySelector:
    """
    Strategy selector following the established pattern.

    Attributes:
        param_name (type): Parameter description
        max_window (int): Maximum lookback window for calculations
    """

    def __init__(self, **params):
        """Initialize with configurable parameters."""
        pass

    def _passes_filters(self, hist: pd.DataFrame) -> bool:
        """Single stock filtering logic."""
        pass

    def select(self, date: pd.Timestamp, data: Dict[str, pd.DataFrame]) -> List[str]:
        """Batch stock selection for given date."""
        pass
```

#### Utility Functions
```python
def compute_technical_indicator(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute technical indicator following established patterns.

    Args:
        df: OHLCV DataFrame with required columns

    Returns:
        DataFrame with added indicator columns
    """
    pass
```

## Configuration Management

### JSON Configuration Structure
```json
{
  "selectors": [
    {
      "class": "StrategyClassName",
      "alias": "策略中文名称",
      "activate": true,
      "params": {
        "parameter_name": value
      }
    }
  ]
}
```

### Configuration Principles
- **Centralized**: All strategy parameters in `configs.json`
- **Hierarchical**: Support for nested configuration structures
- **Validation**: Parameter validation in strategy constructors
- **Flexibility**: Easy activation/deactivation of strategies

## Logging and Error Handling Standards

### Logging Configuration
```python
# Standard logging setup pattern
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_dir / "module_name.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("module_name")
```

### Error Handling Patterns
```python
# Graceful error handling with logging
try:
    result = risky_operation()
    logger.info("Operation completed successfully")
    return result
except SpecificException as e:
    logger.error("Specific error occurred: %s", e)
    return default_value
except Exception as e:
    logger.error("Unexpected error in operation: %s", e)
    raise
```

### Log Message Standards
- **INFO**: Normal operation progress, results summary
- **WARNING**: Non-critical issues, data quality concerns
- **ERROR**: Operation failures, invalid configurations
- **DEBUG**: Detailed diagnostic information (use sparingly)

## Data Processing Standards

### DataFrame Conventions
```python
# Standard OHLCV column names
required_columns = ["date", "open", "high", "low", "close", "volume"]

# Date handling
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date").reset_index(drop=True)

# Data validation
if df.empty or len(df) < min_required_rows:
    logger.warning("Insufficient data for %s", stock_code)
    return None
```

### File I/O Patterns
```python
# CSV reading with error handling
try:
    df = pd.read_csv(file_path, parse_dates=["date"])
    logger.debug("Loaded %d rows from %s", len(df), file_path)
except FileNotFoundError:
    logger.warning("File not found: %s", file_path)
    return None
except Exception as e:
    logger.error("Error reading %s: %s", file_path, e)
    return None

# CSV writing with encoding
df.to_csv(output_path, index=False, encoding="utf-8-sig")
```

## Testing Requirements

### Test Organization
- **Unit tests**: `test_scripts/test_{module}.py`
- **Integration tests**: `test_scripts/test_{feature}_integration.py`
- **Debug tools**: `debug/debug_{specific_issue}.py`

### Test Patterns
```python
def test_strategy_selection():
    """Test strategy selection with known data."""
    # Arrange
    test_data = load_test_data()
    strategy = StrategySelector(**test_params)

    # Act
    results = strategy.select(test_date, test_data)

    # Assert
    assert len(results) > 0
    assert all(code in test_data for code in results)
```

### Validation Scripts
- **Data quality**: Automated checks for data completeness and accuracy
- **Strategy validation**: Backtesting with known historical periods
- **Performance testing**: Execution time and memory usage monitoring

## Documentation Standards

### Code Documentation
```python
def complex_calculation(data: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    """
    Perform complex technical analysis calculation.

    Args:
        data: OHLCV DataFrame with required columns
        window: Lookback window for calculation (default: 20)

    Returns:
        DataFrame with additional calculated columns

    Raises:
        ValueError: If data is insufficient or invalid

    Example:
        >>> df = load_stock_data("000001")
        >>> result = complex_calculation(df, window=30)
    """
    pass
```

### README Structure
- **Purpose**: Clear project description
- **Quick Start**: Installation and basic usage
- **Configuration**: Parameter explanations
- **Examples**: Common use cases
- **Troubleshooting**: Common issues and solutions

### Inline Comments
```python
# Business logic explanation
if j_today < self.j_threshold or j_today <= j_quantile:
    # J值满足条件：低于阈值或处于历史低分位
    pass

# Technical implementation notes
hist = hist.tail(self.max_window + 20)  # 额外缓冲避免边界问题
```

## Package Management

### Dependency Management
- **Primary**: Use `uv` for package management (user preference)
- **Configuration**: `pyproject.toml` for project metadata and dependencies
- **Lock file**: `uv.lock` for reproducible builds
- **Version pinning**: Pin critical dependencies for stability

### Dependency Categories
```toml
[project]
dependencies = [
    "pandas>=2.0.0,<3.0.0",    # Core data processing
    "numpy>=1.21.0,<2.0.0",    # Numerical computations
    "akshare==1.17.7",         # Data source (pinned)
    "mootdx==0.11.7",          # Data source (pinned)
    "tushare>=1.4.0",          # Data source
    "scipy>=1.15.3",           # Scientific computing
    "tqdm>=4.60.0",            # Progress bars
    "optuna>=4.0.0",           # Optimization (future use)
]
```

## Performance and Quality Standards

### Code Quality
- **Readability**: Clear variable names, logical structure
- **Modularity**: Single responsibility principle
- **Reusability**: Common patterns extracted to utilities
- **Maintainability**: Consistent patterns across modules

### Performance Guidelines
- **Data processing**: Vectorized operations with pandas/numpy
- **Memory management**: Process data in chunks for large datasets
- **Concurrency**: Use ThreadPoolExecutor for I/O-bound operations
- **Caching**: Cache expensive computations and API calls

### Quality Assurance
- **Data validation**: Check data quality before processing
- **Error recovery**: Graceful handling of missing or corrupt data
- **Logging**: Comprehensive logging for debugging and monitoring
- **Testing**: Regular validation of core functionality

## Future Architecture Considerations

### Web Interface Integration
- **Backend**: FastAPI with SQLAlchemy ORM
- **Database**: SQLite for development, PostgreSQL for production
- **Frontend**: React/Vue.js with TypeScript
- **API Design**: RESTful endpoints following OpenAPI standards

### Scalability Preparations
- **Database migration**: Plan for CSV to database transition
- **API versioning**: Design for backward compatibility
- **Configuration**: Environment-based configuration management
- **Deployment**: Docker containerization for consistent deployment

This document serves as the foundation for consistent development practices across the StockTradebyZ project. All contributors should follow these guidelines to maintain code quality and project coherence.