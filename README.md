# Binance Data Collector

[![Rust](https://img.shields.io/badge/Rust-1.70+-orange.svg)](https://www.rust-lang.org/)

## Overview

This project is a modular component of a larger trading bot system, designed for collecting and preprocessing historical market data from the Binance exchange to support backtesting of trading strategies. The module retrieves candlestick data, calculates metrics such as Z-scores and stationarity (via the Augmented Dickey-Fuller test), generates synthetic trading pairs, and stores the processed data in a PostgreSQL database. The data is then serialized into JSON and sent via HTTP to another component of the trading bot for further analysis, signal generation, and simulated trades.

This module is exclusively for **historical market simulation** (backtesting), not live trading. It enables strategy testing by replaying past market conditions without financial risk. Data is processed day-by-day (up to 270 days by default) and supports both regular and synthetic pairs (e.g., BTCUSDT/ETHUSDT).

### Key Features
- Fetches historical Kline data from Binance Futures API.
- Computes Z-scores for price series.
- Performs stationarity tests using the Augmented Dickey-Fuller test.
- Generates and processes synthetic trading pairs from base assets.
- Stores data in a PostgreSQL database with migration support.
- Sends processed data to an external analyzer via HTTP.
- Supports configurable timeframes (e.g., H4) and thresholds (e.g., stationarity percent).
- Comprehensive logging with `tracing` for debugging and monitoring.

This module is ideal for developing and validating trading strategies before deploying them in a live environment.

## Prerequisites

- **Rust**: Version 1.70 or higher. Install via [rustup](https://rustup.rs/).
- **PostgreSQL**: Version 13 or higher. Ensure a database is set up and accessible.
- **Binance API Keys**: An API key and secret from Binance (Futures API access required). Read-only access is sufficient for historical data.
- **Environment Variables**: Create a `.env` file in the project root.

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/m18n/binance_collector_test.git
   cd binance_collector_test
   ```

2. Install dependencies:
   ```bash
   cargo build
   ```

3. Set up the database:
   - Create a PostgreSQL database (e.g., `binance_collector`).
   - Update the `.env` file with your database URL.

4. Run migrations (automatically handled on startup, verifiable with `cargo run`).

## Configuration

Create a `.env` file in the project root with the following variables:

```env
DATABASE_URL=postgres://username:password@localhost/binance_collector
API_KEY=your_binance_api_key
SECRET_KEY=your_binance_api_secret
URL=http://localhost:3000/upload  # Endpoint for sending data to the analyzer
```

- `DATABASE_URL`: PostgreSQL connection string.
- `API_KEY` and `SECRET_KEY`: Binance API credentials (read-only access is sufficient).
- `URL`: HTTP endpoint for sending processed data (adjust as needed).

Trading strategies (e.g., H4 candle limits, stationarity thresholds) are stored in the database and can be managed via SQL inserts into the `configuration` table.

## Usage

Run the collector:
```bash
cargo run
```

The program:
- Runs a loop to process historical data day-by-day (simulated via date offsets).
- Fetches candlestick data from Binance, computes metrics (Z-scores, stationarity), and generates synthetic pairs.
- Stores results in the database and sends JSON payloads to the configured URL.
- Writes logs to `./logs/` with timestamps (e.g., `binance_collector_test_YYYY-MM-DD_HH-MM-SS.log`).

### Workflow
1. **Initialization**: Connects to PostgreSQL and Binance API.
2. **Data Fetching**: Retrieves historical candlesticks for regular and requested pairs.
3. **Processing**: Computes Z-scores and stationarity metrics.
4. **Synthetic Pairs**: Generates pairs (e.g., `BTCUSDT/ETHUSDT`) and processes them.
5. **Storage**: Saves data to database tables (`pairs`, `stationarity_pairs`, `pairs_info`).
6. **Export**: Serializes data into JSON and sends it in batches via HTTP.
7. **Simulation**: Advances through historical data up to 270 days for backtesting.

To simulate a specific historical date, modify the `base_date` in the code or database.

## Project Structure

- `main.rs`: Entry point; initializes logging and starts the collector loop.
- `migrations.rs`: Defines database schema migrations (e.g., tables for pairs, logs, configurations).
- `binance_collector.rs`: Core logic for data collection, processing, and HTTP export.
- `mathematics.rs`: Implements Z-score calculations and Dickey-Fuller tests (using PyO3 for statsmodels).
- `exchange/binance.rs`: Binance API wrapper for fetching candlestick data.
- `storage/database.rs`: PostgreSQL interface with async queries and transactions.
- `analysis/asset.rs`: Analyzes regular and synthetic pairs for metrics.
- `logic.rs`: Utility functions for candle conversion and duplicate removal.
- `core/types.rs`: Defines data types (`TradingPair`, `PairData`, `Candle`, etc.).
- `core/config.rs`: Manages trading strategy configurations from the database.
- Unit tests using `mockall` for database and exchange interfaces.

## Testing

Run tests:
```bash
cargo test
```

Tests cover serialization, data trimming, pair addition, and time/date logic using mocked interfaces.

## Limitations and Notes

- **Historical Only**: Designed for backtesting; does not support live data streaming.
- **API Rate Limits**: Adheres to Binance API limits using chunked requests to avoid bans.
- **Dependencies**: Relies on crates like `sqlx`, `binance-async`, `pyo3`, `ndarray`, and `tracing`.
- **Error Handling**: Uses `anyhow` for robust error management; logs errors via `tracing`.
- **Security**: Store API keys securely and avoid committing `.env` files.
- **Scalability**: Handles large numbers of pairs; optimize database connections for your hardware.

## Contributing

Contributions are welcome! Please open an issue or pull request for bugs, features, or improvements.


