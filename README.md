# IDX Stock Tracker

Bot harian yang tracking Top 8 saham Kompas100
berdasarkan analisis teknikal dan dikirim via Telegram.

## Architecture

![Architecture](docs/idx-stock-tracker-architecture.png)

## Tech Stack

- Python
- Apache Airflow
- PostgreSQL
- yfinance
- Telegram Bot

## How It Works

1. yfinance fetch data harian IDX
2. Raw data disimpan ke PostgreSQL
3. Kalkulasi indikator teknikal (MA, RSI, MACD, Relative Volume)
4. Scoring & ranking Top 8 saham
5. Report dikirim via Telegram Bot
