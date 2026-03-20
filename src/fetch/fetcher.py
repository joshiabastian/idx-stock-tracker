import os
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from config.tickers import KOMPAS100_TICKERS


def fetch_historical(tickers=KOMPAS100_TICKERS, period_years=1):
    """
    Initial fetch: ambil data historis 1 tahun kebelakang
    Simpan ke CSV sebagai backup
    """
    end_date = datetime.today()
    start_date = end_date - timedelta(days=365 * period_years)

    print(f"📥 Fetching historical data dari {start_date.date()} s.d {end_date.date()}")

    all_data = []

    for ticker in tickers:
        try:
            df = yf.download(
                ticker,
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                auto_adjust=False,
                progress=False,
            )

            if df.empty:
                print(f"⚠️  {ticker}: data kosong, skip")
                continue

            df = df[["Open", "High", "Low", "Close", "Adj Close", "Volume"]]
            df.columns = ["open", "high", "low", "close", "adj_close", "volume"]
            df.index.name = "date"
            df.reset_index(inplace=True)
            df["ticker"] = ticker.replace(".JK", "")
            df["date"] = pd.to_datetime(df["date"]).dt.date

            all_data.append(df)
            print(f"✅ {ticker}: {len(df)} rows")

        except Exception as e:
            print(f"❌ {ticker}: error - {e}")

    if not all_data:
        print("❌ Tidak ada data yang berhasil di-fetch")
        return None

    final_df = pd.concat(all_data, ignore_index=True)
    final_df = final_df[
        ["date", "ticker", "open", "high", "low", "close", "adj_close", "volume"]
    ]
    final_df = final_df.sort_values(["date", "ticker"]).reset_index(drop=True)

    # Simpan ke CSV sebagai backup
    os.makedirs("data/raw", exist_ok=True)
    csv_path = f"data/raw/historical_{datetime.today().strftime('%Y%m%d')}.csv"
    final_df.to_csv(csv_path, index=False)
    print(f"\n💾 CSV tersimpan: {csv_path}")
    print(f"📊 Total rows: {len(final_df)}")

    return final_df


# Ambil saham setiap hari
def fetch_daily(tickers=KOMPAS100_TICKERS):
    """
    Daily fetch: ambil data 1 hari terakhir
    Return None jika hari libur / data kosong
    """
    today = datetime.today().strftime("%Y-%m-%d")
    tomorrow = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"📥 Fetching daily data: {today}")

    all_data = []

    for ticker in tickers:
        try:
            df = yf.download(
                ticker, start=today, end=tomorrow, auto_adjust=False, progress=False
            )

            if df.empty:
                continue

            df = df[["Open", "High", "Low", "Close", "Adj Close", "Volume"]]
            df.columns = ["open", "high", "low", "close", "adj_close", "volume"]
            df.index.name = "date"
            df.reset_index(inplace=True)
            df["ticker"] = ticker.replace(".JK", "")
            df["date"] = pd.to_datetime(df["date"]).dt.date

            all_data.append(df)

        except Exception as e:
            print(f"❌ {ticker}: error - {e}")

    if not all_data:
        print(f"⚠️  Tidak ada data untuk {today} — kemungkinan hari libur")
        return None

    final_df = pd.concat(all_data, ignore_index=True)
    final_df = final_df[
        ["date", "ticker", "open", "high", "low", "close", "adj_close", "volume"]
    ]
    final_df = final_df.sort_values(["date", "ticker"]).reset_index(drop=True)

    print(f"✅ Daily fetch selesai: {len(final_df)} rows")
    return final_df


def fetch_new_ticker(ticker, period_days=200):
    """
    Fetch historical untuk saham baru masuk Kompas100
    Ambil 200 hari kebelakang biar indikator valid
    """
    end_date = datetime.today()
    start_date = end_date - timedelta(days=period_days)

    print(f"📥 Fetching new ticker {ticker} ({period_days} hari)")

    try:
        df = yf.download(
            ticker,
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            auto_adjust=False,
            progress=False,
        )

        if df.empty:
            print(f"⚠️  {ticker}: data kosong")
            return None

        df = df[["Open", "High", "Low", "Close", "Adj Close", "Volume"]]
        df.columns = ["open", "high", "low", "close", "adj_close", "volume"]
        df.index.name = "date"
        df.reset_index(inplace=True)
        df["ticker"] = ticker.replace(".JK", "")
        df["date"] = pd.to_datetime(df["date"]).dt.date

        final_df = df[
            ["date", "ticker", "open", "high", "low", "close", "adj_close", "volume"]
        ]
        final_df = final_df.sort_values("date").reset_index(drop=True)

        print(f"✅ {ticker}: {len(final_df)} rows")
        return final_df

    except Exception as e:
        print(f"❌ {ticker}: error - {e}")
        return None
