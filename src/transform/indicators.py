import pandas as pd
import numpy as np


# MA indicator
def calculate_ma(df):
    """
    Hitung Moving Average 20, 50, 100
    """
    df["ma20"] = df.groupby("ticker")["close"].transform(
        lambda x: x.rolling(window=20).mean()
    )
    df["ma50"] = df.groupby("ticker")["close"].transform(
        lambda x: x.rolling(window=50).mean()
    )
    df["ma100"] = df.groupby("ticker")["close"].transform(
        lambda x: x.rolling(window=100).mean()
    )
    return df


# RSI indicator
def calculate_rsi(df, period=14):
    """
    Hitung RSI (Relative Strength Index)
    """

    def rsi(series):
        delta = series.diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    df["rsi"] = df.groupby("ticker")["close"].transform(rsi)
    return df


# MACD indicator
def calculate_macd(df, fast=12, slow=26, signal=9):
    """
    Hitung MACD dan MACD Signal
    """

    def macd(series):
        ema_fast = series.ewm(span=fast, adjust=False).mean()
        ema_slow = series.ewm(span=slow, adjust=False).mean()
        return ema_fast - ema_slow

    def macd_signal(series):
        macd_line = macd(series)
        return macd_line.ewm(span=signal, adjust=False).mean()

    df["macd"] = df.groupby("ticker")["close"].transform(macd)
    df["macd_signal"] = df.groupby("ticker")["close"].transform(macd_signal)
    return df


# Relative Volume
def calculate_relative_volume(df):
    """
    Hitung Relative Volume = volume hari ini / avg volume 20 hari
    relative_volume_raw  → nilai asli (buat tampilan Telegram)
    relative_volume      → normalized 0-1 dengan cap 3x (buat scoring)
    """

    def rel_vol_raw(series):
        avg_vol = series.rolling(window=20).mean()
        return series / avg_vol

    def rel_vol_normalized(series):
        avg_vol = series.rolling(window=20).mean()
        rv = series / avg_vol
        rv = rv.clip(upper=3.0)
        rv = (rv - 1.0) / (3.0 - 1.0)
        rv = rv.clip(lower=0.0, upper=1.0)
        return rv

    df["relative_volume_raw"] = df.groupby("ticker")["volume"].transform(rel_vol_raw)
    df["relative_volume"] = df.groupby("ticker")["volume"].transform(rel_vol_normalized)
    return df


def calculate_all(df):
    """
    Hitung semua indikator sekaligus
    Return dataframe dengan kolom indikator lengkap
    """
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    print("⚙️  Menghitung MA...")
    df = calculate_ma(df)

    print("⚙️  Menghitung RSI...")
    df = calculate_rsi(df)

    print("⚙️  Menghitung MACD...")
    df = calculate_macd(df)

    print("⚙️  Menghitung Relative Volume...")
    df = calculate_relative_volume(df)

    print("✅ Semua indikator selesai dihitung")
    return df
