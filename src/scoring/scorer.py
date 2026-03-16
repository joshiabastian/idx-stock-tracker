import pandas as pd
import numpy as np


def calculate_score(df):
    """
    Hitung score per saham per hari

    Scoring:
    - MA20 > MA50 > MA100  : +1
    - RSI 30-70            : +1
    - MACD bullish         : +1
    - Relative Volume      : +0.0 - 1.0 (normalized)

    Max score: 4.0
    """
    df = df.copy()

    # MA Stack: MA20 > MA50 > MA100
    df["score_ma"] = np.where(
        (df["ma20"] > df["ma50"]) & (df["ma50"] > df["ma100"]), 1, 0
    )

    # RSI Normal: 30 - 70
    df["score_rsi"] = np.where((df["rsi"] >= 30) & (df["rsi"] <= 70), 1, 0)

    # MACD Bullish: macd > macd_signal
    df["score_macd"] = np.where(df["macd"] > df["macd_signal"], 1, 0)

    # Relative Volume: sudah normalized 0-1
    df["score_relvol"] = df["relative_volume"]

    # Handle NaN → 0 (scoring parsial)
    df["score_ma"] = df["score_ma"].fillna(0)
    df["score_rsi"] = df["score_rsi"].fillna(0)
    df["score_macd"] = df["score_macd"].fillna(0)
    df["score_relvol"] = df["score_relvol"].fillna(0)

    # Total score
    df["score"] = (
        df["score_ma"] + df["score_rsi"] + df["score_macd"] + df["score_relvol"]
    )

    return df


def get_top8(df, date=None):
    """
    Ambil Top 8 saham berdasarkan score
    Tiebreaker: score DESC → relative_volume DESC → rsi DESC → ticker ASC
    """
    if date:
        df = df[df["date"] == date].copy()

    # Drop rows dimana semua indikator NaN
    df = df.dropna(subset=["ma20", "rsi", "macd"], how="all")

    # Sort
    df = df.sort_values(
        by=["score", "relative_volume", "rsi", "ticker"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)

    top8 = df.head(8)
    return top8


def get_notes(row):
    """
    Generate catatan otomatis per saham
    Kalau ada NaN indikator, kasih catatan
    """
    notes = []

    if pd.isna(row["ma20"]) or pd.isna(row["ma50"]) or pd.isna(row["ma100"]):
        notes.append("⚠️ MA belum valid (data kurang dari 100 hari)")

    if pd.isna(row["rsi"]):
        notes.append("⚠️ RSI belum valid")

    if pd.isna(row["macd"]):
        notes.append("⚠️ MACD belum valid")

    # Golden cross detection
    if (row["ma20"] > row["ma50"]) and not pd.isna(row["ma20"]):
        notes.append("⭐ Golden Cross MA20 > MA50")

    # RSI status
    if not pd.isna(row["rsi"]):
        if row["rsi"] > 70:
            notes.append("🔴 RSI Overbought")
        elif row["rsi"] < 30:
            notes.append("🟢 RSI Oversold")
        else:
            notes.append("🟡 RSI Normal")

    # MACD status
    if not pd.isna(row["macd"]):
        if row["macd"] > row["macd_signal"]:
            notes.append("📈 MACD Bullish")
        else:
            notes.append("📉 MACD Bearish")

    return notes
