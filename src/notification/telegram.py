import os
import asyncio
from telegram import Bot
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def format_daily_report(top8_df, date):
    """
    Format report harian Top 8 saham
    """
    report = f"📊 *DAILY REPORT - {date}*\n"
    report += "━━━━━━━━━━━━━━━━━━━━\n"
    report += f"🏆 *TOP 8 SAHAM HARI INI*\n\n"

    for i, row in top8_df.iterrows():
        rank = i + 1

        # Relative volume display (pakai raw)
        rel_vol_display = f"{row['relative_volume_raw']:.1f}x"

        # RSI status
        if row['rsi'] > 70:
            rsi_status = "Overbought 🔴"
        elif row['rsi'] < 30:
            rsi_status = "Oversold 🟢"
        else:
            rsi_status = "Normal 🟡"

        # MACD status
        if row['macd'] > row['macd_signal']:
            macd_status = "Bullish 📈"
        else:
            macd_status = "Bearish 📉"

        # MA status
        if row['ma20'] > row['ma50'] > row['ma100']:
            ma_status = "MA20 > MA50 > MA100 ✅"
        elif row['ma20'] > row['ma50']:
            ma_status = "MA20 > MA50 ⚠️"
        else: