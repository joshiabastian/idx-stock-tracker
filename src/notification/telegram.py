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

    for rank, (i, row) in enumerate(top8_df.iterrows(), start=1):

        # Relative volume display (pakai raw)
        rel_vol_display = f"{row['relative_volume_raw']:.1f}x"

        # RSI status
        if row["rsi"] > 70:
            rsi_status = "Overbought 🔴"
        elif row["rsi"] < 30:
            rsi_status = "Oversold 🟢"
        else:
            rsi_status = "Normal 🟡"

        # MACD status
        if row["macd"] > row["macd_signal"]:
            macd_status = "Bullish 📈"
        else:
            macd_status = "Bearish 📉"

        # MA status
        if row["ma20"] > row["ma50"] > row["ma100"]:
            ma_status = "MA20 > MA50 > MA100 ✅"
        elif row["ma20"] > row["ma50"]:
            ma_status = "MA20 > MA50 ⚠️"
        else:
            ma_status = "Tidak dalam tren naik ❌"

        # Golden cross
        golden_cross = ""
        if row["ma20"] > row["ma50"]:
            golden_cross = "⭐ Golden Cross terdeteksi\n"

        report += f"*{rank}. {row['ticker']}* | Score: {row['score']:.2f}\n"
        report += f"   📊 Volume: {rel_vol_display} avg 20 hari\n"
        report += f"   {macd_status} MACD\n"
        report += f"   💡 RSI: {rsi_status} ({row['rsi']:.1f})\n"
        report += f"   📈 MA: {ma_status}\n"
        if golden_cross:
            report += f"   {golden_cross}"
        report += "\n"

    report += "━━━━━━━━━━━━━━━━━━━━\n"
    report += "🤖 _IDX Stock Tracker V1_"

    return report


def format_weekly_recap(weekly_df, week_start, week_end):
    """
    Format weekly recap — saham yang paling sering masuk Top 8
    Senin-Jumat direkap Sabtu
    """
    # Hitung frekuensi masuk top 8
    freq = weekly_df["ticker"].value_counts().reset_index()
    freq.columns = ["ticker", "count"]
    freq = freq.sort_values("count", ascending=False)

    report = f"📅 *WEEKLY RECAP*\n"
    report += f"_{week_start} s.d {week_end}_\n"
    report += "━━━━━━━━━━━━━━━━━━━━\n"
    report += "🔥 *Saham Momentum Minggu Ini*\n\n"

    for i, row in freq.iterrows():
        days = row["count"]
        bar = "🟩" * days + "⬜" * (5 - days)
        report += f"*{row['ticker']}* — {days}/5 hari\n"
        report += f"   {bar}\n\n"

    report += "━━━━━━━━━━━━━━━━━━━━\n"
    report += "🤖 _IDX Stock Tracker V1_"

    return report


async def send_message(text):
    """
    Kirim pesan ke Telegram
    """
    bot = Bot(token=TELEGRAM_TOKEN)
    await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode="Markdown")
    print("✅ Report terkirim ke Telegram")


def send_report(text):
    """
    Wrapper untuk jalanin async send_message
    """
    asyncio.run(send_message(text))


def send_alert(message):
    """
    Kirim alert error ke Telegram
    """
    alert = f"⚠️ *ALERT IDX Stock Tracker*\n\n{message}"
    asyncio.run(send_message(alert))
