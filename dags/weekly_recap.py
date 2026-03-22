from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

from src.database.queries import fetch_gold_data
from src.notification.telegram import format_weekly_recap, send_report, send_alert


def generate_weekly_recap(**context):
    today = datetime.today().date()

    week_start = today - timedelta(days=today.weekday() + 1)
    week_end = week_start + timedelta(days=4)

    print(f"📅 Weekly recap: {week_start} s.d {week_end}")

    df = fetch_gold_data(start_date=week_start)

    if df is None or df.empty:
        send_alert(
            f"❌ Weekly recap gagal — gold_data kosong untuk minggu {week_start} s.d {week_end}"
        )
        raise ValueError("gold_data kosong")

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df[df["date"] <= week_end]

    top8_per_day = []
    for date in df["date"].unique():
        df_day = df[df["date"] == date].copy()
        df_day = df_day.sort_values(
            by=["score", "relative_volume", "rsi", "ticker"],
            ascending=[False, False, False, True],
        ).head(8)
        top8_per_day.append(df_day)

    weekly_df = pd.concat(top8_per_day, ignore_index=True)

    if weekly_df.empty:
        send_alert("❌ Weekly recap gagal — tidak ada data Top 8 minggu ini")
        raise ValueError("Top 8 weekly kosong")

    report = format_weekly_recap(
        weekly_df=weekly_df, week_start=week_start, week_end=week_end
    )

    send_report(report)
    print("✅ Weekly recap terkirim!")


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": lambda context: send_alert(
        f"❌ Weekly recap gagal: {context['task_instance'].task_id}\n"
        f"Tanggal: {context['execution_date']}"
    ),
}

with DAG(
    dag_id="weekly_stock_recap",
    default_args=default_args,
    description="IDX Stock Tracker — Weekly Recap",
    schedule_interval="0 3 * * 6",
    start_date=datetime(2026, 3, 21),
    catchup=False,
    tags=["stock", "weekly"],
) as dag:

    t1 = PythonOperator(
        task_id="generate_weekly_recap",
        python_callable=generate_weekly_recap,
    )
