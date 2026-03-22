from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import holidays

from src.fetch.fetcher import fetch_daily
from src.transform.indicators import calculate_all
from src.scoring.scorer import calculate_score, get_top8, get_notes
from src.database.queries import (
    create_tables,
    insert_raw_data,
    insert_gold_data,
    get_top8 as db_get_top8,
)
from src.notification.telegram import format_daily_report, send_report, send_alert


def check_market_open(**context):
    today = context["logical_date"].date()
    id_holidays = holidays.Indonesia(years=today.year)

    if today.weekday() >= 5:
        print(f"⚠️ {today} adalah weekend, skip")
        return False

    if today in id_holidays:
        print(f"⚠️ {today} adalah libur nasional, skip")
        return False

    print(f"✅ {today} adalah hari bursa, lanjut")
    return True


def fetch_and_store(**context):
    is_market_open = context["ti"].xcom_pull(task_ids="check_market_open")

    if not is_market_open:
        print("⏭️ Skip fetch — bukan hari bursa")
        return

    df = fetch_daily()

    if df is None:
        send_alert(
            "❌ Fetch data gagal — data kosong padahal bukan hari libur!\nCek manual yah."
        )
        raise ValueError("Fetch data gagal")

    create_tables()
    insert_raw_data(df)
    print(f"✅ Raw data tersimpan: {len(df)} rows")


def transform_and_score(**context):
    is_market_open = context["ti"].xcom_pull(task_ids="check_market_open")

    if not is_market_open:
        print("⏭️ Skip transform — bukan hari bursa")
        return

    from src.database.queries import fetch_raw_data
    import pandas as pd

    df = fetch_raw_data()

    if df is None or df.empty:
        send_alert("❌ Transform gagal — raw_data kosong!")
        raise ValueError("raw_data kosong")

    df = calculate_all(df)
    df = calculate_score(df)

    today = context["logical_date"].date()
    df_today = df[df["date"] == today].copy()

    gold_cols = [
        "date",
        "ticker",
        "ma20",
        "ma50",
        "ma100",
        "rsi",
        "macd",
        "macd_signal",
        "relative_volume",
        "relative_volume_raw",
        "score",
    ]
    df_today = df_today[gold_cols]

    insert_gold_data(df_today)
    print(f"✅ Gold data tersimpan: {len(df_today)} rows")


def send_daily_report(**context):
    is_market_open = context["ti"].xcom_pull(task_ids="check_market_open")

    if not is_market_open:
        print("⏭️ Skip report — bukan hari bursa")
        return

    today = context["logical_date"].date()
    top8 = db_get_top8(date=today)

    if top8 is None or top8.empty:
        send_alert("❌ Top 8 kosong — cek pipeline!")
        raise ValueError("Top 8 kosong")

    report = format_daily_report(top8, date=today)
    send_report(report)
    print("✅ Daily report terkirim!")


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": lambda context: send_alert(
        f"❌ Task gagal: {context['task_instance'].task_id}\n"
        f"Tanggal: {context['execution_date']}"
    ),
}

with DAG(
    dag_id="daily_stock_pipeline",
    default_args=default_args,
    description="IDX Stock Tracker — Daily Pipeline",
    schedule_interval="0 12 * * 1-5",
    start_date=datetime(2026, 3, 16),
    catchup=False,
    tags=["stock", "daily"],
) as dag:

    t1 = PythonOperator(
        task_id="check_market_open",
        python_callable=check_market_open,
    )

    t2 = PythonOperator(
        task_id="fetch_and_store",
        python_callable=fetch_and_store,
    )

    t3 = PythonOperator(
        task_id="transform_and_score",
        python_callable=transform_and_score,
    )

    t4 = PythonOperator(
        task_id="send_daily_report",
        python_callable=send_daily_report,
    )

    t1 >> t2 >> t3 >> t4
