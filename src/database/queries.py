import os
from sqlalchemy import text
from src.database.connection import get_engine


def create_tables():
    engine = get_engine()
    base_dir = os.path.dirname(__file__)

    sql_files = [
        os.path.join(base_dir, "sql", "create_raw_data.sql"),
        os.path.join(base_dir, "sql", "create_gold_data.sql"),
    ]

    with engine.connect() as conn:
        for sql_file in sql_files:
            with open(sql_file, "r") as f:
                conn.execute(text(f.read()))
        conn.commit()
    print("✅ Tables created successfully")


def insert_raw_data(df):
    engine = get_engine()
    df.to_sql("raw_data", engine, if_exists="append", index=False)
    print(f"✅ Inserted {len(df)} rows to raw_data")


def fetch_raw_data(ticker=None, start_date=None):
    engine = get_engine()
    query = "SELECT * FROM raw_data"
    filters = []

    if ticker:
        filters.append(f"ticker = '{ticker}'")
    if start_date:
        filters.append(f"date >= '{start_date}'")
    if filters:
        query += " WHERE " + " AND ".join(filters)

    query += " ORDER BY date ASC, ticker ASC"

    import pandas as pd

    with engine.connect() as conn:
        return pd.read_sql(query, conn.connection)


def fetch_gold_data(ticker=None, start_date=None):
    engine = get_engine()
    query = "SELECT * FROM gold_data"
    filters = []

    if ticker:
        filters.append(f"ticker = '{ticker}'")
    if start_date:
        filters.append(f"date >= '{start_date}'")
    if filters:
        query += " WHERE " + " AND ".join(filters)

    query += " ORDER BY date ASC, ticker ASC"

    import pandas as pd

    with engine.connect() as conn:
        return pd.read_sql(query, conn.connection)


def get_top8(date):
    engine = get_engine()
    query = f"""
        SELECT * FROM gold_data
        WHERE date = '{date}'
        ORDER BY score DESC, relative_volume DESC, rsi DESC, ticker ASC
        LIMIT 8
    """
    import pandas as pd

    with engine.connect() as conn:
        return pd.read_sql(query, conn.connection)
