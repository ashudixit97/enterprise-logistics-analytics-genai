import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def main():
    load_dotenv()

    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    role = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "ANALYTICS_WH")
    database = os.getenv("SNOWFLAKE_DATABASE", "LOGISTICS_DB")
    schema = os.getenv("SNOWFLAKE_SCHEMA", "RAW")

    # Adjust filename if Kaggle uses a different name
    csv_candidates = [
        Path("data/raw/logistics_ops/DELIVERY_EVENTS.csv"),
        Path("data/raw/logistics_ops/delivery_events.csv"),
        Path("data/raw/logistics_ops/Delivery_Events.csv"),
    ]
    csv_path = next((p for p in csv_candidates if p.exists()), None)
    if csv_path is None:
        raise FileNotFoundError("DELIVERY_EVENTS.csv not found in data/raw/logistics_ops/")

    df = pd.read_csv(csv_path)
    df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]

    table_name = "DELIVERY_EVENTS_RAW"

    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        role=role,
        warehouse=warehouse,
        database=database,
        schema=schema,
    )

  
    try:
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            auto_create_table=True,
            overwrite=True,
        )
        print(f"write_pandas success={success}, chunks={nchunks}, rows_loaded={nrows}")

        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {database}.{schema}.{table_name};")
        print("Row count in Snowflake:", cur.fetchone()[0])
    finally:
        conn.close()

if __name__ == "__main__":
    main()
