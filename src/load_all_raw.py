import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def clean_cols(cols):
    return [c.strip().upper().replace(" ", "_").replace("-", "_") for c in cols]


def table_name_from_file(path: Path) -> str:
    return f"{path.stem.upper()}_RAW"


def main():
    load_dotenv()

    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    role = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "ANALYTICS_WH")
    database = os.getenv("SNOWFLAKE_DATABASE", "LOGISTICS_DB")
    schema = os.getenv("SNOWFLAKE_SCHEMA", "RAW")

    data_dir = Path("data/raw/logistics_ops")
    if not data_dir.exists():
        raise FileNotFoundError(f"Missing folder: {data_dir.resolve()}")

    csv_files = sorted(data_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in: {data_dir.resolve()}")

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
        cur = conn.cursor()
        cur.execute(f"USE ROLE {role}")
        cur.execute(f"USE WAREHOUSE {warehouse}")
        cur.execute(f"USE DATABASE {database}")
        cur.execute(f"USE SCHEMA {schema}")

        cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
        print("Session context:", cur.fetchone())

        for csv_path in csv_files:
            tname = table_name_from_file(csv_path)
            print(f"\nLoading {csv_path.name} -> {database}.{schema}.{tname}")

            df = pd.read_csv(csv_path)
            df.columns = clean_cols(df.columns)

            success, nchunks, nrows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name=tname,
                auto_create_table=True,
                overwrite=True,  # fine for now
            )

            cur.execute(f"SELECT COUNT(*) FROM {database}.{schema}.{tname}")
            count = cur.fetchone()[0]

            print(f"write_pandas success={success}, chunks={nchunks}, rows_loaded={nrows}, verified_count={count}")

        print("\n✅ Finished loading all raw tables.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
