import pyodbc
import pandas as pd
import boto3

# 1. SETUP THE TOOLS
# This 'boto3.client' will automatically find the keys you saved in 'aws configure'
s3_client = boto3.client('s3')

# Update this to your specific bucket name from CloudFormation
BUCKET_NAME = 'adventureworks-landing-458108203662'

# SQL Server Connection String
conn_str = (
    "Driver={SQL Server};"
    "Server=localhost\SQLEXPRESS;"
    "Database=AdventureWorksDW2025;"
    "Trusted_Connection=yes;"
)


def run_full_pipeline():
    try:
        # --- PHASE 1: CONNECT ---
        print("Connecting to SQL Server...")
        conn = pyodbc.connect(conn_str)

        # We want to move these two tables
        tables = ["FactInternetSales", "DimProduct"]

        for table_name in tables:
            print(f"\n--- Processing {table_name} ---")

            # --- PHASE 2: EXTRACT (SQL to RAM) ---
            print(f"Reading {table_name} from database...")
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

            # --- PHASE 3: CONVERT (RAM to Local CSV) ---
            local_file = f"{table_name}.csv"
            df.to_csv(local_file, index=False)
            print(f"Created local file: {local_file}")

            # --- PHASE 4: LOAD (Local CSV to AWS S3) ---
            s3_key = f"landing/{local_file}"
            print(f"Uploading to S3 bucket: {BUCKET_NAME}...")
            s3_client.upload_file(local_file, BUCKET_NAME, s3_key)
            print(f"SUCCESS: {table_name} is now in S3!")

        conn.close()

    except Exception as e:
        print(f"ERROR: Something went wrong: {e}")


# THIS IS THE START BUTTON
# Without these two lines, the script will do nothing when you run it.
if __name__ == "__main__":
    run_full_pipeline()
