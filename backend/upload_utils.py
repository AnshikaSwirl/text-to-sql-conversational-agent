import pandas as pd
from backend.database import engine

def upload_new_table(csv_path: str, table_name: str):
    # Load CSV
    df = pd.read_csv(csv_path)

    # Clean same as ingestion step
    df.drop_duplicates(inplace=True)
    df.fillna("Unknown", inplace=True)
    df.columns = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns]

    # Ensure lowercase table name
    table_name = table_name.lower()

    # Upload to Azure SQL
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)

    return f" Uploaded {len(df)} rows to table '{table_name}' successfully!"