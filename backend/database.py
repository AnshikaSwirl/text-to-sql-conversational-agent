import os
from sqlalchemy import create_engine, inspect, text
from urllib.parse import quote_plus

server = os.getenv("AZURE_SQL_SERVER")
database = os.getenv("AZURE_SQL_DATABASE")
username = os.getenv("AZURE_SQL_USERNAME")
password = os.getenv("AZURE_SQL_PASSWORD")
driver = "ODBC Driver 18 for SQL Server"

username_encoded = quote_plus(username)
password_encoded = quote_plus(password)
driver_encoded = quote_plus(driver)

connection_string = (
    f"mssql+pyodbc://{username_encoded}:{password_encoded}"
    f"@{server}/{database}?driver={driver_encoded}"
    f"&Encrypt=yes&TrustServerCertificate=no"
)

engine = create_engine(connection_string)

def get_engine():
    return engine

def get_table_schema(table_name):
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name)
    return {col["name"]: str(col["type"]) for col in columns}

def get_all_table_names():
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
        )
        return [row[0] for row in rows]
