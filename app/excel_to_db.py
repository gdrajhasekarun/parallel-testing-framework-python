import itertools
import sqlite3
import uuid

import pandas as pd
from io import BytesIO
from typing import List, Dict

from app.database import connect_to_db


# Function to convert Excel file to SQLite DB
def excel_to_db(excel_file_content, db_file_path):
    # Open the SQLite database (or create one if it doesn't exist)
    conn, cursor = connect_to_db(db_file_path)

    # Read the Excel file into a pandas ExcelFile object
    excel_file = pd.ExcelFile(excel_file_content)

    # Iterate over each sheet in the Excel file
    for sheet_name in excel_file.sheet_names:
        # Read the sheet into a DataFrame
        df = excel_file.parse(sheet_name)

        # Create a table for the sheet (if it doesn't already exist)
        columns = ', '.join([f'"{col}" TEXT' for col in df.columns])
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {sheet_name} ({columns})")

        # Insert data from the DataFrame into the corresponding table
        for row in df.itertuples(index=False, name=None):
            placeholders = ', '.join(['?' for _ in row])
            cursor.execute(f"INSERT INTO {sheet_name} VALUES ({placeholders})", row)

        # Commit each insert operation
        conn.commit()

    # Close the connection to the database
    conn.close()

def get_list_tables(db_name: str) -> List[str]:
    conn, cursor = connect_to_db(db_name)
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()
    return tables

def get_col_names_from_db(sheet_name: str) -> List[str]:
    conn, cursor = connect_to_db('source')

    cursor.execute(f"PRAGMA table_info({sheet_name})")
    columns = [row[1] for row in cursor.fetchall()]
    conn.close()
    return columns

def create_batch_job_setup(db_name: str, table_name: str, primary_col: List[str]):
    col_name = "TEXT, ".join(primary_col)
    table = db_name.replace(" ", "_")
    conn, cursor = connect_to_db(table)
    sql = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id TEXT,
        {col_name},
        result TEXT
    )
    """
    cursor.execute(sql)
    conn.commit()
    # Create a table to store differences
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS differences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER,
            row_index INTEGER,
            column_name TEXT,
            old_value TEXT,
            new_value TEXT
        )
    """)
    conn.commit()
    conn.close()
    return generate_unique_combinarions(table, table_name, primary_col)

def generate_unique_combinarions(db_name: str, table_name: str, primary_col: List[str]):
    col_name = ", ".join(primary_col)

    query = f"""
    SELECT DISTINCT {col_name} FROM {table_name}
    """
    conn, cursor = connect_to_db('source')
    cursor.execute(query)
    results = cursor.fetchall()
    combinations = list(itertools.combinations(results, len(primary_col)))
    conn.close()
    conn, cursor = connect_to_db(db_name)
    batch_id = str(uuid.uuid4())
    placeholders = ", ".join(["?"] * (len(primary_col) + 2))
    for combo in combinations:
        flattened_combo = [item if isinstance(item, str) else item[0] for item in combo]
        values = (batch_id, *flattened_combo, "")
        cursor.execute(
            f"""INSERT INTO {db_name} (batch_id, {col_name}, result) 
                VALUES ({placeholders})""",
            values
        )
    conn.commit()
    conn.close()
    return {"jobId": batch_id, "total_combinations": len(combinations)}


def get_status_job_id(db_name: str, job_id: str):
    sql = f"""
        SELECT 
            COUNT(CASE WHEN result = 'Pass' THEN 1 END) AS pass_count,
            COUNT(CASE WHEN result = 'Fail' THEN 1 END) AS fail_count,
            COUNT(CASE WHEN result IS NULL OR result = '' THEN 1 END) AS blank_count
        FROM {db_name} where batch_id=?;
        """
    conn, cursor = connect_to_db(db_name)
    cursor.execute(sql, (job_id,))
    pass_count, fail_count, blank_count = cursor.fetchone()
    conn.close()
    return pass_count, fail_count, blank_count

def get_data_from_sheet_db(sheet_name: str, db_name:str) -> List[Dict]:
    conn, cursor = connect_to_db(db_name)
    cursor.execute(f"SELECT * FROM {sheet_name}")
    rows = cursor.fetchall()

    # Get column names
    columns = [description[0] for description in cursor.description]

    # Convert rows into a list of dictionaries
    data = [dict(zip(columns, row)) for row in rows]

    conn.close()
    return data

# # Example Usage
# excel_file_path = "example.xlsx"
# db_file_path = "example.db"
# excel_to_db(excel_file_path, db_file_path)
