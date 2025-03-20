import itertools
import uuid
import pandas as pd
from typing import List, Dict
import dask.dataframe as dd

from app.database import connect_to_db, update_tables
import numpy as np

class ExcelHandler:

    def __read_excel_in_chunks(self, excel_file_content, sheet_name):
        df = pd.read_excel(excel_file_content, sheet_name=sheet_name, engine='openpyxl')
        return dd.from_pandas(df, npartitions=5)

    def __process_sheet(self, sheet_name, excel_file_content, cursor, conn):
        try:
            dask_df = self.__read_excel_in_chunks(excel_file_content, sheet_name)
            df = dask_df.compute()
            df.replace({np.nan: 'Null'}, inplace=True)
            # Ensure DataFrame has columns
            if df.empty or df.columns.empty:
                print(f"Warning: Sheet '{sheet_name}' is empty. Skipping...")
                return
            # Create a table for the sheet (if it doesn't already exist)
            columns = ', '.join([f'"{col}" TEXT' for col in df.columns])
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {update_tables(sheet_name)} ({columns})")

            # Insert data from the DataFrame into the corresponding table
            for idx, row in enumerate(df.itertuples(index=False, name=None), start=1):
                try:
                    placeholders = ', '.join(['?' for _ in row])
                    cursor.execute(f"INSERT INTO {update_tables(sheet_name)} VALUES ({placeholders})", row)
                except Exception as e:
                    print(f"Error inserting row {idx} in sheet '{sheet_name}': {e}")
                    print(f"Problematic row data: {row}")  # Print the actual row data

            # Commit each insert operation
            conn.commit()
            print(f"Sheet {sheet_name} processed successfully")
        except Exception as e:
            print(f"Sheet {sheet_name} cannot be processed: {e}")

    # Function to convert Excel file to SQLite DB
    def excel_to_db(self, excel_file_content, db_file_path):
        try:

            # Open the SQLite database (or create one if it doesn't exist)
            conn, cursor = connect_to_db(db_file_path)

            # Read the Excel file into a pandas ExcelFile object
            excel_file = pd.ExcelFile(excel_file_content)

            # Iterate over each sheet in the Excel file
            for sheet_name in excel_file.sheet_names:
                # Read the sheet into a DataFrame
                self.__process_sheet(sheet_name, excel_file_content, cursor, conn)
            conn.close()
        except Exception as e:
            raise Exception("Failed to Process the File")

    def get_list_tables(self, db_name: str) -> List[str]:
        conn, cursor = connect_to_db(db_name)
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tables

    def get_col_names_from_db(self, sheet_name: str) -> List[str]:
        conn, cursor = connect_to_db('source')

        cursor.execute(f"PRAGMA table_info({update_tables(sheet_name)})")
        columns = [row[1] for row in cursor.fetchall()]
        conn.close()
        return columns

    def create_batch_job_setup(self, db_name: str, table_name: str, primary_col: List[str]):
        col_name = "TEXT, ".join(primary_col)
        table = db_name.replace(" ", "_")
        conn, cursor = connect_to_db(table)
        sql = f"""
        CREATE TABLE IF NOT EXISTS {update_tables(table)} (
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
        return self.__generate_unique_combinarions(table, table_name, primary_col)

    def __generate_unique_combinarions(self, db_name: str, table_name: str, primary_col: List[str]):
        col_name = ", ".join(primary_col)

        query = f"""
        SELECT DISTINCT {col_name} FROM {update_tables(table_name)}
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
                f"""INSERT INTO {update_tables(db_name)} (batch_id, {col_name}, result) 
                    VALUES ({placeholders})""",
                values
            )
        conn.commit()
        conn.close()
        return {"jobId": batch_id, "total_combinations": len(combinations)}


    def get_status_job_id(self, db_name: str, job_id: str):
        sql = f"""
            SELECT 
                COUNT(CASE WHEN result = 'Pass' THEN 1 END) AS pass_count,
                COUNT(CASE WHEN result = 'Fail' THEN 1 END) AS fail_count,
                COUNT(CASE WHEN result IS NULL OR result = '' THEN 1 END) AS blank_count
            FROM {update_tables(db_name)} where batch_id=?;
            """
        conn, cursor = connect_to_db(db_name)
        cursor.execute(sql, (job_id,))
        pass_count, fail_count, blank_count = cursor.fetchone()
        conn.close()
        return pass_count, fail_count, blank_count

    def get_data_from_sheet_db(self, sheet_name: str, db_name:str) -> List[Dict]:
        conn, cursor = connect_to_db(db_name)
        cursor.execute(f"SELECT * FROM {update_tables(sheet_name)}")
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
