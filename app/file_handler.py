import sqlite3
from typing import List

from app.database import connect_to_db
from app.constants import Constants

class FileHandler:

    def __init__(self):
        self.conn, self.cursor = connect_to_db('files_status')
        self.__create_table()

    def __create_table(self):
        self.cursor.execute(f"""
                            CREATE TABLE IF NOT EXISTS {Constants.FILE_STORE_TABLE} (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            FileName TEXT,
                            SOURCE TEXT,
                            UPLOAD_STATUS TEXT
                            )
                        """)
        self.conn.commit()

    def save_files_names_to_database(self, files: List[str], db_source: str):
        # conn, cursor = connect_to_db('files_status')
        for file in files:
            self.__insert_files_to_database(file, db_source)
        self.conn.commit()

    def get_file_status(self):
        sql=f"""SELECT * FROM {Constants.FILE_STORE_TABLE}"""
        self.cursor.execute(sql)
        result =  self.cursor.fetchall()
        # Get column names
        columns = [description[0] for description in self.cursor.description]
        # Convert rows into a list of dictionaries
        data = [dict(zip(columns, row)) for row in result]
        return data

    def get_distinct_sources(self):
        try:
            sql = f"""SELECT DISTINCT(SOURCE) from {Constants.FILE_STORE_TABLE}"""
            self.cursor.execute(sql)
            return self.cursor.fetchall()
        except sqlite3.OperationalError as e:
            if Constants.FILE_STORE_TABLE in str(e):
                self.__create_table()
            return []


    def get_file_processing_status(self):
        sql = f"""
                SELECT 
                    COUNT(CASE WHEN UPLOAD_STATUS = 'pass' THEN 1 END) AS pass_count,
                    COUNT(CASE WHEN UPLOAD_STATUS = 'fail' THEN 1 END) AS fail_count,
                    COUNT(CASE WHEN UPLOAD_STATUS IS NULL OR UPLOAD_STATUS = '' THEN 1 END) AS blank_count
                FROM {Constants.FILE_STORE_TABLE};
                """
        self.cursor.execute(sql)
        pass_count, fail_count, blank_count = self.cursor.fetchone()
        return {"pass": pass_count, "fail": fail_count, "yet": blank_count}

    def update_file_upload_status(self, status, file_name):
        sql = f"""UPDATE {Constants.FILE_STORE_TABLE} SET UPLOAD_STATUS=? WHERE FileName=?"""
        self.cursor.execute(sql, (status, file_name))
        self.conn.commit()


    def __insert_files_to_database(self, file: str, source: str):
        sql = f"""INSERT INTO {Constants.FILE_STORE_TABLE} (FileName, SOURCE) VALUES (?, ?)"""
        self.cursor.execute(sql, (file, source))
        self.conn.commit()

    def __del__(self):
        self.conn.commit()
        self.conn.close()