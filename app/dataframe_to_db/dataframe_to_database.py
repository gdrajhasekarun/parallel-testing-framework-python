from app.database import connect_to_db, update_tables
from app.dataframe_to_db.database_interfaces import DatabaseHandler


class ExcelDatabaseHandler(DatabaseHandler):

    def __init__(self, table_name):
        super().__init__(table_name)

    def create_database_table(self, column_list):
        columns = ', '.join([
            f'"{update_tables(col)}" DATETIME' if col == "Effective Date" else f'"{update_tables(col)}" TEXT'
            for col in column_list
        ])
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {update_tables(self.table_name)} ({columns})")

    def insert_value_to_database(self, data_frame):
        for idx, row in enumerate(data_frame.itertuples(index=False, name=None), start=1):
            try:
                placeholders = ', '.join(['?' for _ in row])
                self.cursor.execute(f"INSERT INTO {update_tables(self.table_name)} VALUES ({placeholders})", row)
            except Exception as e:
                print(f"Error inserting row {idx} in sheet '{self.table_name}': {e}")
                print(f"Problematic row data: {row}")  # Print the actual row data
        self.conn.commit()





