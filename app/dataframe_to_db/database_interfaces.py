from abc import ABC, abstractmethod

from app.database import connect_to_db, update_tables


class DatabaseHandler(ABC):

    def __init__(self, table_name):
        self.conn, self.cursor = connect_to_db(table_name)
        self.table_name = table_name

    @abstractmethod
    def create_database_table(self, column_list):
        pass

    @abstractmethod
    def insert_value_to_database(self, data_frame):
        pass

    def get_value_from_database(self, column_name, column_value, service_column, service_date):
        sql = f"""
            SELECT * FROM {update_tables(self.table_name)}
            WHERE {update_tables(column_name)} = ? 
              AND {update_tables(service_column)} <= ?
            ORDER BY {update_tables(service_column)} DESC
--             LIMIT 1
        """
        self.cursor.execute(sql, (column_value, service_date))
        return self.cursor.fetchall()