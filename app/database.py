import sqlite3


def connect_to_db(db_name:str):
    conn = sqlite3.connect(db_name + '.db')
    cursor = conn.cursor()
    return conn, cursor