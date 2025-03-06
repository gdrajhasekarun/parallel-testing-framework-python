from deepdiff import DeepDiff

from app.cache import cache_container, get_cache_value
from app.database import connect_to_db
import pandas as pd

from app.model import job_store


def process_items(batch_id: str ):
    source_table = get_cache_value("source_table")
    target_table = get_cache_value("target_table")
    excluded_column = get_cache_value("excluded_column")
    db_name = get_cache_value("db_name")
    s_conn, s_cursor = connect_to_db('source')
    t_conn, t_cursor = connect_to_db('target')
    conn, cursor = connect_to_db(db_name)
    cursor.execute(f"PRAGMA table_info({db_name})")
    columns = [row[1] for row in cursor.fetchall() if row[1] not in[ 'batch_id', 'result']]
    comb_query = f"SELECT {', '.join(columns)} FROM {db_name} WHERE batch_id=?"
    cursor.execute(comb_query, (batch_id,))
    results = cursor.fetchall()
    condition = ' AND '.join([f"{col} = ?" for col in columns if col != 'id'])
    for result in results:
        s_data = execute_query(condition, result, s_cursor, source_table)
        t_data = execute_query(condition, result, t_cursor, target_table)
        diff = DeepDiff(s_data, t_data, ignore_order=True)
        differences = []
        for key, value in diff.get("values_changed", {}).items():
            row_index = key.split("[")[1].split("]")[0]  # Extract row index from DeepDiff key
            column_name = key.split("'")[1] # Extract column name from DeepDiff key
            if column_name in excluded_column:
                continue
            old_value = value["old_value"]
            new_value = value["new_value"]
            differences.append({
                "run_id": result[0],
                "row_index": int(row_index),
                "column_name": column_name,
                "old_value": str(old_value),
                "new_value": str(new_value)
            })
        # Insert differences into the table
        cursor.executemany("""
            INSERT INTO differences (run_id, row_index, column_name, old_value, new_value)
            VALUES (:run_id, :row_index, :column_name, :old_value, :new_value)
        """, differences)
        status = "Pass" if len(differences) == 0 else "Fail"
        update_sql = f"""
        UPDATE {db_name}
        SET result = ?
        WHERE {condition}
        """
        cursor.execute(update_sql, (status, *result[1:]))
        conn.commit()
    job_store[batch_id] = "completed"


def execute_query(condition, result, cursor, table):
    source_query = f"SELECT * FROM {table} where {condition}"
    cursor.execute(source_query, result[1:])
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=columns)

    # Convert the DataFrame to a list of dictionaries
    return df.to_dict(orient="records")


