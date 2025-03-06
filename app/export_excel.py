import pandas as pd
import io

from starlette.responses import StreamingResponse

from app.database import connect_to_db


def export_to_excel(db_name:str) :
    result = get_all_data_from_table(db_name, db_name)
    differences = get_all_data_from_table(db_name, 'differences')
    combined_df = pd.merge(result, differences, how='left', left_on='id', right_on='run_id')
    # Drop the `run_id` column if you don't want it in the final result
    combined_df.drop('run_id', axis=1, inplace=True)
    # Export to Excel
    # combined_df.to_excel('Result.xlsx', index=False)
    # Save DataFrame to an in-memory buffer
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        combined_df.to_excel(writer, index=False, sheet_name="Combined Data")

    output.seek(0)  # Reset buffer position to the beginning

    # Return file as a response
    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition": "attachment; filename=Result.xlsx"})


def get_all_data_from_table(db_name:str,table_name: str) :
    conn, cursor = connect_to_db(db_name)
    cursor.execute(f"SELECT * from {table_name}")
    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    conn.close()
    return pd.DataFrame(results, columns=columns)


# # Connect to your SQLite database
# conn = sqlite3.connect('your_database.db')
# cursor = conn.cursor()
#
# # Fetch data from the first table
# cursor.execute("SELECT * FROM first_table")
# first_table_data = cursor.fetchall()
# first_table_columns = [desc[0] for desc in cursor.description]
#
# # Fetch data from the second table
# cursor.execute("SELECT * FROM second_table")
# second_table_data = cursor.fetchall()
# second_table_columns = [desc[0] for desc in cursor.description]
#
# # Convert data to DataFrame
# first_table_df = pd.DataFrame(first_table_data, columns=first_table_columns)
# second_table_df = pd.DataFrame(second_table_data, columns=second_table_columns)
#
# # Merge the tables based on the relationship
# # 'id' is in the first table, 'run_id' is in the second table
# combined_df = pd.merge(first_table_df, second_table_df, how='left', left_on='id', right_on='run_id')
# # Drop the `run_id` column if you don't want it in the final result
# combined_df.drop('run_id', axis=1, inplace=True)
# # Export to Excel
# combined_df.to_excel('combined_table.xlsx', index=False)
#
# # Close the database connection
# conn.close()
#
# print("Exported to combined_table.xlsx")
