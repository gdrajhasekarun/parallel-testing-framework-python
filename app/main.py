import fileinput
from io import BytesIO
from typing import List

from fastapi import FastAPI, File, UploadFile, Request
from starlette.background import BackgroundTasks

from app.batch_processing import process_items
from app.cache import put_value_to_cache
from app.excel_to_db import excel_to_db, get_list_tables, get_col_names_from_db, create_batch_job_setup, \
    get_status_job_id
from app.export_excel import export_to_excel
from app.file_handler import FileHandler
from app.model import BatchRequest, job_store
import os
import io
import aiofiles

app = FastAPI()
file_handler = FileHandler()

# @app.get("/batch/clearData")
# async def clear_files() :
#     # Get the current working directory
#     working_dir = os.getcwd()
#
#     # List all files in the directory
#     files = os.listdir(working_dir)
#
#     # Loop through the files and delete those with a .db extension
#     for file in files:
#         if file.endswith('.db'):
#             file_path = os.path.join(working_dir, file)
#             try:
#                 os.remove(file_path)
#                 print(f"Deleted: {file_path}")
#             except Exception as e:
#                 print(f"Error deleting {file_path}: {e}")

def handle_files(files: List[UploadFile], db_source: str):
    for file in files:
        file_path = f"/tmp/{file.filename}"  # Save file temporarily

        # Save file to disk
        with open(file_path, "wb") as f:
            f.write(file.file.read())  # Correct way to read file content

        # Process file
        with open(file_path, 'rb') as f:
            status = "fail"
            try:
                excel_to_db(BytesIO(f.read()), db_source)
                status = "pass"
            except Exception as e:
                status = "fail"
            FileHandler().update_file_upload_status(status, file.filename)

@app.post("/batch/upload")
async def update_to_database(request: Request, background_tasks: BackgroundTasks):
    form = await request.form()  # Parse incoming form data
    files: List[UploadFile] = form.getlist("file")  # Extract multiple files
    db_source: str = form.get("db_source")  # Get db_source parameter
    file_names = [file.filename for file in files]
    file_handler.save_files_names_to_database(file_names, db_source)
    background_tasks.add_task(handle_files, files, db_source)
    return {"message": "started"}

@app.get("/batch/upload-status")
async def upload_status_files():
    return {"filestatus":file_handler.get_file_status(), "status": file_handler.get_file_processing_status()}

@app.get("/batch/data-source")
async def distinct_data_source():
    return file_handler.get_distinct_sources()

@app.get("/batch/tables-list/{target}")
async def get_list_of_tables(target: str):
    return get_list_tables(target)

@app.post("/batch/update/{target}")
async def upload_excel(target: str, file: List[UploadFile] = File(...)):
    for f in file:
        async with aiofiles.open(file.filename, "wb") as f:
            while chunk := await file.read(1024 * 1024):  # Read 1 MB at a time
                await f.write(chunk)
        with open(file.filename, "rb") as f:
            excel_to_db(BytesIO(f.read()), target)
    return get_list_tables(target)
    # return JSONResponse(content={"message": "Excel file uploaded and data saved"}, status_code=200)

@app.get("/batch/columns")
async def retrieve_columns(request: Request):
    sheet_name = request.query_params.get("sheetName")
    return get_col_names_from_db(sheet_name)

@app.post("/batch/compare")
async def setup_batch(batch_request: BatchRequest):
    put_value_to_cache('source_table', batch_request.sourceTable)
    put_value_to_cache('target_table', batch_request.targetTable)
    put_value_to_cache('excluded_column', batch_request.excludedColumns)
    put_value_to_cache('db_name', batch_request.compareSessionName.replace(" ", "_"))
    return create_batch_job_setup(batch_request.compareSessionName, batch_request.sourceTable,batch_request.primaryColumns)


@app.get("/batch/compare")
async def trigger_batch(request: Request, background_tasks: BackgroundTasks):
    job_id = request.query_params.get("jobId")
    job_store[job_id] = "in_progress"
    background_tasks.add_task(process_items, job_id)
    return {"jobId": job_id}


@app.get("/batch/status")
async def get_status(request: Request):
    job_id = request.query_params.get("jobId")
    session_id = request.query_params.get("sessionName")
    pass_count, fail_count, blank_count = get_status_job_id(session_id.replace(" ", "_"), job_id)
    status = job_store.get(job_id, "failed")
    return {"jobId": job_id, "ExecutionStatus": status, "Pass": pass_count,
                "Fail": fail_count, "Pending": blank_count}

@app.get("/batch/download")
async def download_excel(request: Request):
    session_id = request.query_params.get("sessionName")
    return export_to_excel(session_id.replace(" ", "_"))