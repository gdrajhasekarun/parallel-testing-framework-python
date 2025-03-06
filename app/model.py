# Define request body schema
from pydantic import BaseModel
from typing import List


class BatchRequest(BaseModel):
    sourceTable: str
    targetTable: str
    primaryColumns: List[str]
    excludedColumns: List[str]
    compareSessionName: str

job_store = {}