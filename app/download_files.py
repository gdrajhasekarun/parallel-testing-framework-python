import os
from io import BytesIO

import requests
from bs4 import BeautifulSoup

from app.database import connect_to_db, update_tables
import pandas as pd
import dask.dataframe as dd
import numpy as np

from app.dataframe_to_db.dataframe_to_database import ExcelDatabaseHandler
from app.file_readers.xl_file_reader_impl import ExcelReader


class DownloadFiles:

    def __init__(self):
        self.base_url = "https://hfs.illinois.gov/medicalproviders/medicaidreimbursement"
        self.downloads_folder = "Downloads"
        self.merged_file = "Consolidated_ExcelFile.xlsx"

    def __get_files_link(self, page_name: str, archive: str):
        list_of_pages = [page_name, f"{page_name}/archive{archive.lower()}fee"]
        i = 10
        links = []
        for page in list_of_pages:
            if i < 0:
                break
            response = requests.get(f"{self.base_url}/{page}.html")
            if response.status_code != 200:
                raise Exception(f"Not able to access the link provider {self.base_url}/{page_name}.html")

            soap = BeautifulSoup(response.text, "html.parser")


            for link in soap.find_all("a", href=True):
                if i < 0:
                    break
                file_url = link['href']
                if file_url.endswith((".xls", ".xlsx", ".csv")):
                    title = link.get_text()
                    if title.find("Effective") == -1:
                        continue
                    if page.find("archive") != -1:
                        i = i - 1
                    str_len = title.find("Effective") + len("Effective")
                    effective_date = title[str_len + 1: str_len + 11]
                    file_detail = {}
                    if not file_url.startswith("http"):
                        file_url = "https://hfs.illinois.gov" + file_url
                    file_detail["date"] = effective_date
                    file_detail["url"] = file_url
                    links.append(file_detail)
        return links

    def __download_files(self, links):
        if not os.path.exists(self.downloads_folder):
            os.makedirs(self.downloads_folder)
        for link in links:
            file_name = os.path.join(self.downloads_folder, os.path.basename(link['url']))
            print(f"Downloading {file_name}...")

            response = requests.get(link['url'], stream=True)
            if response.status_code == 200:
                with open(file_name, "wb") as file:
                    for chunk in response.iter_content(chunk_size=1024):
                        file.write(chunk)
                link["file_path"] = file_name
            else:
                print(f"Failed to download {link}")

    def __read_data_from_file(self, table_name, file_lists):
        excel_database_handler = ExcelDatabaseHandler(table_name)
        for file in file_lists:
            if not file["file_path"].endswith(".xlsx"):
                continue
            df = ExcelReader(table_name).read_file(file["file_path"], file["date"])
            excel_database_handler.create_database_table(df.columns)
            excel_database_handler.insert_value_to_database(df)



    def upload_files_content(self, page_name: str, archive: str):
        try:
            file_lists = self.__get_files_link(page_name, archive)
            self.__download_files(file_lists)
            self.__read_data_from_file(page_name, file_lists)
        except Exception as e:
            raise Exception("Unable to Download the content")

    def get_value_from_database(self, page_name, column_name, column_value, service_column, service_date):
        return ExcelDatabaseHandler(page_name).get_value_from_database(column_name, column_value, service_column, service_date)