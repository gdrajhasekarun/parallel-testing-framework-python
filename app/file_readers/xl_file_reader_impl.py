from io import BytesIO

from app.file_readers.file_reader_interfaces import FileReader
import pandas as pd
import numpy as np



class ExcelReader(FileReader):

    def __init__(self, target_sheet_name):
        self.target_sheet_name = target_sheet_name

    def read_file(self, file_name, effective_date):
        with open(file_name, 'rb') as f:
            excel_file = pd.ExcelFile(BytesIO(f.read()))
            for sheet_name in excel_file.sheet_names:
                if str(sheet_name).lower().startswith(self.target_sheet_name):
                    break
            df = pd.read_excel(excel_file, sheet_name=sheet_name, engine='openpyxl')
            first_row = 0
            for index, row in df.iterrows():
                if str(df.iloc[index, 0]).lower().startswith("procedure code"):
                    first_row = index
                    break
            df = pd.read_excel(excel_file, sheet_name=sheet_name, engine='openpyxl', skiprows=first_row + 1)
            df.replace({np.nan: 'Null'}, inplace=True)
            list_of_columns = ["Procedure Code", "Note", "ProgCov", "Eff Date", "HP", "NDC Ind", "Surg Ind", "AV",
                               "M1", "M2", "Asst Surg", "Co-Surg",
                               "Unit price", "Max Qty", "State Max", "Add on Surg", "Add on Child", "Add on Adult",
                               "Smart Unit price", "Smart State Max"]
            if len(df.columns) > len(list_of_columns):
                df = df.iloc[:, :len(list_of_columns)]
            df.columns = list_of_columns[: len(list_of_columns)]
            df["Effective Date"] = effective_date
            return df