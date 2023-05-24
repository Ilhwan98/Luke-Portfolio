import pandas as pd
import os
import pytz
import re
import dateparser
import dateutil.parser
from datetime import datetime
from dateutil import parser

csv_path = "C:/Users/E80-1798/Desktop/Payment Final"
change_path = "C:/Users/E80-1798/Desktop/Payment Data Change"

for files in os.listdir(csv_path):
    if files.endswith(".csv"):
        file_path = os.path.join(csv_path, files)
        filename = os.path.splitext(files)[0] + "_cleaned.csv"
        file_path_new = os.path.join(change_path, filename)
        if os.path.exists(file_path_new):
            print(f"{file_path_new} already exists, skipping...")
        else:
            try:
                if '_BR_' in files:
                    df = pd.read_csv(file_path, sep=',', header=6, low_memory=False)
                else:
                    df = pd.read_csv(file_path, sep=',', header=7, low_memory=False)
            
                #Account type/tipo de conta 제거
                headers = list(df.columns.values)
                for header in headers:
                    if "account type" in header.lower() or "tipo de conta" in header.lower():
                        df.drop(header, axis=1, inplace=True)

                #제품명 제거
                for index, row in df.iterrows():
                    if isinstance(row.iloc[2], str) and row.iloc[2] in ["Order", "Pedido"]:
                        df.iat[index, 5] = ""
                    else:
                        pass

                #날짜 변경
                for index, row in df.iterrows():
                    if isinstance(row.iloc[0], str):
                        date_obj = dateparser.parse(row.iloc[0])
                        if date_obj:
                            new_date_str = date_obj.strftime("%Y-%m-%d")
                            df.iat[index, 0] = new_date_str

                #SIS SKU 변경
                for index, row in df.iterrows():
                    if isinstance(row.iloc[4], str):
                        match1 = re.search(r'\b.*([A-Za-z]{3}\d{5}).*\b', row.iloc[4])
                        match2 = re.search(r'\b.*(\d{3}[A-Za-z]{2}\d{5}).*\b', row.iloc[4])
                        match3 = re.search(r'\b.*([A-Za-z]{1}\d{2}[A-Za-z]{2}\d{5}).*\b', row.iloc[4])
                        if match1:
                            new_value = match1.group(1)
                            df.iat[index, 4] = new_value
                        elif match2:
                            new_value = match2.group(1)
                            df.iat[index, 4] = new_value
                        elif match3:
                            new_value = match3.group(1)
                            df.iat[index, 4] = new_value
                        else:
                            new_value = str(row.iloc[4])
                            for substring in ["FBA-", "CO-", "FM-"]:
                                new_value = new_value.replace(substring, "")
                            df.iat[index, 4] = new_value

                #필요 없는 컬럼 제거
                if '_BR_' in files:
                    try:
                        df = df.drop(df.index[:5])
                        df.drop(df.columns[[1, 3]], axis=1, inplace=True)

                        df.insert(14, 'product sales tax', 0)
                        df.insert(16, 'shipping credits tax', 0)
                        df.insert(17, 'gift wrap credits', 0)
                        df.insert(18, 'giftwrap credits tax', 0)
                        df.insert(19, 'Regulatory Fee', 0)
                        df.insert(20, 'Tax On Regulatory Fee', 0)
                        df.insert(22, 'promotional rebates tax', 0)
                        df.insert(23, 'marketplace withheld tax', 0)
                    except IndexError:
                        print(f"CSV file '{files}' could not be processed and was skipped.")
                        continue
                else:    
                    try:
                        df = df.drop(df.index[:7])
                        df.drop(df.columns[[1, 3, 12]], axis=1, inplace=True)
                    except IndexError:
                        print(f"CSV file '{files}' could not be processed and was skipped.")
                        continue         

                filename = os.path.splitext(files)[0] + "_cleaned.csv"
                file_path = os.path.join(change_path, filename)
                df.to_csv(file_path, index=False)
            except pd.errors.ParserError:
                print(f"Skipping file {filename}: ParserError")
                continue

print("Done")

