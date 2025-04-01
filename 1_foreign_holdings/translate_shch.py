# pip install every non-built-in library before importing modules
import subprocess
import sys
def install_package(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])
install_package("regex")
install_package("openpyxl")
install_package("deepl")
install_package("xlsxwriter")
install_package("ipython")
# not a notebook setting, import display module
from IPython.display import display
# Importing Modules
import os
import sys
import numpy as np
import pandas as pd
import regex as re
import openpyxl
import time
import deepl
import xlsxwriter

# Defining Paths
dpmain = sys.argv[1]
dpraw = dpmain + '/input/shch/'

# Ensure the destination directory exists
destination_dir = dpraw + 'shch_en/'

if os.path.exists(destination_dir):
    print("Translated files available")
    
else:
    os.makedirs(destination_dir)

    # Create a Translator object providing your DeepL API authentication key
    translator = deepl.Translator("<DeepL API Key>")

    # Translating files
    h_files = os.listdir(path = dpraw + 'raw_cn/')
    for file in h_files:
        display('. ' + file)
        ss=openpyxl.load_workbook(dpraw + 'raw_cn/' + file)
        sheets =["总览","发行","兑付金额","托管量（按产品）","投资者持有结构","报表说明"]
        std_sheets=["表一","表二","表三","表四","表五","报表项目说明"]
        n= len(sheets)
        #printing the sheet names
        for i in range(0,n):
            try: 
                ss_sheet = ss[sheets[i]]  
                ss_sheet.title = std_sheets[i]
            except:  
                continue
        ss.save(dpraw + 'shch_en/' + file)

    for file in h_files:
        display('. ' + file)
        time.sleep(5)
        sheets =["表1","表2","表3","表4","表5","表一","表二","表三","表四","表五","报表项目说明"]

        # Initialize xlsx writer
        file_new = re.sub('.xlsx', '_en_js.xlsx', file)
        writer = pd.ExcelWriter(dpraw + 'shch_en/' + file_new, engine='xlsxwriter')
        workbook = writer.book

        for sheet in sheets:
            sheet_en = str(translator.translate_text(sheet, target_lang="EN-US"))
            if sheet_en[0] == "表":
                sheet_en = sheet_en.replace("表","Table ", 1)            
            print(sheet_en)
            try:
                data = pd.read_excel(dpraw + 'shch_en/' + file,
                                 header = None,
                                 sheet_name = sheet,
                                 dtype = 'str')
                data=data.replace(np.nan, "", regex=True)
                for col in range(0, len(data.columns)):
                    time.sleep(5)
                    data[col] = translator.translate_text(data[col], target_lang="EN-US") 
                    translator.raise_Exception = True
                    print(data[col])

                data.fillna('', inplace=True)
                display(data)
            except:
                continue
            data.to_excel(writer, sheet_name= sheet_en, header = False, index = False)
        writer.save()
