from google.cloud import bigquery
import pandas as pd
import os
import sys
import time
from datetime import date, datetime
import logging
import pandas_gbq

# pip install openpyxl

invoice_yyyymm = "202111"  # input by user #202101
hotel_group = "IHG"  # input by user

# ======================================================================================================================
invoice_folder = "Invoice and PO line data"
invoice_file_name = "IHG invoice-"+invoice_yyyymm+".xlsx"

# Todo: check if this file name already uploaded to BigQuery?
invoice_file_path = "D:\\P+ Dashboard\\Data\\"+hotel_group+"\\"+invoice_folder+"\\"+invoice_yyyymm+"\\"+invoice_file_name
print(invoice_file_path)
invoice_df = pd.read_excel(invoice_file_path, engine='openpyxl')

# Todo: Food_Non-food category.xlsx tp pandas dataframe
food_nonfood_category_file_path = "D:\\P+ Dashboard\\Data\\General\\Food_Non-food category.xlsx"
print(food_nonfood_category_file_path)
food_nonfood_category_df = pd.read_excel(food_nonfood_category_file_path, engine='openpyxl')
# Add suffix to column name
food_nonfood_category_df = food_nonfood_category_df.add_suffix('_Category')
print(food_nonfood_category_df)

# =====================Process invoice_df =============================================================================
# Covert datetime format in column "Document Date" to only date format without time
invoice_df['Document Date'] = invoice_df['Document Date'].apply(lambda x: datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d') if str(x) != 'NaT' else None)
print(invoice_df['Document Date'])
print(invoice_df.dtypes)
print(invoice_df['Document Created At'])
print(invoice_df[invoice_df['Document Date'].isnull()])

# Replace "-" to 0 for Tax Amount column because data type of these column in  Bigquery schemaField are support float
invoice_df["Tax Amount"].astype(str).replace({"-": 0}, inplace=True)
invoice_df["Tax Percentage"].astype(str).replace({"-": 0}, inplace=True)
invoice_df["Adjustment Ex Tax"].astype(str).replace({"-": 0}, inplace=True)
invoice_df["Adjustment Tax"].astype(str).replace({"-": 0}, inplace=True)
invoice_df["Line Tax"].astype(str).replace({"-": 0}, inplace=True)

# Remove unused columns name is 'code_1' to 'code_6'
invoice_df = invoice_df.drop(['Purchaser Product Code', 'Supplier Product Code', 'code_1', 'code_2', 'code_3',\
                              'code_4', 'code_5', 'code_6'], axis=1)
# Todo: Adding column 'File Name' and 'Date Uplaod' to the right-end of Dataframe
# Adding File Name Column that has constant today date to existing pandas dataframe
upload_date = date.today()
invoice_df['Hotel_Group'] = hotel_group
invoice_df['File_Name'] = invoice_file_name
invoice_df['Date_Upload'] = upload_date
report_month_datetimeobject = datetime.strptime(invoice_yyyymm, '%Y%m')
report_month_newformat = report_month_datetimeobject.strftime('%Y-%m-01')
invoice_df['Report_Month'] = report_month_newformat
# Replace white space in column name of pandas dataframe to "_" (In order to make column name match with BigQuery Table)
invoice_df.columns = invoice_df.columns.str.replace(" ", "_")
pd_column_list = invoice_df.columns.values.tolist()
print(pd_column_list)
print(invoice_df)
print("Finish process Invoice Line Files")

# Todo: Join invoice_df with food_nonfood_category_df
invoice_leftjoin_food_nonfood_category_df = pd.merge(invoice_df, food_nonfood_category_df, how='left', left_on='Category',\
                         right_on='Category_Category')
print(invoice_leftjoin_food_nonfood_category_df)

# Export invoice_df to csv format for create BigQuery schema
# invoice_leftjoin_food_nonfood_category_df.to_csv('D:\\invoice_df.csv', index=False, header=True)
# sys.exit()

# =====================================# Connect to BigQuery schema and upload to BigQuery =============================

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:\\GCS_credential_json\\data-science-tan-0f186ed964c7.json"
project_id = 'data-science-tan'
dataset_name = "Purchaseplus_Dashboard"
table_name_invoice_line = "invoice_line"

# Instantiates a client
client_bq = bigquery.Client()
dataset_ref_purchaseplus_dashboard = client_bq.dataset(dataset_name)
table_ref_invoice_line = dataset_ref_purchaseplus_dashboard.table(table_name_invoice_line)

# Get table schema
table_invoice_line = client_bq.get_table(table_ref_invoice_line)
print(table_invoice_line.schema)

# Loop to create schema
table_invoice_line_schema = [{'name':schema.name, 'type':schema.field_type} for schema in table_invoice_line.schema]
print(table_invoice_line_schema)

# Big Query log
logger = logging.getLogger('pandas_gbq')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

# Todo: Writing code that append pandas dataframe into Google
print("Start loading invoice_df to BigQuery table")
invoice_line_table_bq = 'Purchaseplus_Dashboard.invoice_line'
start = time.time()
pandas_gbq.to_gbq(invoice_leftjoin_food_nonfood_category_df, invoice_line_table_bq, project_id=project_id, chunksize=4000,\
                  if_exists='append', table_schema=table_invoice_line_schema)
end = time.time()
print("time alternative 1 " + str(end - start))