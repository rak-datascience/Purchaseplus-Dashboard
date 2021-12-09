from google.cloud import bigquery
import pandas as pd
import os
import sys
import time
from datetime import datetime, date
import logging
import pandas_gbq

# pip install openpyxl

daily_pp_yyyymm = "202111"  # Input by user
hotel_group = "IHG"   # Input by user

# ======================================================================================================================
# Todo: Read folder name in daily_pp_yyyymm to create purchaser_hotels_List
daily_pp_folder = "Product Price"
hotel_folder_path = "D:\\P+ Dashboard\\Data\\"+hotel_group+"\\"+daily_pp_folder+"\\"+daily_pp_yyyymm


# hotel_folder_list = ['InterContinental Phuket Resort']
# ['Amburaya Resort Co Ltd (Branch No 00001) Tax ID _ 0105545051910',
# 'Crowne Plaza Bangkok Lumpini Park',
# 'Holiday Inn Bangkok Sukhumvit',
# 'Holiday Inn Pattaya',
# 'Holiday Inn Resort Phuket',
# 'Holiday Inn Resort Phuket Mai Khao Beach',
# 'Holiday Inn Vana Nava Hua Hin (Branch 00002)',
# 'Holiday Inn & Suites Siracha Laemchabang',
# 'InterContinental Hua Hin Resort',
# 'InterContinental Phuket Resort',
# 'Kebsup Group Co Ltd Branch 0002 (Hotel Indigo Phuket Patong)',
# 'Kimpton Maa-Lai Bangkok Hotel',
# 'President Hotel & Tower Co Ltd',
# 'SAYA (Thailand) Limited (Holiday Inn Bangkok Silom) TAX Id_ 0105545009921 Head Office']

# ====================================================================================================================
# This will be list of 14 hotels within IHG group

# =====================================# Connect to BigQuery schema and upload to BigQuery ====================
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:\\GCS_credential_json\\data-science-tan-0f186ed964c7.json"
project_id = 'data-science-tan'
dataset_name = "Purchaseplus_Daily_Product_Price"
table_name_dpp = "daily_product_price_" + daily_pp_yyyymm


# Loop hotel_folder_list
# for purchaser_hotel in hotel_folder_list:
# Todo: Create For Loop to loop purchaser_hotels_List
daily_pp_folder_path = "D:\\P+ Dashboard\\Data\\"+hotel_group+"\\"+daily_pp_folder+"\\"+daily_pp_yyyymm
file_name_list = os.listdir(daily_pp_folder_path)
print(file_name_list)

# Process file name
daily_date_list = []
for file_name_str in file_name_list:
    # split string by _ and remove ".csv" out of string name (This approach work effectively with file name \
    # that use only single _ to split date in its file name
    first_part_str = file_name_str.split('_')[0]
    daily_date = file_name_str.split('_')[1].replace(".csv", "")
    daily_date_list.append(daily_date)
    # Sort list of file name by date
    daily_date_list.sort(key=lambda date: datetime.strptime(date, "%Y%m%d"))
    print(daily_date_list)

for daily_date_str in daily_date_list:
    combined_dd_file_name = "product" + "_" + daily_date_str + ".csv"   # (*Change)
    print(combined_dd_file_name)
    daily_price_product_file_path = daily_pp_folder_path+"\\"+combined_dd_file_name  # (*Change)
    # Todo: check if this file name already uploaded to BigQuery?
    daily_price_product_df = pd.read_csv(daily_price_product_file_path, engine='python')  # engine='python'
    print(daily_date_str)
    print(daily_price_product_df)

    # Todo: Adding column 'File Name' and 'Date Uplaod' to the right-end of Data frame
    # Adding File Name Column that has constant today date to existing pandas data frame
    upload_date = date.today()
    daily_price_product_df['File Name'] = combined_dd_file_name
    daily_price_product_df['Date Upload'] = upload_date
    report_month_datetimeobject = datetime.strptime(daily_pp_yyyymm, '%Y%m')
    report_month_newformat = report_month_datetimeobject.strftime('%Y-%m-01')
    daily_price_product_df['Report_Month'] = report_month_newformat

    # Todo: Adding column 'Date Price' and 'Purchaser Name' to the left-end of Data frame
    # Adding File Name Column that has constant today date to existing pandas data frame
    # Covert date formay from '%Y%m%d' to "%Y-%m-%d" and add to new column 'Date Price'
    new_dd_format = datetime.strptime(daily_date_str, '%Y%m%d').strftime("%Y-%m-%d")
    daily_price_product_df['Hotel_Group'] = hotel_group
    daily_price_product_df['Date Price'] = new_dd_format
    print(daily_price_product_df.columns)
    daily_price_product_df.columns = ['Catalogue Title', 'Brand', 'Description', 'Sell Unit',
       'Category Name', 'Subcategory Name', 'Unit Price', 'Tax Percentage',
       'Product code', 'Supplier Name', 'Product', 'File Name', 'Date Upload',
       'Report_Month', 'Hotel_Group', 'Date Price']
    print(daily_price_product_df.columns)
    daily_price_product_df['Purchaser Name'] = daily_price_product_df['Catalogue Title']  # (*Change)

    # Todo: Order column name in pandas dataframe
    pd_column_list = daily_price_product_df.columns.values.tolist()
    print(pd_column_list)
    columns_ordered = ['Date Price', 'Purchaser Name', 'Hotel_Group', 'Catalogue Title', 'Brand', 'Description',\
                       'Sell Unit', 'Category Name', 'Subcategory Name', 'Unit Price', 'Tax Percentage',\
                       'Product Code', 'Supplier Name', 'Product', 'File Name', 'Date Upload', 'Report_Month']

    daily_price_product_df = daily_price_product_df.reindex(columns=columns_ordered)

    # Replace white space in column name of pandas dataframe to "_"
    # (In order to make column name match with BigQuery Table)
    daily_price_product_df.columns = daily_price_product_df.columns.str.replace(" ", "_")
    pd_column_list = daily_price_product_df.columns.values.tolist()
    print(pd_column_list)
    print(daily_price_product_df)
    print("Finish process Daily Price Product Files")

    # Export invoice_df to csv format for create BigQuery schema
    # daily_price_product_df.to_csv('D:\\daily_price_product_df.csv', index=False, header=True)

    # ==================================Connect to BigQuery ========================================================
    # Instantiates a client
    client_bq = bigquery.Client()
    dataset_ref_purchaseplus_dashboard = client_bq.dataset(dataset_name)
    table_ref_dpp = dataset_ref_purchaseplus_dashboard.table(table_name_dpp)

    # Get table schema
    table_dpp = client_bq.get_table(table_ref_dpp)
    print(table_dpp.schema)

    # Loop to create schema
    table_dpp_schema = [{'name': schema.name, 'type': schema.field_type} for schema in
                        table_dpp.schema]
    print(table_dpp_schema)

    # Big Query log
    logger = logging.getLogger('pandas_gbq')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())
    # =============================================================================================================
    # Todo: Writing code that append pandas dataframe into Google
    print("Start loading daily_price_product_df to BigQuery table")
    print(combined_dd_file_name)
    dpp_table_bq = 'Purchaseplus_Daily_Product_Price.daily_product_price_' + daily_pp_yyyymm
    start = time.time()
    pandas_gbq.to_gbq(daily_price_product_df, dpp_table_bq, project_id=project_id, chunksize=4000, \
                      if_exists='append', table_schema=table_dpp_schema)
    end = time.time()
    print("time alternative 1 " + str(end - start))

