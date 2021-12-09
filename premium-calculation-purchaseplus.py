from google.cloud import bigquery
import pandas as pd
import os
import sys
import time
from datetime import date, datetime
import logging
import pandas_gbq

# Query Daily_Product_Price table from Bigquery to Pandas Data Frame
daily_pp_yyyymm = "202111"  # Input by user
invoice_yyyymm = daily_pp_yyyymm
hotel_group = "IHG"   # Input by user

start = time.time()
# ===================================== Connect to BigQuery ============================================================
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:\\GCS_credential_json\\data-science-tan-0f186ed964c7.json"
project_id = 'data-science-tan'
dataset_name = "Purchaseplus_Dashboard"

# ======================================================================================================================
# Todo: Read folder name in daily_pp_yyyymm to create purchaser_hotels_List
invoice_folder = "Invoice and PO line data"
invoice_file_name = "IHG invoice-"+invoice_yyyymm+".xlsx"

# Todo: check if this file name already uploaded to BigQuery?
invoice_file_path = "D:\\P+ Dashboard\\Data\\"+hotel_group+"\\"+invoice_folder+"\\"+invoice_yyyymm+"\\"+invoice_file_name
print(invoice_file_path)
invoice_df = pd.read_excel(invoice_file_path, engine='openpyxl')
duplicated_hotel_folder_list = invoice_df["Purchaser Name"].tolist() # read pandas column "Purchaser Name to list
hotel_folder_list = list(dict.fromkeys(duplicated_hotel_folder_list))  # Remove duplicated hotel name in list
print(hotel_folder_list)
print(len(hotel_folder_list))

# Todo: Why No 'Holiday Inn Resort Phuket Mai Khao Beach' in invoice line'
# hotel_folder_list = ['Holiday Inn Vana Nava Hua Hin (Branch 00002)', 'InterContinental Hua Hin Resort',\
                     # 'InterContinental Phuket Resort', 'Kebsup Group Co Ltd Branch 0002 (Hotel Indigo Phuket Patong)',\
                     # 'Kimpton Maa-Lai Bangkok Hotel', 'President Hotel & Tower Co Ltd',\
                     # 'SAYA (Thailand) Limited (Holiday Inn Bangkok Silom) TAX Id_ 0105545009921 Head Office']


for Hotel_Name in hotel_folder_list:
    Hotel_Name_Replaced = Hotel_Name.replace(":", "_")
    # Instantiates a client
    client_bq = bigquery.Client()
    # Big Query log
    logger = logging.getLogger('pandas_gbq')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    # ==================================================================================================================
    daily_product_price_dataset = "data-science-tan.Purchaseplus_Daily_Product_Price.daily_product_price_" + daily_pp_yyyymm
    daily_pp_datetimeobject = datetime.strptime(daily_pp_yyyymm, '%Y%m')
    report_month_format = daily_pp_datetimeobject.strftime('%Y-%m-01')
    print(report_month_format)
    query_daily_product_price = "SELECT * FROM %s WHERE Report_Month = '%s' AND Purchaser_Name = '%s' "\
                                % (daily_product_price_dataset, report_month_format, Hotel_Name)
    daily_product_price_query_job = client_bq.query(query_daily_product_price, project=project_id)
    daily_product_price_df = daily_product_price_query_job.result().to_dataframe()
    pd.set_option('display.max_columns', None)
    # print(daily_product_price_df)
    print(type(daily_product_price_df))
    time.sleep(5)

    # Query Invoice_Line table from Bigquery to Pandas Data Frame
    query_invoice_line = "SELECT * FROM `data-science-tan.Purchaseplus_Dashboard.invoice_line` \
                             WHERE Report_Month = '%s' AND Purchaser_Name = '%s' "\
                         % (report_month_format, Hotel_Name)
    invoice_line_query_job = client_bq.query(query_invoice_line, project=project_id)
    invoice_line_df = invoice_line_query_job.result().to_dataframe()
    pd.set_option('display.max_columns', None)
    print(invoice_line_df.shape[0])
    print(type(invoice_line_df))
    time.sleep(5)
    # ==================================================================================================================
    final_result_nested_list = []
    for index, row in invoice_line_df.iterrows():
        invoice_Line_row = [row['Document_Date'], row['Report_Month'], row['Document_Type'], row['Invoice_Number'],\
                            row['Purchase_Order_Number'], row['Purchaser_Name'], row['Supplier_Name'], row['State'],\
                            row['Brand'], row['Item_Description'],row['Sell_Unit'], row['Category'],\
                            row['Line_Total_Ex_Tax'], row['Department_Code'], row['Department_Name'], row['Product'],\
                            row['Quantity'], row['Unit_Price']]
        print('invoice_Line_row')
        print(invoice_Line_row)
        print(type(invoice_Line_row))
        match_daily_price_row_df = daily_product_price_df.loc[((daily_product_price_df['Date_Price'] == row['Document_Date']) \
                                   & (daily_product_price_df['Purchaser_Name'] == row['Purchaser_Name']) \
                                   & (daily_product_price_df['Product'] == row['Product'])), ('Date_Price', 'Product', \
                                     'Supplier_Name', 'Unit_Price')]
        print('match_daily_price_row_df')
        print(match_daily_price_row_df)
        min_price = match_daily_price_row_df['Unit_Price'].min()
        print('min_price')
        print(min_price)
        min_daily_price_row_output_df = match_daily_price_row_df.loc[daily_product_price_df['Unit_Price'] == min_price]
        print('min_daily_price_row_output_df')
        print(min_daily_price_row_output_df)
        print(type(min_daily_price_row_output_df))

        if min_daily_price_row_output_df.empty:
            req_date_offset_list = []
            print('DataFrame is empty!')
            print(row['Document_Date'])
            print(type(row['Document_Date']))
            initial_date = str(row['Document_Date'])
            req_date_1 = pd.to_datetime(initial_date) - pd.DateOffset(days=1)
            req_date_2 = pd.to_datetime(initial_date) - pd.DateOffset(days=2)
            req_date_3 = pd.to_datetime(initial_date) - pd.DateOffset(days=3)
            print('req_date')
            print(req_date_1)
            print(req_date_2)
            print(req_date_3)
            req_date_offset_list = [req_date_1, req_date_2, req_date_3]

            for req_date_offset_item in req_date_offset_list:
                print('req_date_offset_item')
                print(req_date_offset_item)
                match_daily_price_row_df_offset = daily_product_price_df.loc[(\
                     (daily_product_price_df['Date_Price'] == req_date_offset_item) \
                     & (daily_product_price_df['Purchaser_Name'] == row['Purchaser_Name']) \
                     & (daily_product_price_df['Product'] == row['Product'])), \
                    ('Date_Price', 'Product', 'Supplier_Name', 'Unit_Price')]

                print('match_daily_price_row_df_offset')
                print(match_daily_price_row_df_offset)
                min_price = match_daily_price_row_df_offset['Unit_Price'].min()
                print('min_price')
                print(min_price)
                min_daily_price_row_output_df_offset = match_daily_price_row_df_offset.loc[daily_product_price_df['Unit_Price'] == min_price]
                if min_daily_price_row_output_df_offset.empty:
                    print('-1 day still can not match, move to the next offset date')
                    min_daily_price_row_output_list_offset = [None, None, None, None]
                else:
                    print('-1 match!!!!!!!!!!!!!!!!!!')
                    min_daily_price_row_output_list_offset = min_daily_price_row_output_df_offset.values.tolist()[0]
                    print(min_daily_price_row_output_list_offset)
                    print('YESSSSSSSSSSSSSSSSSSSSSSSS min price found')
                    print('Break the loop')
                    break

            joined_output_result_list = invoice_Line_row + min_daily_price_row_output_list_offset
            print('joined_output_result_list')
            print(joined_output_result_list)

        else:
            print('in_daily_price_row_output_df is not empty')
            # Convert df to list
            min_daily_price_row_output_list = min_daily_price_row_output_df.values.tolist()[0]
            print('min_daily_price_row_output_list')
            print(min_daily_price_row_output_list)
            print(type(min_daily_price_row_output_list))
            joined_output_result_list = invoice_Line_row + min_daily_price_row_output_list
            print('joined_output_result_list')
            print(joined_output_result_list)

        final_result_nested_list.append(joined_output_result_list)
        print('final_result_nested_list')
        print(final_result_nested_list)

    premium_result_df = pd.DataFrame(final_result_nested_list,\
                                     columns=['Document_Date', 'Report_Month', 'Document_Type', 'Invoice_Number',\
                                     'Purchase_Order_Number', 'Purchaser_Name', 'Supplier_Name', 'State', 'Brand',\
                                     'Item_Description', 'Sell_Unit', 'Category', 'Line_Total_Ex_Tax',\
                                     'Department_Code','Department_Name', 'Product', 'Quantity', 'Unit_Price',\
                                     'Date_Price_Daily','Product_Daily', 'Supplier_Name_Daily', 'Min_Unit_Price_Daily'])

    # Calculate Premium of each invoice line compare to the lowest daily price of the product
    # on the same date from multiple suppliers
    def check_same_supplier(row_df):
        if row_df['Supplier_Name'] == row_df['Supplier_Name_Daily']:
            same_supplier = True
        else:
            same_supplier = False
        return same_supplier

    def premium_conditions(row_df):
        val_premium = row_df['Premium_Per_Unit']
        if row_df['State'] == 'cancel':
            val_premium = 0
        elif row_df['Category'] == 'Services':
            val_premium = 0
        elif row_df['Supplier_Name'] == 'Purchase Petty Cash' or row_df['Supplier_Name_Daily'] == 'Petty Cash – Purchasing':
            val_premium = 0
        elif row_df['Document_Type'] == 'Credit Note Line':
            val_premium = 0
        elif row_df['Check_Same_Supplier'] is True:
            val_premium = 0
        return val_premium

    premium_result_df['Date_Offset'] = premium_result_df['Document_Date'] - premium_result_df['Date_Price_Daily']
    premium_result_df['Premium_Per_Unit'] = premium_result_df['Unit_Price'] - premium_result_df['Min_Unit_Price_Daily']
    premium_result_df['Check_Same_Supplier'] = premium_result_df.apply(check_same_supplier, axis=1)
    premium_result_df['Premium_Per_Unit'] = premium_result_df.apply(premium_conditions, axis=1)
    premium_result_df['Premium_Total_Quantity'] = premium_result_df['Premium_Per_Unit'] * premium_result_df['Quantity']
    premium_result_df['Premium_Total_Quantity'] = premium_result_df['Premium_Total_Quantity'].fillna(0)
    premium_result_df['Premium_Total_Quantity_Adjusted'] = premium_result_df['Premium_Total_Quantity'].apply(lambda x: x \
                                                           if x >= 0 else 0)

    # Adding File Name Column that has constant today date to existing pandas dataframe
    Upload_Date = date.today()
    premium_result_df['File_Name'] = Hotel_Name_Replaced + "_Premium_Result_" + daily_pp_yyyymm + ".xlsx"
    premium_result_df['Date_Upload'] = Upload_Date
    print('premium_result_df')
    print(premium_result_df)
    end = time.time()
    print("time alternative 1 " + str(end - start))

    # export final_result_df to csv file in local drive D
    premium_result_file_name = Hotel_Name_Replaced + "_Premium_Result_" + daily_pp_yyyymm + ".xlsx"
    premium_result_file_path = 'D:\\P+ Dashboard\\Data\\Premium_Result\\'+daily_pp_yyyymm+'\\'+premium_result_file_name
    premium_result_df.to_excel(premium_result_file_path, index=False, header=True)
    time.sleep(5)





















