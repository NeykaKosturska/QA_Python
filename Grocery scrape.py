import csv, logging, re, xlrd
from datetime import datetime
import pandas as pd
import openpyxl
import numpy as np

now = datetime.now()
logging.basicConfig(filename='qa_log_%s.log' % (now.strftime("%d%m%y")),level=logging.DEBUG,format='%(message)s')

#today = now.strftime("%Y-%m-%d")
today = "2023-06-16"

def get_summary_df(input_path):
    df = pd.read_csv(input_path,  dtype={11: str})
    total_rows = len(df)
    total_new_rows = len(df[df["Price Movement"].notna()])
    sku_in_stock = len(df[df["In Stock"] == 1])
    price_not_zero = len(df[(df["Price Movement"] != 0) & (df["Price Movement"].notna())])
    if total_new_rows != 0:
        percent_price_change = [price_not_zero / total_new_rows]
    else:
        percent_price_change = [0]
    return pd.DataFrame({"input_path": [input_path],
                         "total_rows": [total_rows],
                         "prices in both reports": [total_new_rows],
                         "percent_price_change": percent_price_change,
                         "percent_sku_in_stock": [sku_in_stock / total_rows]})

def check_date_collected(df, desired_date):
    if 'Date Collected' not in df.columns:
        logging.warning("The 'Date Collected' column does not exist in the DataFrame.")
        return False
    else:
        date_collected = df['Date Collected'].apply(pd.to_datetime, errors='coerce')
        if date_collected.isna().any():
            logging.warning("Some values in the 'Date Collected' column could not be converted to datetime.")
            return False
        else:
            date_collected_match = date_collected.dt.date == desired_date
            if date_collected_match.all():
                logging.warning("1_All values in the 'Date Collected' column match the desired date.")
                return True
            else:
                logging.warning("Not all values in the 'Date Collected' column match the desired date.")
                return False

desired_date = datetime.strptime(now.strftime('%Y-%m-%d'), '%Y-%m-%d').date()
#now.strftime('%Y-%m-%d')


def price_check(df, error_log):
    exceptions = pd.DataFrame()
    try:
        df[["Base Price", "Current Price"]] = df[["Base Price", "Current Price"]].apply(pd.to_numeric,errors='coerce').astype(float)
        if len(df[df["Current Price"] > df["Base Price"]]) > 0:
            exceptions = df.loc[df["Current Price"] > df["Base Price"], ['SKU', 'Retailer', 'Product URL', 'Base Price', 'Current Price']]
            exceptions['Base Price > Current Price'] = df["Current Price"] - df["Base Price"]
            logging.warning(exceptions)
    except Exception as e:
        logging.error('An error occurred while processing the file: %s' % e)
    if not exceptions.empty:
        with pd.ExcelWriter(error_log) as writer:
            exceptions.to_excel(writer, index=False, sheet_name='Sheet1')

def find_duplicates(df, input_path, competitor):
    df['SKU_Retailer'] = df['SKU'].astype(str) + df['Retailer'].astype(str)
    if df.duplicated(['SKU_Retailer']).any():
        duplicates = df[df.duplicated(['SKU_Retailer'], keep=False)]
        logging.warning('Duplicates found in %s' % input_path)
        duplicates.to_excel(f"{competitor}_duplicates.xlsx")
    else:
        logging.info('1_No duplicates found in %s' % input_path)


def check_fields(input_path, expected_fields):
        # Check if the field order is correct
        if list(df.columns) == expected_fields:
            logging.info('1_Field order is correct in %s' % input_path)
        else:
            logging.warning('Field order is incorrect in %s' % input_path)
            logging.warning('Expected fields: %s' % expected_fields)
            logging.warning('Actual fields: %s' % list(df.columns))

        return df

def check_non_blank(df, non_blank_tests):
    blank_entries = pd.DataFrame()
    for exceptions in non_blank_tests:
        try:
            if (df[exceptions] == "").any():
                logging.warning(f'Blank entry in {exceptions}')
                temp = df.loc[df[exceptions] == "", ['SKU', 'Retailer', exceptions]]
                temp.columns = ['SKU', 'Retailer', 'Test Column']
                temp['Test Name'] = exceptions
                blank_entries = blank_entries.append(temp)
        except KeyError:
            logging.warning(f'{exceptions} is absent from the file')
    if not blank_entries.empty:
        logging.warning('Blank entries found. Writing to Excel file...')
        blank_entries.to_excel(writer, index=False, sheet_name='Sheet1')
    else:
        logging.warning('1_No blank entries found.')



input_path_list = [
    #f"Grocery_Aldi_Full_Site_Scrape_{today}.csv",
    f"Grocery_Asda Groceries_Full_Site_Scrape_{today}.csv",
    f"Grocery_B&M_Full_Site_Scrape_{today}.csv",
    f"Grocery_Coop_Full_Site_Scrape_{today}.csv",
    f"Grocery_Iceland_Full_Site_Scrape_{today}.csv",
    f"Grocery_Lidl_Full_Site_Scrape_{today}.csv",
    f"Grocery_Morrisons_Full_Site_Scrape_{today}.csv",
    f"Grocery_Ocado_Full_Site_Scrape_{today}.csv",
    f"Grocery_Sainsburys_Full_Site_Scrape_{today}.csv",
    #f"Grocery_Tesco_Full_Site_Scrape_{today}.csv",
    f"Grocery_Waitrose_Full_Site_Scrape_{today}.csv"
]

expected_fields = ['Retailer', 'SKU', 'Category 1', 'Category 2', 'Category 3', 'Category 4', 'Category 5',
                        'Brand', 'Product Name', 'Product URL', 'Product Description', 'Barcode', 'Base Price',
                        'Current Price', 'In Stock', 'Stock Quantity', 'Start Date', 'End Date', 'Promotion Detail',
                        'Price Movement', 'Date Collected']



non_blank_tests = ['Retailer', 'SKU', 'Product Name', 'Product URL', 'Current Price', 'Date Collected']
numeric_tests = ["Base Price", "Current Price"]
field_list = []
DateTests = ['Start Date', 'End Date', 'Date Collected']
DateTimeTests = []

for input_path in input_path_list:
    with open(input_path, "r", encoding='utf-8') as file:
        df = pd.read_csv(file,  dtype={11: str})
        competitor = input_path.split('_')[1]
        logging.warning(competitor)

        price_check(df, 'error_log.xlsx')
        summary_df = pd.concat([get_summary_df(input_path) for input_path in input_path_list], ignore_index=True)
        summary_df.to_excel("summary.xlsx", index=False)
        df = check_fields(input_path, expected_fields)
        find_duplicates(df, 'input_file.csv', competitor)
        result = check_date_collected(df, desired_date)
        print("The result of check_date_collected is: %s" % result)
        check_non_blank(df, non_blank_tests)
