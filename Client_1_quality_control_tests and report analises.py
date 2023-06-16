#from itertools import count
import logging
import datetime
import pandas as pd
import numpy as np

pd.options.mode.chained_assignment = None

competitor_list = [
    "competitor_1",
    "competitor_2",
    "competitor_3"
]

def pivot_table(df, index, values, aggfunc='mean'):
    return df.pivot_table(values=values, index=index, aggfunc=aggfunc)


def calculate_percent_sku_in_stock(df_new, comp_column):
    stock_df = pd.DataFrame(columns=["competitor", "total_rows", "percent_sku_in_stock"])
    for retailer in df_new[comp_column].unique():
        df = df_new[df_new[comp_column] == retailer]
        total_rows = len(df)
        sku_in_stock = len(df[df["Stock Status"] == 1])
        percent_sku_in_stock = sku_in_stock / total_rows
        row = {"competitor": retailer, "total_rows": total_rows, "percent_sku_in_stock": percent_sku_in_stock}
        stock_df = pd.concat([stock_df, pd.DataFrame(row, index=[0])], ignore_index=True)

    stock_df = stock_df.sort_values(by=["competitor"], ascending=True)
    return stock_df

def get_price_changes(cur_r, previous_r, id_columns, price_field):
    df_total = previous_r.merge(cur_r, indicator=True, how="inner", on=id_columns)
    df_total["old_price"] = df_total[f"{price_field}_x"]
    price_changes = df_total[df_total["_merge"] != "left_only"].filter(regex=".*(?<!_x)$", axis="columns")
    price_changes[["old_price", f"{price_field}_y"]] = price_changes[["old_price", f"{price_field}_y"]].apply(
        pd.to_numeric)
    price_changes["price_delta_pct"] = abs(price_changes[f"{price_field}_y"] - price_changes["old_price"]) \
                                       / price_changes["old_price"] * 100
    price_changes = price_changes.sort_values(by=["_merge", "price_delta_pct"], ascending=[True, False])

    # create a new Excel file to store the results
    #writer = pd.ExcelWriter('price_change_pct.xlsx', engine='xlsxwriter')
    #price_changes.to_excel(writer, sheet_name='Price Changes', index=False)

    # create a new sheet with the list of competitors and their price change status
    competitors = price_changes.groupby('Competitor_y').apply(
        lambda x: f"{round((x['price_delta_pct'] != 0).sum() / len(x) * 100, 2)}%" if len(
            x) > 0 else 'Price not changed'
    )
    competitors = pd.DataFrame(competitors, columns=['Price Change Status'])
    competitors.index.name = 'Competitor ID'
    #competitors.to_excel(writer, sheet_name='Competitor Status', index=True)

    #writer.save()

    return price_changes, competitors

# set dates
time_format = "%Y-%m-%d"
today = datetime.datetime.now()
current_date = today.strftime(time_format)
# sets previous date to friday if today is monday
if today.weekday() == 0:
    previous_date = (today - datetime.timedelta(3)).strftime(time_format)
else:
    previous_date = (today - datetime.timedelta(1)).strftime(time_format)
# set dates manually
#current_date = "2023-06-08"
#previous_date = "2023-06-07"

logging.basicConfig(filename=f"Client_log_{current_date}.txt", level=logging.DEBUG,format='%(asctime)s %(message)s', datefmt='%H:%M:%S')

# skiprows removes the header add names of old and new report in the quotes
df_old = pd.DataFrame(data = pd.read_excel(f"Client Report {previous_date}.xlsx", skiprows=[0]))
df_new = pd.DataFrame(data = pd.read_excel(f"Client Report {current_date}.xlsx", skiprows=[0]))

error_table = pd.DataFrame(columns=df_new.columns)
error_table["Error"] = ""
error_table["Error_Field"] = ""

numeric_tests = ['RRP','Sell Price'] #Nonblank entries must contain a number.
non_blank_tests = {"SKU": [],"Competitor": [],"URL": [] ,"Country": [],"Match ID": [],"Match Type": []} #Entry must not be blank
regex_tests = {}

match_id = "Match ID"
competitor_column = "Competitor"
price_column = "RRP"

# drops and new matches
df_merged = pd.merge(df_old, df_new.drop_duplicates(), on=['Match ID'], how='outer', indicator=True)
drops = df_merged.query("_merge == 'left_only'")
new_matches = df_merged.query("_merge == 'right_only'")
aver_RRP_competitor = pivot_table(df_new, index='Competitor', values='RRP')

# % of Price Changes and % of Products In Stock
pc_df, cmp_df= get_price_changes(df_new, df_old, match_id, price_column)
stock_df = calculate_percent_sku_in_stock(df_new, competitor_column)

# Total and single matches count
results = pd.DataFrame({'Total Results': [len(df_new)], 'Single Matches': [len(df_new["SKU"].unique())]})

# data tests
for column, exceptions in non_blank_tests.items():
    add_error_table = df_new[(df_new[column] == "") & (~df_new[competitor_column].isin(exceptions))]
    if len(add_error_table) != 0:
        error_table = error_table.append(add_error_table.assign(Error="Blank", Error_Field=column))

for column in numeric_tests:
    add_error_table = df_new[df_new[column] != ""].loc[pd.to_numeric(df_new[column], errors='coerce').isnull()]
    if len(add_error_table) != 0:
        error_table = error_table.append(add_error_table.assign(Error="Non-numeric", Error_Field=column))

for column, expression in regex_tests.items():
    add_error_table = df_new[~df_new[column].str.match(expression)]
    add_error_table = add_error_table[add_error_table[column] != ""]
    if len(add_error_table) != 0:
        add_error_table["Error"] = "Incorrect format"
        add_error_table["Error_Field"] = column
        error_table = error_table.append(add_error_table)

report_competitor_list = list(df_new['Competitor'].drop_duplicates().sort_values())
report_competitor_list = list(dict.fromkeys(report_competitor_list))
if report_competitor_list != competitor_list:
    missing_comps = list(set(competitor_list) - set(report_competitor_list))
    extra_comps = list(set(report_competitor_list) - set(competitor_list))
    logging.debug("Competitor list mismatch \n missing {} \n extra {}".format(missing_comps, extra_comps))

error_table.to_csv(f"errors.csv", encoding="utf8", index=False)
error_table["error_code"] = error_table[competitor_column] + " " + error_table["Error"] + " " + error_table["Error_Field"]
for issue in error_table["error_code"].unique():
    print(issue, len(error_table[error_table["error_code"] == issue]))
    logging.info("{} {}".format(issue, len(error_table[error_table["error_code"] == issue])))

drops.to_excel(f"drops_{current_date}.xlsx")

with pd.ExcelWriter(f"QA_Data_{current_date}.xlsx") as writer:
    df_new.to_excel(writer, sheet_name='Original_Data')
    new_matches.to_excel(writer, sheet_name='New_matches')
    pc_df.to_excel(writer, sheet_name='Price change', index=False)
    results.to_excel(writer, sheet_name='Collection Summary', index=False)
    aver_RRP_competitor.to_excel(writer, sheet_name='RRP Avarage')
    cmp_df.to_excel(writer, sheet_name='% of Price Changes')
    stock_df.to_excel(writer, sheet_name="% of Products In Stock", index=False)

logging.info('\n')
