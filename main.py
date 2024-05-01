from sqlalchemy import create_engine
from crawler import table_latest_date, date_range, month_range, season_range, update_table, crawl_price, crawl_monthly_report, crawl_finance_statement_by_date
import datetime
import argparse
import json
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

def update_fundamental_data():
    parser = argparse.ArgumentParser(description="argparse for fundamental data update")
    parser.add_argument("-t", "--table", help="provide table name you want to update", dest="table", type=str,
                        required=True,
                        choices=["tw_stock_price_day_twse", "tw_stock_monthly_report_twse",
                                 "tw_stock_balance_sheet_twse"])

    my_sql_login_file = "./login/mysql_login.json"
    with open(my_sql_login_file) as json_file:
        db_config = json.load(json_file)
    conn = create_engine(
        f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['db']}")

    args = parser.parse_args()

    # crawling price
    if args.table in ["tw_stock_price_day_twse"]:
        '''
        df = crawl_price(datetime.datetime(2022, 11, 1))
        '''
        try:
            start_time = table_latest_date(conn, args.table) + datetime.timedelta(1)
        except:
            start_time = datetime.datetime(2005, 4, 1)
        end_time = datetime.datetime.now()
        date_rg = date_range(start_time, end_time)
        print(date_rg)
        update_table(conn, args.table, crawl_price, date_rg)

    # crawling monthly_revenue
    if args.table in ["tw_stock_monthly_report_twse"]:
        '''
        obtain monthly report "next month"
        df = crawl_monthly_report(datetime.datetime(2022,11,1))
        update day date 15 every month
        '''

        try:
            start_time = table_latest_date(conn, args.table) - datetime.timedelta(31)
        except:
            start_time = datetime.datetime(2002, 1, 1)
        end_time = datetime.datetime.now()
        monthly_rg = month_range(start_time, end_time)
        update_table(conn, args.table, crawl_monthly_report, monthly_rg)

    # crawl finance_statement
    if args.table in ["tw_stock_balance_sheet_twse"]:
        '''
        # Q1季報-5月15日前 (crawl date: 5/31)
        # Q2季報-8月14日前 (crawl date: 8/31)
        # Q3季報-11月14日前 (crawl date: 11/31)
        # 年度財務報告-次年3月31日前 (crawl date 4/15)
        '''

        try:
            start_time = table_latest_date(conn, args.table) + datetime.timedelta(31)
        except:
            start_time = datetime.datetime(2013, 4, 15)
        end_time = datetime.datetime.now()

        season_rg = season_range(start_time, end_time)
        update_table(conn, args.table, crawl_finance_statement_by_date, season_rg)


if __name__ == '__main__':
    update_fundamental_data()

