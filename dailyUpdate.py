from hanaIO import *
from sqlIO import *
from datetime import datetime, timedelta

def update_ZRPP_D001():
    table_name = "CAL_ZRPP_D001"
    date_col = "GSTRP"
    start_date = "20200101"
    end_date = "20231231"
    hana = Hana()
    fwqsql = SQLOperating()
    fwqsql.drop_table(table_name)
    colmuns = hana.get_hana_columns(table_name)
    info = hana.get_hana_data(table_name, date_col, start_date, end_date)
    fwqsql.insert_data(table_name, colmuns, info)

# 获取昨天的日期
yesterday = datetime.now() - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y%m%d')

date_col = "CALDAY"
table_list = ["CAL_ZRMM_D001", "CAL_ZRMM_D002",
              "CAL_ZRMM_D003", "CAL_ZRMM_D006",
              "CAL_ZRMM_D007", "CAL_ZRSD_D001"]

for table_name in table_list:
    hana = Hana()
    fwqsql = SQLOperating()
    colmuns = hana.get_hana_columns(table_name)
    info = hana.get_hana_data(table_name, date_col,
                              yesterday_str, yesterday_str)
    fwqsql.update_db(table_name, date_col, yesterday_str,
                  yesterday_str, colmuns, info)
update_ZRPP_D001()