from hanaIO import *
from sqlIO import *
from datetime import datetime, timedelta


# 获取昨天的日期
yesterday = datetime.now() - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y%m%d')
schema_name = "schema_name"
date_col = "date_col"
table_list = ["table_name1", "table_name2",
              "table_name3", "table_name4",
              "table_name5", "table_name6"]


def create_table_from_hana(schema_name, table_name):
    hana = Hana()
    fwqsql = SQLOperating()
    hanadb_info = hana.get_hana_structure(schema_name, table_name)
    fwqsql.create_db(table_name, hanadb_info)

# 批量更新昨日数据到数据表
for table_name in table_list:
    hana = Hana()
    fwqsql = SQLOperating()
    colmuns = hana.get_hana_columns(schema_name, table_name)
    info = hana.get_hana_data(schema_name, table_name, date_col,
                              yesterday_str, yesterday_str)
    fwqsql.update_db(table_name, date_col, yesterday_str,
                     yesterday_str, colmuns, info)

