from hanaIO import *
from sqlIO import *
from datetime import datetime, timedelta

# 获取昨天的日期
yesterday = datetime.now() - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y%m%d')
startday = '20200101'
endday = '20200630'
date_col = "CALDAY"
table_name = "CAL_ZRSD_D001"
hana = Hana()
fwqsql = SQLOperating()
colmuns = hana.get_hana_columns(table_name)
info = hana.get_hana_data(table_name, date_col,
                          startday, endday)
fwqsql.update_db(table_name, date_col, startday,
                 endday, colmuns, info)
