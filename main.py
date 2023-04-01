from hanaIO import *
from sqlIO import *
table_name = "CAL_ZRPP_D001"
date_col = "GSTRP"
start_date = "20200101"
end_date = "20231231"
hana = Hana()
colmuns = hana.get_hana_columns(table_name)
info = hana.get_hana_data(table_name, date_col, start_date, end_date)
