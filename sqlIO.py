import pymysql
import re
from hanaIO import Hana


def comb_rows(row1, row2):
    if row2 == 'NULL':
        return row1
    else:
        return f"{row1},{row2}"


class SQLOperating:
    def __init__(self):
        # 数据库连接凭证初始化
        self.conn = None
        self.cursor = None
        self.config = {
            'host': '',
            'user': '',
            'password': '',
            'port': 3306,
            'db': ''
        }

    def get_conn(self):
        self.conn = pymysql.connect(**self.config)
        self.cursor = self.conn.cursor()

    def close_conn(self):
        self.cursor.close()
        self.conn.close()

    def create_db(self, table_name, hanadb_info):
        '''
        组合建表sql语句,数据表结构来源于 hanaIO文件下的 get_hana_structure
        '''

        # 小数点数据在建表时使用的是 decimal(3,3) 如果没有小数点是 decimal(3) list第4,5项代表 长度和小数点位数
        sql_list = [
            f'''
            `{row[1]}` {row[2]}({comb_rows(row[3],row[4])}) DEFAULT {row[6]} COMMENT '{row[0]}'
            ''' for row in hanadb_info
        ]
        # 比较粗糙的自动建表,数据类型只能是 varchar 和 decimal 
        # 如果需要日期/时间，需要在上面的sql_list初始化那部分数据为对应的类型
        sql = f'''
            CREATE TABLE `{table_name}` (
            {','.join(sql_list)}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 ROW_FORMAT=DYNAMIC COMMENT='自动建表:{table_name}';
            '''
        self.get_conn()
        try:
            self.cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            print('导入数据库失败:', e)
            self.conn.rollback()
        else:
            print(f"创建{table_name}表数据成功")
        finally:
            self.close_conn()

    def delte_data(self,
                  table_name: str,
                  date_col: str,
                  start_date: str, end_date: str):
        '''
        根据列，删除记录，这里用的是根据日期列
        '''
        del_sql = f'''
        delete from {table_name} 
        where 
        {date_col}>="{start_date}" and {date_col} <="{end_date}"
        '''
        try:
            self.get_conn()
            self.cursor.execute(del_sql)
        except Exception as e:
            print("删除数据失败,原因:",e)
        else:
            print("删除数据成功")

    def drop_table(self,table_name):
        drop_sql = f'''
        drop table {table_name}
        '''
        try:
            self.cursor.execute(drop_sql)
        except Exception as e:
            print(f"删除表{table_name}失败,原因:", e)
        else:
            print(f"删除表{table_name}成功")


    def update_db(self, 
                  table_name: str,
                  date_col: str,
                  start_date: str, end_date: str,
                  columns:list,
                  info:list):
        self.delte_data(table_name, date_col, start_date, end_date)
        self.insert_data(table_name, columns, info)


    def get_hana_col(self, table_name):
        hanadb = Hana()
        return hanadb.get_hana_columns(table_name)

    def insert_data(self,
                    table_name: str,
                    columns: list,
                    info: list):
        self.get_conn()
        sql = f"INSERT INTO `{self.config.get('db')}`.`{table_name}` ({','.join(columns)}) VALUES ({','.join(['%s']*len(columns))})"
        try:
            self.cursor.executemany(sql, info)
            self.conn.commit()
        except Exception as e:
            print('导入数据库失败:', e)
            self.conn.rollback()
        else:
            print(f"导入{table_name}表数据成功")
        finally:
            self.close_conn()

    def get_colmunname(self, table_name):
        self.get_conn()
        sql = f"SHOW COLUMNS FROM {table_name};"
        try:
            self.cursor.execute(sql)
            res = self.cursor.fetchall()
            col_list = [col[0] for col in res]
            type_list = [col[1] for col in res]
            new_type_list = [re.search(
                r'(\w+)\(', row).group(1) if re.search(r'(\w+)\(', row) else row for row in type_list]

        except Exception as e:
            print('获取列名失败:', e)
            self.conn.rollback()
        else:
            print(f"成功获取列名")
            print(col_list, new_type_list)
        finally:
            self.close_conn()
        pass
