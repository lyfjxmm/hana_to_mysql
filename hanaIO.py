import pyhdb


class Hana():
    def __init__(self):
        self.config = {
            'host': '192.168.0.68',
            'port': '30047',
            'user': 'SCYYBFWQ',
            'password': 'Scyyb123'
        }
        self.conn = None
        self.cur = None

    def get_conn(self):
        self.conn = pyhdb.connect(**self.config)
        self.cur = self.conn.cursor()
        return self.cur

    def get_db(self, sql):
        self.cur.execute(sql)
        results = self.cur.fetchall()
        return results

    def close_conn(self):
        self.cur.close()
        self.conn.close()
        return

    def get_hana_columns(self,
                         table_name: str) -> list:
        sql = f'''
            SELECT COLUMN_NAME
            FROM SYS.VIEW_COLUMNS
            WHERE SCHEMA_NAME = '_SYS_BIC'
            AND VIEW_NAME = 'ZPBI/{table_name}'
            ORDER BY POSITION
            '''
        try:
            self.get_conn()
            self.cur.execute(sql)
            res = self.cur.fetchall()
        except Exception as e:
            print('获取列名失败:', e)
            self.conn.rollback()
        else:
            print(f'获取{table_name}列名成功')
            return [col[0] for col in res]

    def get_hana_structure(self, table_name):
        sql = f'''
            SELECT 
            COMMENTS,COLUMN_NAME,DATA_TYPE_NAME,
            LENGTH,SCALE,IS_NULLABLE,DEFAULT_VALUE
            FROM SYS.VIEW_COLUMNS
            -- 这里输入数据库名
            WHERE SCHEMA_NAME = '_SYS_BIC'
            -- 这里输入视图名
            AND VIEW_NAME = 'ZPBI/{table_name}'
            ORDER BY POSITION;
            '''
        try:
            self.get_conn()
            self.cur.execute(sql)
            res = self.cur.fetchall()
            newres = [(t[0], t[1], 'VARCHAR', t[3], t[4], t[5], t[6])
                      if t[2] == 'NVARCHAR' else t for t in res]
            res = [(t[0], t[1], t[2], t[3], "NULL" if t[4] is None else t[4],
                    t[5], "NULL" if t[6] is None else t[6]) for t in newres]
        except Exception as e:
            print('获取结构失败:', e)
        else:
            print(f'获取{table_name}列名成功')
            return res

    def get_hana_data(self,
                      table_name: str,
                      date_col: str,
                      start_date: str, end_date: str) -> list:
        sql = f'''
            SELECT * FROM 
            "_SYS_BIC"."ZPBI/{table_name}"
            where {date_col} >= '{start_date}' 
            AND {date_col} <= '{end_date}'
            '''
        try:
            self.get_conn()
            info = self.get_db(sql)
        except Exception as e:
            print(f"hana数据库数据获取失败，原因:{e}")
        else:
            print(f'已获取hana数据库的{table_name}数据')
            return info
