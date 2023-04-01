import pyhdb


class Hana():
    def __init__(self):
        # 初始化 hana 数据库信息
        self.config = {
            'host': '',
            'port': '',
            'user': '',
            'password': ''
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
        # 这条并没有用，pyhdb好像没有 close() 的方法
        self.cur.close()
        self.conn.close()
        return

    def get_hana_columns(self,
                         schema_name:str,
                         table_name: str) -> list:
        '''
        功能为获取视图的列
        使用的是 VIEW_NAME 如果是表改为TABLE_NAME
        '''
        sql = f'''
            SELECT COLUMN_NAME
            FROM SYS.VIEW_COLUMNS
            WHERE SCHEMA_NAME = '{schema_name}'
            AND VIEW_NAME = '{table_name}'
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

    def get_hana_structure(self, schema_name,table_name):
        '''
        获取视图的列字段信息，依次为:备注、列名、数据类型、长度、小数点位数、能否为NULL、默认值
        根据POSITION排序，排序后的列名列表排序就是select结果的列名排序
        为mysql自动建表用
        '''
        sql = f'''
            SELECT 
            COMMENTS,COLUMN_NAME,DATA_TYPE_NAME,
            LENGTH,SCALE,IS_NULLABLE,DEFAULT_VALUE
            FROM SYS.VIEW_COLUMNS
            -- 这里输入数据库名
            WHERE SCHEMA_NAME = '{schema_name}'
            -- 这里输入视图名
            AND VIEW_NAME = '{table_name}'
            ORDER BY POSITION;
            '''
        try:
            self.get_conn()
            self.cur.execute(sql)
            res = self.cur.fetchall()
            # hana 数据库中数据类型名称可能是 NVARCHAR 改为VARCHAR 
            # 返回的结果是None就转为NULL
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
                      schema_name: str,
                      table_name: str,
                      date_col: str,
                      start_date: str, end_date: str) -> list:
        '''
        获取 hana 数据，要设定筛选列名称，一般采用日期列
        '''
        sql = f'''
            SELECT * FROM 
            "{schema_name}"."{table_name}"
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
