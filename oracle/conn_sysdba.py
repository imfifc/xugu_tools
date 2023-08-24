import cx_Oracle

# 设置数据库连接参数
# user = 'sys'
# password = '123456'
# host = 'localhost:1521'
# service_name = 'XEPDB1'
# dsn = 'localhost:1521/XEPDB1'

# user = 'u1'
# password = '123456'
user = 'sys'
password = 'rootroot'
host = '192.168.2.217:1521'
service_name = 'ORCL'
dsn = '192.168.2.217:1521/ORCL'


def make_dict_factory(cursor):
    column_names = [d[0] for d in cursor.description]

    def create_row(*args):
        return dict(zip(column_names, args))

    return create_row


def use_privilege_conn(sql):
    with cx_Oracle.connect(user, password, dsn, mode=cx_Oracle.SYSDBA) as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            # cur.rowfactory = make_dict_factory(cur)
            columns = [col[0] for col in cur.description]
            cur.rowfactory = lambda *args: dict(zip(columns, args))
            data = cur.fetchall()
            print(data)
            # for i in data:
            #     print(i)
        except Exception as e:
            print("查询失败", e)
        conn.commit()


sql = "SELECT NAME, BYTES FROM V$SGAINFO"
sql2 = 'select * from "SCOTT"."SALGRADE"'
use_privilege_conn(sql2)


#  插入需要提交，失败进行回滚

def insertdata():
    with cx_Oracle.connect(user, password, dsn, mode=cx_Oracle.SYSDBA) as conn:
        cur = conn.cursor()
        sql = "insert into "
        # sql = 'select * from "SCOTT"."SALGRADE"'
        try:
            cur.execute(sql)
            conn.commit()
            data = cur.fetchall()
            for k, v in data:
                print(k)
        except Exception as e:
            conn.rollback()
            print("查询失败", e)
        cur.close()  # 关闭游标
