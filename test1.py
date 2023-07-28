import pymysql
from pymysql.cursors import DictCursor

# db_host = '192.168.2.212'
# db_user = 'ecology'
# db_pwd = 'Weaver@2023'
# db_port = 3306

db_host = '127.0.0.1'
db_user = 'root'
db_pwd = '123456'
db_port = 3306

connection = pymysql.connect(host=db_host, user=db_user, password=db_pwd, port=db_port, charset='utf8')


def executor(sql):
    # if self._conn is None:
    #     self.connect_db()
    # try:
    #     cur = self.cursor()
    #     cur.execute(sql)
    #     cur.close()
    #     self._conn.commit()
    #     return 200, "OK"
    # except Exception as err:
    #     self._conn.rollback()
    #     return 500, err
    try:
        if connection is None:
            return
        cursor = connection.cursor(DictCursor)
        cursor.execute(sql)
        result = cursor.fetchall()
        print(result)
        return result
    except pymysql.MySQLError as e:
        print(200, e)
    finally:
        connection.close()


def get_table_space():
    # 数据库表空间模式（on：独立表空间模式；off：系统表空间模式
    sql = "show variables like 'innodb_file_per_table';"
    executor(sql)


def get_db_and_charset():
    # 查看数据库及数据库的字符集'
    sql = 'show databases;'
    # sql = r'show create database ecology \G;'
    databases = executor(sql)
    datas = [i.get('Database') for i in databases]
    for db in datas:
        sql = f'show create database {db} ;'
        # sql2 = f'show tables from {db};'
        executor(sql)


get_db_and_charset()
