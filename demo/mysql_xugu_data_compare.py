import csv
import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import freeze_support
from pathlib import Path

import pymysql
from pymysql.cursors import DictCursor

import xgcondb

# 创建一个线程锁
lock = threading.Lock()
BASE_PATH = str(Path(__file__).parent)
timestands = time.strftime('%Y年%m月%d日%H%M%S', time.localtime())
dir = f'result_{timestands}'
EXCLUDE_DBS = ['information_schema', 'mysql', 'performance_schema', 'sys']


class MysqlConnectionPool:
    def __init__(self, max_connections, connection_params):
        self.max_connections = max_connections
        self.connection_params = connection_params
        self._pool = queue.Queue(maxsize=max_connections)
        self._initialize_pool()

    def _initialize_pool(self):
        for _ in range(self.max_connections):
            connection = self._create_connection()
            self._pool.put(connection)

    def _create_connection(self):
        return pymysql.connect(**self.connection_params)

    def get_connection(self):
        try:
            return self._pool.get(block=False)
        except queue.Empty:
            raise Exception("Connection pool is empty. Try again later.")

    def release_connection(self, connection):
        self._pool.put(connection)

    def close_all_connections(self):
        while not self._pool.empty():
            connection = self._pool.get()
            connection.close()

    def executor(self, data):
        if isinstance(data, tuple):
            sql, tmp_sql = data
            conn = self.get_connection()
            cursor = conn.cursor(DictCursor)
            try:
                cursor.execute(sql)
            except Exception as e:
                conn.rollback()
                self.release_connection(conn)
                print(f'执行异常；{sql},{e}')
                return None
            result = cursor.fetchall()
            conn.commit()
            self.release_connection(conn)
            return result, tmp_sql

        conn = self.get_connection()
        cursor = conn.cursor(DictCursor)
        try:
            cursor.execute(data)
        except Exception as e:
            conn.rollback()
            self.release_connection(conn)
            print(f'执行异常；{data},{e}')
            return None
        rows = cursor.fetchall()
        conn.commit()
        self.release_connection(conn)
        return rows


class XuguConnectionPool:
    def __init__(self, max_connections, connection_params):
        self.max_connections = max_connections
        self.connection_params = connection_params
        self._pool = queue.Queue(maxsize=max_connections)
        self._initialize_pool()

    def _initialize_pool(self):
        for _ in range(self.max_connections):
            connection = self._create_connection()
            self._pool.put(connection)

    def _create_connection(self):
        try:
            return xgcondb.connect(**self.connection_params)
        except Exception as e:
            print('xugu 建立连接失败，重试一次', e)
            return xgcondb.connect(**self.connection_params)

    def get_connection(self):
        try:
            return self._pool.get(block=False)
        except queue.Empty:
            raise Exception("Connection pool is empty. Try again later.")

    def release_connection(self, connection):
        self._pool.put(connection)

    def close_all_connections(self):
        while not self._pool.empty():
            connection = self._pool.get()
            connection.close()

    def executor(self, data):
        if isinstance(data, tuple):
            sql, tmp_sql = data
            conn = self.get_connection()
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
            except Exception as e:
                print(f'执行异常；{sql},{e}')
                self.release_connection(conn)
                return None
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            rows = [dict(zip(column_names, row)) for row in rows]
            self.release_connection(conn)
            return rows, tmp_sql

        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(data)
        except Exception as e:
            conn.rollback()
            print(f'执行异常；{data},{e}')
            self.release_connection(conn)
            return None
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        conn.commit()
        self.release_connection(conn)
        return [dict(zip(column_names, row)) for row in rows]


def write_csv(filename, data):
    """
    :param filename:
    :param data: [{'table': 'test2', 'my_cnt': 5000000, 'xg_cnt': 5000000, 'is_equal': False}, ...]
    :return:
    """

    # filename = os.path.join(dir, filename)
    fieldnames = ['table', 'my_cnt', 'xg_cnt', 'is_equal']
    with open(filename, 'w', newline='', errors='ignore') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def get_databases():
    sql = 'show databases;'
    databases = pool.executor(sql)
    all_dbs = [i.get('Database') for i in databases]
    dbs = [i for i in all_dbs if i not in EXCLUDE_DBS]
    return dbs


def get_tables(db):
    sql = f"select table_name from information_schema.tables where  table_type !='VIEW' and table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys') and  table_schema='{db}'"
    data = pool.executor(sql)
    tables = [j for i in data for j in i.values()]
    return tables


def get_table_count(db, table):
    sql = f"select count(*) cnt from `{db}`.`{table}`"
    data = pool.executor(sql)
    return table, data[0].get('cnt')


def mysql_run(db, tables):
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(get_table_count, db, table) for table in tables]
        res = []
        for future in as_completed(futures):
            table_name, result = future.result()
            res.append({'table': table_name, 'my_cnt': result})
        data = sorted(res, key=lambda x: x['my_cnt'], reverse=True)
    return data


def xg_tables(schema_name):
    sql = f"select t.table_name from dba_tables t left join dba_schemas s on s.schema_id=t.schema_id where s.schema_name='{schema_name}'"
    data = xg_pool.executor(sql)
    tables = [i.get('TABLE_NAME') for i in data]
    # print(tables)
    return tables
    # print(data)


def xg_schemas():
    sql = 'select schema_name from dba_schemas'
    data = xg_pool.executor(sql)
    excludes = ['SYSSSO', 'SYSAUDITOR']
    schemas = [i.get('SCHEMA_NAME') for i in data if i.get('SCHEMA_NAME') not in excludes]
    return schemas


def get_xg_table_count(schema, table):
    sql = f'''select count(*) cnt from  "{schema}"."{table}" '''
    data = xg_pool.executor(sql)
    return table, data[0].get('CNT')


def xg_run(schema, tables):
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(get_xg_table_count, schema, table) for table in tables]
        res = []
        for future in as_completed(futures):
            table_name, result = future.result()
            res.append({'table': table_name, 'xg_cnt': result})
        data = sorted(res, key=lambda x: x['xg_cnt'], reverse=True)
    return data


def mysql_main(db):
    # db = 'zjm'
    tables = get_tables(db)
    print('mysql_tables: ', len(tables))
    start = time.time()
    res = mysql_run(db, tables)
    end = time.time() - start
    print(f'耗时{end:.2f}秒')
    return res


def xg_main(schema):
    # schema = 'ZJM'
    # sql = 'select schema_name from dba_schemas;'
    tables = xg_tables(schema)
    print('xugu_tables: ', len(tables))
    start = time.time()
    res = xg_run(schema, tables)
    end = time.time() - start
    print(f'耗时{end:.2f}秒')
    return res


def main():
    dbs = get_databases()
    print(f"\n当前mysql的库有{dbs}")
    db = input('请输入数据库: ')
    mysql_data = mysql_main(db)

    schemas = xg_schemas()
    print(f"\n当前xugu库: {xg_pool.connection_params.get('database')} 模式有{schemas}")
    schema = input('请输入schema: ')
    xg_data = xg_main(schema)
    my_dict = {i['table']: i for i in mysql_data}

    for i in xg_data:
        tb = i['table'].lower()
        if tb in my_dict:
            my_dict[tb].update(i)
        else:
            mysql_data.append(i)

    for i in mysql_data:
        if i.get('my_cnt') != i.get('xg_cnt'):
            i.update({
                'is_equal': False
            })

    # print(mysql_data)
    count = sum(1 for i in mysql_data if i.get('is_equal') is False)
    print(f"生成文件为: {schema}_{timestands}.csv; 数量有差异的表有 {count} 个 ")
    write_csv(f'{schema}_{timestands}.csv', mysql_data)


if __name__ == '__main__':
    if sys.platform == 'win32':
        freeze_support()  # linux 不需要
    host, port, user, password = input("请输入mysql ip,端口,用户,密码,以空格分开: ").split()
    pool = MysqlConnectionPool(
        max_connections=12,
        connection_params={
            "user": user,
            "password": password,
            "host": host,
            "port": int(port),
            # "database": db_name,
            "charset": 'utf8',
        },
    )
    xg_host, xg_port, xg_db, xg_user, xg_password = input("请输入xugu ip,端口,数据库,用户,密码,以空格分开: ").split()
    xg_pool = XuguConnectionPool(
        max_connections=6,
        connection_params={
            "user": xg_user.strip(),
            "password": xg_password.strip(),
            "host": xg_host.strip(),
            "port": int(xg_port),
            "database": xg_db.strip(),
            # "charset": 'utf8',
        },
    )
    # xg_schemas()
    while True:
        main()
        q = input('\nPress q to exit…or continue ')
        if q == 'q' or q == 'Q':
            break
    # input('\nPress Enter to exit…')

# 10.28.23.207 3306 root Admin@123

# 192.168.103.149  3306 xugu wsB#s4a6ANa0PMni
# 192.168.103.144 5138 pro_air SYSDBA SYSDBA
# 127.0.0.1 5138 SYSTEM SYSDBA SYSDBA
# 10.28.20.101 5136 system zjm 123456
# pyinstaller -c -F  --clean  --hidden-import=xgcondb   --hidden-import=pymysql data_compare.py
