import argparse
import csv
import os
import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import freeze_support
from pathlib import Path
import xgcondb

import pymysql
from pymysql.cursors import DictCursor
from generate_assess_speed import get_speed

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
            print(e)

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
    data: [
           (
            [{
                'Name': 'actionexecutelog',
                'Engine': 'InnoDB',
            }], "db--table--sql: ecology--actionexecutelog-- show table status from `ecology` like 'actionexecutelog';")]
    :param filename:
    :param data: [(  [{}],temp_sql), ([],temp_sql) ]
    :return:
    """

    filename = os.path.join(dir, filename)
    with open(filename, 'a+', newline='', errors='ignore') as f:
        writer1 = csv.DictWriter(f, fieldnames=[])
        write = csv.writer(f)
        for item in data:
            if item and item[0]:
                datas, temp_sql = item
                write.writerow([temp_sql])
                fieldnames = datas[0].keys()
                writer1.fieldnames = fieldnames
                writer1.writeheader()
                writer1.writerows(datas)


def get_databases():
    sql = 'show databases;'
    databases = pool.executor(sql)
    all_dbs = [i.get('Database') for i in databases]
    dbs = [i for i in all_dbs if i not in EXCLUDE_DBS]
    return dbs


def get_tables(db):
    sql = f"select table_name from information_schema.tables where  table_type !='VIEW' and table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys') and  table_schema='{db}'"
    data = pool.executor(sql)
    tables = [i.get('TABLE_NAME') for i in data]
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
    print(tables)
    return tables
    # print(data)


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


def mysql_main():
    db = 'zjm'
    dbs = get_databases()
    print(dbs)
    tables = get_tables(db)
    print('tables', len(tables))
    start = time.time()
    res = mysql_run(db, tables)
    print(res)
    end = time.time() - start
    print(f'耗时{end:.2f}秒')
    return res


def xg_main():
    schema = 'ZJM'
    tables = xg_tables(schema)
    print('tables', len(tables))
    start = time.time()
    res = xg_run(schema, tables)
    print(res)
    end = time.time() - start
    print(f'耗时{end:.2f}秒')
    return res


if __name__ == '__main__':
    if sys.platform == 'win32':
        freeze_support()  # linux 不需要
    host = '10.28.23.207'
    port = 3306
    user = 'root'
    password = 'Admin@123'
    db = 'SYSTEM'
    db_charset = 'utf8'
    # host, port, user, password = parse_args()
    pool = MysqlConnectionPool(
        max_connections=100,
        connection_params={
            "user": user,
            "password": password,
            "host": host,
            "port": port,
            # "database": db_name,
            "charset": 'utf8',
        },
    )

    xg_host = '10.28.20.101'
    xg_port = 5136
    xg_user = 'zjm'
    xg_password = '123456'
    xg_db = 'SYSTEM'
    xg_db_charset = 'utf8'
    # host, port, user, password, db = parse_args()
    xg_pool = XuguConnectionPool(
        max_connections=100,
        connection_params={
            "user": xg_user,
            "password": xg_password,
            "host": xg_host,
            "port": xg_port,
            # "db"
            "database": xg_db,
            # "charset": 'utf8',
        },
    )

    mysql_data = mysql_main()

    xg_data = xg_main()

    for i in mysql_data:
        for j in xg_data:
            if i.get('table') == j.get('table'):
                i.update({
                    'xg_cnt': j.get('xg_cnt')
                })

    for i in mysql_data:
        if i.get('my_cnt') != i.get('xg_cnt'):
            i.update({
                'diff': True
            })
        else:
            i.update({
                'diff': False
            })
    print(mysql_data)

    # os.path.exists(dir) or os.makedirs(dir)
    # print(dir)

    # print(res)
#     串行，0.21s
