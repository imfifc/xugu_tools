import argparse
import csv
import os
import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import freeze_support
from pathlib import Path

import pymysql
from pymysql.cursors import DictCursor
from generate_assess_speed import get_speed

# 创建一个线程锁
lock = threading.Lock()
BASE_PATH = str(Path(__file__).parent)
timestands = time.strftime('%Y年%m月%d日%H%M%S', time.localtime())
dir = f'result_{timestands}'
EXCLUDE_DBS = ['information_schema', 'mysql', 'performance_schema', 'sys']


class ConnectionPool:
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
                result = cursor.fetchall()
                conn.commit()
                return result, tmp_sql
            except Exception as e:
                print(f'执行异常；{sql},{e}')
                conn.rollback()
                self.release_connection(conn)
                return None

        with self.get_connection() as conn:
            try:
                with conn.cursor(DictCursor) as cursor:
                    cursor.execute(data)
                    result = cursor.fetchall()
                    conn.commit()
                    return result
            except Exception as e:
                conn.rollback()
                print(f'执行异常；{data},{e}')
                self.release_connection(conn)
        return None


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
