import argparse
import concurrent.futures
import multiprocessing
import random
import re
import sys
import os
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from multiprocessing import freeze_support

import xgcondb

"""
雷达数据 
数值预报
"""


# 连接字符串请视情况而定，输入自己本地的数据库服务器IP地址、端口等
# conn = xgcondb.connect(host="127.0.0.1", port="5138", database="SYSTEM", user="SYSDBA", password="SYSDBA")
# cur = conn.cursor()
# 大对象导入
# cur.execute("select count(*) from all_tables where table_name='TEST_LOB';")
# row = cur.fetchone()
# if row[0] == 1:
#     cur.execute("drop table test_lob;")
# cur.execute("create table test_lob(col1 int,  col2 blob);")

# blob_buf = open("./xg_lob/test_blob.jpg", "rb").read()
# cur.cleartype()
# cur.setinputtype((xgcondb.XG_C_INTEGER, xgcondb.XG_C_BLOB))
# cur.execute("insert into test_lob values(?,?);", (1, blob_buf))
# cur.execute("select len(col1),len(col2) from test_lob where col1=1;")
# row = cur.fetchone()
# # print(row)
# print("CLOB length is", row[0])
# print("BLOB length is", row[1])

# 大对象导出
# cur.execute("select * from test_lob;")
# row = cur.fetchone()
# blob_fd = open("./xg_lob/getBlob.jpg", "wb+")
# blob_fd.write(row[1])
# if len(blob_buf) == len(row[1]):
#     print("")
#     print("The large object was successfully exported with the same length of data")
def parse_args():
    parser = argparse.ArgumentParser(
        description='基于execute_many实现',
        prefix_chars='-'
    )
    # 添加位置参数
    # parser.add_argument('input_file', help='输入文件的路径')
    # 添加可选参数
    # parser.add_argument('-o', '--output', help='输出文件的路径')
    parser.add_argument('-H', '--host', help='输入数据库ip地址')
    parser.add_argument('-P', '--port', help='Port number 数据库端口', type=int, default=5138)
    parser.add_argument('-u', '--user', help='输入数据库 用户')
    parser.add_argument('-p', '--pwd', help='输入数据库密码')
    parser.add_argument('-d', '--database_name', help='输入数据库名')
    parser.add_argument('-f', '--file', help='输入blob文件绝对路径')
    # 添加标志参数
    parser.add_argument('-v', '--verbose', action='store_true', help='是否显示详细信息')
    args = parser.parse_args()
    # 访问解析后的参数
    # input_file = args.input_file
    # output_file = args.output
    host = args.host
    port = args.port
    user = args.user
    password = args.pwd
    db = args.database_name
    verbose = args.verbose
    path = args.file

    # 在这里可以根据解析的参数执行相应的操作
    if len(sys.argv) == 1:
        host = input("请输入ip: ")
        port = input("请输入端口: ")
        user = input("请输入用户: ")
        password = input("请输入密码: ")
        db = input("请输入数据库: ")
        path = input("输入blob文件绝对路径: ")
    if verbose:
        print("显示详细信息")
    if not host:
        parser.print_help()
        raise Exception('没有输入ip !!!\n')
    if not port:
        parser.print_help()
        raise Exception('没有输入port !!!\n')
    if not user:
        parser.print_help()
        raise Exception('没有输入user !!!\n')
    if not password:
        parser.print_help()
        raise Exception('没有输入password !!!\n')
    if not db:
        parser.print_help()
        raise Exception('没有输入数据库 !!!\n')
    if not path:
        parser.print_help()
        raise Exception('没有输入blob 文件路径 !!!\n')
    # if host and port and user and password and db:
    #     print(f'host: {host} port: {port} user: {user} password: {password} db: {db} \n')
    print(host, port, user, password, db, path)
    return host, port, user, password, db, path.strip()


def get_cur(db_config):
    conn = xgcondb.connect(host=db_config['db_host'], port=db_config['db_port'], database=db_config['db_name'],
                           user=db_config['db_user'], password=db_config['db_pwd'], charset="utf8")
    cur = conn.cursor()
    return cur


def rebuild_table(table, db_config):
    """"binary 最大只能装64k"""
    cur = get_cur(db_config)
    sql = f"drop table if exists {table} cascade"
    sql2 = f""" 
    CREATE TABLE {table} (
        X_ZHOU SMALLINT COMMENT 'X轴',
        Y_ZHOU SMALLINT COMMENT 'Y轴',
        XY SMALLINT COMMENT 'XY',
        val_data blob COMMENT '数值',
        high_level SMALLINT COMMENT '高度',
        val_time TIMESTAMP  COMMENT '资料时间',
        validtime TIMESTAMP COMMENT '预报时效',
        strat_time  TIMESTAMP COMMENT '起报时效'
        )PARTITION BY list(high_level) PARTITIONS 
        (PART1 values(1),
        PART2 values(100),
        PART3 values(200),
        PART4 values(300),
        PART5 values(400),
        PART6 values(500),
        PART7 values(600),
        PART8 values(700),
        PART9 values(800),
        PART10 values(900),
        PART11 values(1000),
        PART12 values(1100),
        PART13 values(1200),
        PART14 values(1300),
        PART15 values(1400),
        PART16 values(1500),
        PART17 values(1600),
        PART18 values(1700),
        PART19 values(1800),
        PART20 values(1900),
        PART21 values(2000)) hotspot 20 copy number 1 COMMENT '比湿数值预报表' """
    try:
        cur.execute(sql)
    except Exception as e:
        print(e)
    cur.execute(sql2)


def rebuild_radr_tab(table, db_config):
    cur = get_cur(db_config)
    sql = f"drop table if exists {table} cascade"
    sql2 = f"""
    CREATE TABLE {table} (
        AREA VARCHAR COMMENT '国家/区域',
        FILE_NAME VARCHAR COMMENT '文件名',
        SITE_ID VARCHAR  COMMENT '站点号',
        REP_TIME TIMESTAMP COMMENT '上报时间',
        FILE_TIME  TIMESTAMP COMMENT '资料时间',
        FILE_FMT VARCHAR COMMENT '文件格式',
        FILE_DATA BLOB COMMENT '流数据文件'
        )PARTITION BY RANGE(FILE_TIME) PARTITIONS
        (PART1 VALUES LESS THAN ('2023-12-01 23:59:59'),
        PART2 VALUES LESS THAN ('2023-12-02 23:59:59'),
        PART3 VALUES LESS THAN ('2023-12-03 23:59:59'),
        PART4 VALUES LESS THAN ('2023-12-04 23:59:59'),
        PART5 VALUES LESS THAN ('2023-12-05 23:59:59'),
        PART6 VALUES LESS THAN ('2023-12-06 23:59:59'),
        PART7 VALUES LESS THAN ('2023-12-07 23:59:59'),
        PART8 VALUES LESS THAN ('2023-12-08 23:59:59'),
        PART9 VALUES LESS THAN ('2023-12-09 23:59:59'),
        PART10 VALUES LESS THAN ('2023-12-10 23:59:59'),
        PART11 VALUES LESS THAN ('2023-12-11 23:59:59'),
        PART12 VALUES LESS THAN ('2023-12-12 23:59:59'),
        PART13 VALUES LESS THAN ('2023-12-13 23:59:59'),
        PART14 VALUES LESS THAN ('2023-12-14 23:59:59'),
        PART15 VALUES LESS THAN ('2023-12-15 23:59:59'),
        PART16 VALUES LESS THAN ('2023-12-16 23:59:59'),
        PART17 VALUES LESS THAN ('2023-12-17 23:59:59'),
        PART18 VALUES LESS THAN ('2023-12-18 23:59:59'),
        PART19 VALUES LESS THAN ('2023-12-19 23:59:59'),
        PART20 VALUES LESS THAN ('2023-12-20 23:59:59'),
        PART21 VALUES LESS THAN ('2023-12-21 23:59:59'),
        PART22 VALUES LESS THAN ('2023-12-22 23:59:59'),
        PART23 VALUES LESS THAN ('2023-12-23 23:59:59'),
        PART24 VALUES LESS THAN ('2023-12-24 23:59:59'),
        PART25 VALUES LESS THAN ('2023-12-25 23:59:59'),
        PART26 VALUES LESS THAN ('2023-12-26 23:59:59'),
        PART27 VALUES LESS THAN ('2023-12-27 23:59:59'),
        PART28 VALUES LESS THAN ('2023-12-28 23:59:59'),
        PART29 VALUES LESS THAN ('2023-12-29 23:59:59'),
        PART30 VALUES LESS THAN ('2023-12-30 23:59:59')) hotspot 20 copy number 2 COMMENT '雷达Z9745站点表'  ;
    """
    try:
        cur.execute(sql)
    except Exception as e:
        print(e)
    cur.execute(sql2)


def insert_batch(nums, table, db_config):
    cur = get_cur(db_config)
    sql = f"insert into {table} values(?,?)"
    blob_buf = open("./xg_lob/test_blob.jpg", "rb").read()
    cur.cleartype()
    cur.setinputtype((xgcondb.XG_C_INTEGER, xgcondb.XG_C_BLOB))
    rows = []
    for i in range(nums):
        data = (i, blob_buf)
        rows.append(data)
    print(len(rows))
    p0, p1 = zip(*rows)
    # print(p0)
    cur.executebatch(sql, (p0, p1))


def insert_many(time, path, table, db_config):
    cur = get_cur(db_config)
    sql = f"insert into {table} values(?,?,?,?,?,sysdate,?,?)"
    blob_buf = open(path, "rb").read()
    cur.cleartype()
    cur.setinputtype(
        (xgcondb.XG_C_SHORT, xgcondb.XG_C_SHORT, xgcondb.XG_C_SHORT, xgcondb.XG_C_BINARY, xgcondb.XG_C_SHORT,
         xgcondb.XG_C_DATETIME, xgcondb.XG_C_DATETIME, xgcondb.XG_C_DATETIME))
    high_levels = [1, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800,
                   1900, 2000]
    now = datetime.today()
    rounded_hour = now.replace(hour=8, minute=0, second=0, microsecond=0)
    rows = []
    for x in range(70, 140):
        for y in range(1, 61):
            for level in high_levels:
                data = (x, y, (61 - y) * 60 + x, blob_buf, level, time, rounded_hour)
                rows.append(data)
    cur.executemany(sql, tuple(rows))
    # batch_size = 100  # 根据需要调整批量大小
    # chunks = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     executor.submit(cur.executemany, [sql] * len(chunks), chunks)


def insert_many_radr(day, path, table, db_config):
    cur = get_cur(db_config)
    sql = f"insert into {table} values(?,?,?,sysdate,?,?,?)"
    file_name = os.path.basename(path)
    blob_buf = open(path, "rb").read()
    cur.cleartype()
    cur.setinputtype((xgcondb.XG_C_CHAR, xgcondb.XG_C_CHAR, xgcondb.XG_C_CHAR, xgcondb.XG_C_DATETIME,
                      xgcondb.XG_C_DATETIME, xgcondb.XG_C_CHAR, xgcondb.XG_C_BLOB))
    day = datetime.strptime(day, '%Y-%m-%d %H:%M:%S')
    print(day, type(day))
    min_datas = [str(day + timedelta(minutes=j * 6)) for j in range(240)]  # 240个6分钟，即24小时
    # print(time_datas)
    tab = table.split('_')
    site = tab[1] if len(tab) == 2 else table
    rows = []
    for i in min_datas:
        min_str = datetime.strptime(i, '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M%S')
        updated_file_name = re.sub(r'\d{5,}', min_str, file_name)
        data = ('Z', updated_file_name, site, i, 'bin.bz2', blob_buf)
        rows.append(data)
    cur.executemany(sql, tuple(rows))


def show(table, db_config):
    cur = get_cur(db_config)
    sql = f'select count(*) from {table}'
    data = cur.execute(sql)
    row = cur.fetchone()
    print(f'{table} : {row}')


def multi_process(path, table, db_config):
    now = datetime.today()
    rounded_hour = now.replace(hour=8, minute=0, second=0, microsecond=0)
    times = []
    for i in range(72):
        rounded_hour = rounded_hour + timedelta(hours=1)
        times.append(str(rounded_hour))

    processes = []
    for time_n in times:
        process = multiprocessing.Process(target=insert_many, args=(time_n, path, table, db_config))
        processes.append(process)
    for process in processes:
        process.start()
    # 等待所有进程完成
    for process in processes:
        process.join()


def multi_process_radr(path, table, db_config):
    now = datetime.today()
    rounded_hour = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    days_30 = [str(rounded_hour + timedelta(days=i)) for i in range(30)]

    processes = []
    for day in days_30:
        process = multiprocessing.Process(target=insert_many_radr, args=(day, path, table, db_config))
        processes.append(process)
    for process in processes:
        process.start()
    # 等待所有进程完成
    for process in processes:
        process.join()


def multi_process_radr2(num_processes, path, table, db_config):
    """仅用于linux"""
    now = datetime.today()
    rounded_hour = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    days_30 = [str(rounded_hour + timedelta(days=i)) for i in range(30)]

    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = [executor.submit(insert_many_radr, day, path, table, db_config) for day in days_30]
        for future in futures:
            future.result()


def once_proc(table, path, db_config):
    path2 = os.path.isfile(path)
    print(f"文件是否存在: {path2}")
    if path2:
        # nums = int(input("请输入表行数: "))
        # parallel_n = int(input("请输入并发数: "))
        start = time.time()
        multi_process(path, table, db_config)
        end = time.time() - start
        show(table, db_config)
        print(f'耗时{end:.2f}秒')


def once_proc_radr(table, path, db_config):
    path2 = os.path.isfile(path)
    print(f"文件是否存在: {path2}")
    if path2:
        # nums = int(input("请输入表行数: "))
        parallel_n = int(input("请输入并发数: "))
        start = time.time()
        multi_process_radr2(parallel_n, path, table, db_config)
        end = time.time() - start
        show(table, db_config)
        print(f'耗时{end:.2f}秒')


if __name__ == '__main__':
    if sys.platform == 'win32':
        freeze_support()  # linux 不需要
    # db_host = '10.28.20.101'
    # db_port = 5136
    # db_user = 'SYSDBA'
    # db_pwd = 'SYSDBA'
    # db_name = 'SYSTEM'
    db_host, db_port, db_user, db_pwd, db_name, path = parse_args()
    db_config = {
        'db_host': db_host,
        'db_port': db_port,
        'db_user': db_user,
        'db_pwd': db_pwd,
        'db_name': db_name,
    }
    select = input('请选择生成雷达数据1，还是数值预报2，默认2: ') or 2
    table = input('请输入表名(默认 test_blob )：') or 'test_blob'
    cur = get_cur(db_config)
    cur.execute('set max_loop_num to 0')
    cur.execute('set max_trans_modify to 0')

    if int(select) == 2:
        rebuild_table(table, db_config)
        once_proc(table, path, db_config)
    else:
        rebuild_radr_tab(table, db_config)
        once_proc_radr(table, path, db_config)
# D:\llearn\xugu\demo\xg_lob\test_blob.jpg
# D:\llearn\xugu\demo\xg_lob\test_blob.jpg
