import argparse
import multiprocessing
import sys
import os
import time
from multiprocessing import freeze_support

import xgcondb


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

    # 在这里可以根据解析的参数执行相应的操作
    if len(sys.argv) == 1:
        host = input("请输入ip: ")
        port = input("请输入端口: ")
        user = input("请输入用户: ")
        password = input("请输入密码: ")
        db = input("请输入数据库: ")
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
    # if host and port and user and password and db:
    #     print(f'host: {host} port: {port} user: {user} password: {password} db: {db} \n')

    return host, port, user, password, db


def get_cur(db_config):
    conn = xgcondb.connect(host=db_config['db_host'], port=db_config['db_port'], database=db_config['db_name'],
                           user=db_config['db_user'], password=db_config['db_pwd'], charset="utf8")
    cur = conn.cursor()
    return cur


def rebuild_table(table, db_config):
    cur = get_cur(db_config)
    sql = f"drop table if exists {table} cascade"
    sql2 = f"create table {table}(id int,  name blob)"
    cur.execute(sql)
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


def insert_many(path, nums, table, db_config):
    cur = get_cur(db_config)
    sql = f"insert into {table} values(?,?)"
    # blob_buf = open("./xg_lob/test_blob.jpg", "rb").read()
    blob_buf = open(path, "rb").read()
    # blob_buf = open(path, "rb").read()
    cur.cleartype()
    cur.setinputtype((xgcondb.XG_C_INTEGER, xgcondb.XG_C_BLOB))
    rows = []
    for i in range(nums):
        data = (i, blob_buf)
        rows.append(data)
    # print(len(rows))
    cur.executemany(sql, tuple(rows))
    # print(cur.rowcount)


def show(table, db_config):
    cur = get_cur(db_config)
    sql = f'select count(*) from {table}'
    data = cur.execute(sql)
    row = cur.fetchone()
    print(f'{table} : {row}')


def multi_process(n, path, nums, table, db_config):
    processes = []
    for i in range(n):
        process = multiprocessing.Process(target=insert_many, args=(path, nums, table, db_config))
        processes.append(process)
    for process in processes:
        process.start()
    # 等待所有进程完成
    for process in processes:
        process.join()


def once_proc(table, db_config):
    path = input('请输入blob文件路径：')
    # print(filepath)
    # # file_path = r'C:\path\to\your\file.txt'
    # ff = f"r'{filepath}'"
    # print(ff)
    path2 = os.path.isfile(path)
    # print(path2)
    if path2:
        nums = int(input("请输入表行数: "))
        parallel_n = int(input("请输入并发数: "))
        start = time.time()
        multi_process(parallel_n, path, nums, table, db_config)
        end = time.time() - start
        show(table, db_config)
        print(f'耗时{end:.2f}秒', f'tps:{(nums * parallel_n / end):.2f} 行/s')


if __name__ == '__main__':
    if sys.platform == 'win32':
        freeze_support()  # linux 不需要
    # db_host = '10.28.20.101'
    # db_port = 5136
    # db_user = 'SYSDBA'
    # db_pwd = 'SYSDBA'
    # db_name = 'SYSTEM'
    db_host, db_port, db_user, db_pwd, db_name = parse_args()
    db_config = {
        'db_host': db_host,
        'db_port': db_port,
        'db_user': db_user,
        'db_pwd': db_pwd,
        'db_name': db_name,
    }
    table = 'test_lob3'
    cur = get_cur(db_config)
    cur.execute('set max_loop_num to 0')
    cur.execute('set max_trans_modify to 0')

    rebuild_table(table, db_config)
    once_proc(table, db_config)
# D:\llearn\xugu\demo\xg_lob\test_blob.jpg
# D:\llearn\xugu\demo\xg_lob\test_blob.jpg
