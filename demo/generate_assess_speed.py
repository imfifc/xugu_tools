# -*- coding: utf-8 -*-
import argparse
import multiprocessing
import sys
import time
import uuid
from datetime import datetime, timedelta
from multiprocessing import freeze_support
# from random import seed
from faker import Faker
import xgcondb


def get_cur(db_config):
    conn = xgcondb.connect(host=db_config['db_host'], port=db_config['db_port'], database=db_config['db_name'],
                           user=db_config['db_user'], password=db_config['db_pwd'], charset="utf8")
    cur = conn.cursor()
    return cur


def parse_args():
    parser = argparse.ArgumentParser(
        description='基于faker的造数工具，纯单个insert批量插入，插入速度最快。建议放在数据库所在的服务器，避免网络占满',
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


def drop_tb(table, db_config):
    cur = get_cur(db_config)
    sql = f"drop table if exists {table} cascade"
    cur.execute(sql)


def create_product_tb(table, db_config):
    cur = get_cur(db_config)
    sql = f"""
    create table {table}(
    product_no varchar(50) not null,
    product_name varchar(200),
    product_introduce varchar(4000),
    manufacture_date date,
    sell_date datetime,
    address varchar(200),
    product_type varchar(50)
    )PARTITION BY RANGE(sell_date)INTERVAL 1 DAY PARTITIONS(('1970-01-01 00:00:00')) ;
    """
    cur.execute(sql)


def show(table, db_config):
    cur = get_cur(db_config)
    sql = f'select count(*) from {table}'
    analyze_sql = f"EXEC dbms_stat.analyze_table('{db_config['db_user']}.{table}','all columns',1, null)"
    data = cur.execute(sql)
    row = cur.fetchone()
    print(f'{table} : {row}')
    cur.execute(analyze_sql)


def rebuild_tables(table, db_config):
    """
    重建表： 先删除后重建
    :return:
    """
    drop_tb(table, db_config)
    create_product_tb(table, db_config)


def generate_dates(n):
    current_date = datetime.now()
    date_list = [current_date + timedelta(days=i) for i in range(n)]
    date_strings = [date.strftime("%Y-%m-%d") for date in date_list]
    return date_strings


def insert_batch(table, date, nums, db_config):
    cur = get_cur(db_config)
    sql = f"insert into {table} values(?,?,?,?,?,?,?)"
    fake = Faker(locale='zh_CN')
    Faker.seed()  # 使用不同的种子
    # print(uuid.uuid4(),type(uuid.uuid4()))
    rows = []
    for i in range(nums):
        chinese_text = fake.text(max_nb_chars=100)
        data = (str(uuid.uuid4()), f'零食大礼包{i}', chinese_text, date, date, fake.city(), fake.city())
        rows.append(data)
    # print(len(rows), rows[:1])
    p0, p1, p2, p3, p4, p5, p6 = zip(*rows)
    cur.executebatch(sql, (p0, p1, p2, p3, p4, p5, p6))


# 多进程调用
def multi_process(table, dates, nums, db_config):
    processes = []
    for date in dates:
        process = multiprocessing.Process(target=insert_batch,
                                          args=(table, date, nums, db_config))
        processes.append(process)
    for process in processes:
        process.start()
    # 等待所有进程完成
    for process in processes:
        process.join()


def once_proc(table, db_config):
    nums = 25000
    days = 40
    dates = generate_dates(days)
    # print(dates)
    start = time.time()
    multi_process(table, dates, nums, db_config)
    end = time.time() - start
    show(table, db_config)
    print(f'耗时{end:.2f}秒', f'tps:{(nums * days / end):.2f} 行/s')
    return end


def get_table_size(table, db_config):
    sql = f"""
    select d.db_name,s.schema_name,t.table_name,count(*)*8 as cnt from sys_schemas s,sys_tables t,sys_gstores g,sys_databases d 
    where g.obj_id=t.table_id and s.schema_id=t.schema_id and s.db_id=t.db_id and g.db_id=d.db_id  and t.db_id=d.db_id and t.table_name='{table}'
    group by t.table_name,s.schema_name,d.db_name 
    """
    cur = get_cur(db_config)
    data = cur.execute(sql)
    row = cur.fetchone()
    if row:
        return row[3]
    return None


def get_speed():
    if sys.platform == 'win32':
        freeze_support()  # linux 不需要
    # db_host = '10.28.23.174'
    # db_port = 5138
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
    cur = get_cur(db_config)
    cur.execute('set max_loop_num to 0')
    cur.execute('set max_trans_modify to 0')

    table = 'procuct_666'
    rebuild_tables(table, db_config)
    time = once_proc(table, db_config)
    ## drop_tb('product555')
    size = get_table_size(table, db_config)  # MB
    speed = size / time
    print(speed)  # 普通迁移的速度
    drop_tb(table, db_config)
    return speed


if __name__ == '__main__':
    get_speed()

# 目的： 从临时表中取出1w数据到正式表
#  目的： 通过facker 快速insert batch 插入
