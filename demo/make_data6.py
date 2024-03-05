# -*- coding: utf-8 -*-
import argparse
import multiprocessing
import queue
import sys
import time
import uuid
import random
from datetime import datetime, timedelta
from multiprocessing import freeze_support
# from random import seed

from faker import Faker

import xgcondb


def get_cur(db_host, db_port, db_user, db_pwd, db_name):
    conn = xgcondb.connect(host=db_host, port=db_port, database=db_name, user=db_user, password=db_pwd, charset="utf8")
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


def drop_tb(table):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = f"drop table if exists {table} cascade"
    try:
        cur.execute(sql)
    except Exception as e:
        print('执行异常', sql)


def create_product_tb(table):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = f"""
        CREATE TABLE `{table}` (
          `id` bigint NOT NULL  COMMENT '主键',
          `key_id` varchar(128)  NOT NULL COMMENT '密钥主键',
          `app_id` bigint NOT NULL COMMENT '应用主键',
          `algorithm` varchar(12)  NOT NULL COMMENT '算法',
          `cert_sn` varchar(64)  DEFAULT NULL COMMENT '加密证书序列号',
          `key_usage` varchar(64)  DEFAULT NULL COMMENT '密钥用途',
          `key_name` varchar(64)  NOT NULL COMMENT '密钥名称',
          `key_path` varchar(100)  NOT NULL DEFAULT '' COMMENT '非对称密钥oss存储路径',
           `public_key` varchar(1000)  DEFAULT NULL COMMENT '非对称密钥公钥',
           `private_key` varchar(4500)  DEFAULT NULL COMMENT '非对称密钥私钥',
           `sym_key` varchar(200)  DEFAULT NULL COMMENT '对称密钥密文',
           `not_before` char(19)  NOT NULL COMMENT '密钥有效起始时间',
           `not_after` char(19)  DEFAULT NULL COMMENT '密钥有效截止时间',
           `status` tinyint NOT NULL DEFAULT '1' COMMENT '密钥状态 1：生效中 2：已过期',
           `create_by` varchar(64)  NOT NULL COMMENT '创建人',
           `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
           `update_by` varchar(64)  DEFAULT NULL COMMENT '修改人',
           `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '修改时间',
           `cur_version` int NOT NULL DEFAULT '0' COMMENT '版本号'
         )  comment '密钥在用库表';
    """
    cur.execute(sql)


def show(table):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = f'select count(*) from {table}'
    analyze_sql = f"EXEC dbms_stat.analyze_table('{db_user}.{table}','all columns',1, null)"
    data = cur.execute(sql)
    row = cur.fetchone()
    print(f'{table} : {row}')
    cur.execute(analyze_sql)


def rebuild_tables():
    """
    重建表： 先删除后重建
    :return:
    """
    tables = ['sys_key']
    tables = [f'{db_user}.{i}' for i in tables]
    for table in tables:
        drop_tb(table)

    create_product_tb('sys_key')


def generate_dates(n):
    current_date = datetime.now()
    date_list = [current_date + timedelta(days=i) for i in range(n)]
    date_strings = [date.strftime("%Y-%m-%d") for date in date_list]
    return date_strings


def insert_batch(nums, db_host, db_port, db_user, db_pwd, db_name):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = "insert into sys_key values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    # fake = Faker(locale='zh_CN')
    fake = Faker()
    Faker.seed()  # 使用不同的种子
    # print(uuid.uuid4(),type(uuid.uuid4()))
    rows = []
    for i in range(nums):
        # chinese_text = fake.text(max_nb_chars=100)
        data = (
            random.randint(1, 20),
            str(fake.uuid4()),
            1,
            random.choice(['RSA', 'AES', 'RSA2048', 'SM2']),
            str(fake.uuid4()),
            fake.word(),
            'CA',
            fake.file_path(),
            fake.text(max_nb_chars=100),
            fake.text(max_nb_chars=450),
            fake.password(length=20),
            fake.date_time_this_year().strftime('%Y-%m-%d %H:%M:%S'),
            fake.future_datetime().strftime('%Y-%m-%d %H:%M:%S'),
            random.choice([1, 2]),
            fake.name(),
            fake.date_time_this_decade().strftime('%Y-%m-%d %H:%M:%S'),
            fake.name(),
            fake.date_time_this_decade().strftime('%Y-%m-%d %H:%M:%S'),
            0
        )
        # data = (str(uuid.uuid4()), f'零食大礼包{i}', chinese_text, date, date, fake.city(), fake.city())
        rows.append(data)
    # print(len(rows), rows[:1])
    p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19 = zip(*rows)
    cur.executebatch(sql, (p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19))


# 多进程调用
def multi_process(paralell_n, nums, db_host, db_port, db_user, db_pwd, db_name):
    processes = []
    for _ in range(paralell_n):
        process = multiprocessing.Process(target=insert_batch,
                                          args=(nums, db_host, db_port, db_user, db_pwd, db_name))
        processes.append(process)
    for process in processes:
        process.start()
    # 等待所有进程完成
    for process in processes:
        process.join()


def once_proc():
    nums = int(input("请输入表行数: "))
    paralell_n = int(input("请输入并发数: "))

    # dates = generate_dates(paralell_n)
    # print(dates)
    start = time.time()
    multi_process(paralell_n, nums, db_host, db_port, db_user, db_pwd, db_name)
    end = time.time() - start
    if nums * paralell_n <= 1000000:
        show('sys_key')
    # show('sys_key') # 数据量太大不用
    print(f'耗时{end:.2f}秒', f'tps:{(nums * paralell_n / end):.2f} 行/s')


if __name__ == '__main__':
    if sys.platform == 'win32':
        freeze_support()  # linux 不需要
    # db_host = '127.0.0.1'
    # db_port = 5138
    # db_user = 'SYSDBA'
    # db_pwd = 'SYSDBA'
    # db_name = 'SYSTEM'
    db_host, db_port, db_user, db_pwd, db_name = parse_args()
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)

    cur.execute('set max_loop_num to 0')
    cur.execute('set max_trans_modify to 0')
    # 目的： 从临时表中取出1w数据到正式表
    rebuild_tables()
    while True:
        once_proc()
        flag = input("是否需要清除表重建，(默认不重建) 请输入Y/N: ")
        if flag == 'Y' or flag == 'y':
            rebuild_tables()
            print('已重建表')
        q = input('\nPress q to exit…or continue')
        if q == 'q' or q == 'Q':
            break

#  目的： 通过facker 快速insert batch 插入
