# -*- coding: utf-8 -*-
import argparse
import multiprocessing
import threading
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
    parser.add_argument('-m', '--mode', default=1, help='输入数据库名')
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
    mode = args.mode

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

    return host, port, user, password, db, mode


def drop_tb(table):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = f"drop table if exists {table} cascade"
    try:
        cur.execute(sql)
    except Exception as e:
        print('执行异常', sql)


def create_sys_key_tb(table):
    """
    sys_key
    :param table:
    :return:
    """
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = f"""
        CREATE TABLE {table} (
          ID bigint identity(1,1) Primary Key NOT NULL  COMMENT '主键',
          KEY_ID varchar(128)  NOT NULL COMMENT '密钥主键',
          APP_ID bigint NOT NULL COMMENT '应用主键',
          ALGORITHM varchar(12)  NOT NULL COMMENT '算法',
          CERT_SN varchar(64)  DEFAULT NULL COMMENT '加密证书序列号',
          KEY_USAGE varchar(64)  DEFAULT NULL COMMENT '密钥用途',
          KEY_NAME varchar(64)  NOT NULL COMMENT '密钥名称',
          KEY_PATH varchar(100)  NOT NULL DEFAULT '' COMMENT '非对称密钥oss存储路径',
          PUBLIC_KEY varchar(1000)  DEFAULT NULL COMMENT '非对称密钥公钥',
          PRIVATE_KEY varchar(4500)  DEFAULT NULL COMMENT '非对称密钥私钥',
          SYM_KEY varchar(200)  DEFAULT NULL COMMENT '对称密钥密文',
          NOT_BEFORE char(19)  NOT NULL COMMENT '密钥有效起始时间',
          NOT_AFTER char(19)  DEFAULT NULL COMMENT '密钥有效截止时间',
          STATUS tinyint NOT NULL DEFAULT '1' COMMENT '密钥状态 1：生效中 2：已过期',
          CREATE_BY varchar(64)  NOT NULL COMMENT '创建人',
          CREATE_TIME timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
          UPDATE_BY varchar(64)  DEFAULT NULL COMMENT '修改人',
          UPDATE_TIME timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP  COMMENT '修改时间',
          CUR_VERSION int NOT NULL DEFAULT '0' COMMENT '版本号'
         )  comment '密钥在用库表';
    """
    cur.execute(sql)


def create_sys_key_generate_tb(table):
    """
    sys_key_generate
    :param table:
    :return:
    """
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = f"""
        CREATE TABLE {table} (
            ID bigint identity(1,1) Primary Key NOT NULL COMMENT '主键',
            APP_ID bigint NOT NULL COMMENT '应用主键',
            ALGORITHM varchar(12)  NOT NULL COMMENT '算法',
            KEY_PATH varchar(100)  NOT NULL DEFAULT '' COMMENT '非对称密钥oss存储路径',
            PUBLIC_KEY varchar(1000)  DEFAULT NULL COMMENT '非对称密钥公钥',
            PRIVATE_KEY varchar(4500)  DEFAULT NULL COMMENT'非对称密钥私钥',
            SYM_KEY varchar(200)  DEFAULT NULL COMMENT '对称密钥密文',
            CREATE_BY varchar(64)  NOT NULL COMMENT '创建人',
            CREATE_TIME timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
            CUR_VERSION int NOT NULL DEFAULT '0' COMMENT '版本号'
            ) hotspot 12 comment '密钥备用库表';
    """
    cur.execute(sql)


def show(table):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = f'select count(*) from {table}'
    analyze_sql = f"EXEC dbms_stat.analyze_table('{db_user}.{table}','all columns',1, null)"
    data = cur.execute(sql)
    row = cur.fetchone()
    print(f'{table} : {row}')
    # cur.execute(analyze_sql)


def rebuild_tables():
    """
    重建表： 先删除后重建
    :return:
    """
    tables = ['sys_key', 'sys_key_generate']
    tables = [f'{db_user}.{i}' for i in tables]
    for table in tables:
        drop_tb(table)

    create_sys_key_tb('SYS_KEY')
    create_sys_key_generate_tb('SYS_KEY_GENERATE')


def generate_dates(n):
    current_date = datetime.now()
    date_list = [current_date + timedelta(days=i) for i in range(n)]
    date_strings = [date.strftime("%Y-%m-%d") for date in date_list]
    return date_strings


def insert_batch_bak(nums, db_host, db_port, db_user, db_pwd, db_name):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = "insert into SYS_KEY_GENERATE (app_id,algorithm,public_key,private_key,sym_key,create_by,create_time,cur_version) values(?,?,?,?,?,?,?,?)"
    # fake = Faker(locale='zh_CN')
    fake = Faker()
    Faker.seed()  # 使用不同的种子
    # print(uuid.uuid4(),type(uuid.uuid4()))
    rows = []
    for i in range(nums):
        # chinese_text = fake.text(max_nb_chars=100)
        data = (
            random.randint(1, 20),
            random.choice(['RSA', 'AES', 'RSA2048', 'SM2']),
            'CA',
            "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCX1UzmA3FpS4WYisR8JX5d/qBn0lyzbXzkITS06wN2RJ8QSKwzkkOCNcEmXU3QuyTviFlA9sjrwEHKrkoHqJ4FrMTh3qv5qKSLrmX0zJRF+F7rBJxLYkyUByGRoQ0SKVOeK+ePaUkcVIvSrZOvoxYasMTR69j+X6a2c+nE7LsnF5Hvw/lIQ7Gwz7Omk4F5v0XZKLRvlfj3aMUbzcRStEa4TgaJs+AVX8LfXT1zw+65dKQxJ1QX2ZU43kJMYS0AqLmlnDJsi7NmOTJA2JHys0MXh5i8iOz6IQW9QNhIZVqQA0srgWejNG9CJjUvyk3iUu1XLpe+pmYGaudnWegUPGP9AgMBAAECggEAAh6tQyzbIYA3bkjJs34GhKNwc+Kg/rRRoRHBnaEGXFla6yxIkzPAk4nSl4mH85kZS+rYbCrF5Vy6zmyehfrZsfSWjxy/w+2R1o1pyQAuNcfg82EOGZNWxF5RHqpj91PyYev1ALCUn7uidB+OR058mYTMSq4DSpHlTvjRU7mGZ2ZMVoFpe/gj3vEBH43nBVKdvUUtzMUAN1f0ZPRwkccZHjtnaCSQ0KLtGjjFy6QyVqJcJkmSlvY5hjEE1v4p2tP3WkKCNgSTSOngeljoNKtg7QH39+r6O7MeE2N5dDfw2ZhHofFvwbsvc0u0kfRxHCq56CD20BWXJIQb6XQY2090wQKBgQDKTznj48OtU59buFJo3dCcZ5IhPgq2pxA5rwwhVRXSk894DOZ1S8RfuX20DoXaW3mBA6nS5wMR5G+h1vfHKFJovgiEE31Kxlfp5fTuGrEfAdTR/3so9B+C4B0TFpbAP/J2UUgk7lMN2AKugj0yU0vj1BuxpA7Vch2HZ4b9GqGqsQKBgQDAIL/N8Q4w05I/tpISZBPa/y4yZ1FZKNlRQW/LN+gQuk+ng839DoFw0NrGjUDiDcSj+c70l0o03+QgUMJpNG9yIqZLcA7iMVI0bYbgrMIvKv3DsVFuOIxYeanUY39nzV8bS4QL0o2s0iyEQUW+x95ywBowgx4TfLrt+IwL4A6JDQKBgQCqb8xpfPzhDM34S3TB+/0/htHJR6dm8Z/tuNcTTccwvG3Qya1trAMoUfDgvEtwBhh65Ecx8oTXkgep5JyiIAotS8uI2ZJ4aFiwKJcL8Vi+sqBEumNbwS6652wXJLSkCjuOCjQpSM6Xnx/DPN2phBsvOQaLxZbpo1XVORa0LLd+QQKBgE+kg738AhTtbz+oD+89uxlaFK1iNM+P5l4PkScdmr5sbR6Uvav7+l5oxXfvEAllCbGrmUW8/U7jzK40rBJuYRBgGq1E23aaWWhQSswR8UEaQi1/8LCPEmaOsH7Ra/v8n0MyT5+S/ZW5XijI4f+O3MCWr4SSpcIQP3DsSimWDJB1AoGBAJyDt2maDdIgdYMXmbWO460mlmOGvJBFZ5l98KmMnqoSIk0DUBdO8Jw6QrVZMthCla4I5fZaX8Jb5XRGk0smLje3YH4541RLq7+xbZajSAYd4TQoVXBGl78w8PdM/yNvOryFunZXnUmHPo/yVf5uMX2J3/mNKlosJmk9TYtUN4Aj",
            fake.password(length=20),
            fake.name(),
            fake.date_time_this_decade().strftime('%Y-%m-%d %H:%M:%S'),
            0
        )
        # data = (str(uuid.uuid4()), f'零食大礼包{i}', chinese_text, date, date, fake.city(), fake.city())
        rows.append(data)
    # print(len(rows), rows[:1])
    p1, p2, p3, p4, p5, p6, p7, p8 = zip(*rows)
    cur.executebatch(sql, (p1, p2, p3, p4, p5, p6, p7, p8))


def trans(db_host, db_port, db_user, db_pwd, db_name):
    # 事务
    sql = "SELECT id,app_id,algorithm,public_key,private_key,sym_key,create_by, create_time,cur_version FROM sys_key_generate limit 1;"
    sql2 = "insert into sys_key (key_id,key_name,not_before,app_id,algorithm,public_key,private_key,sym_key,create_by,create_time,cur_version) values(1,'s','s',?,?,?,?,?,?,?,?)"
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    cur.execute(sql)
    rows = cur.fetchall()
    column_names = [desc[0] for desc in cur.description]
    rows = [dict(zip(column_names, row)) for row in rows]
    # print(rows[0], len(rows[0]))
    if rows and rows[0]:
        id = rows[0].get('ID')
        app_id = rows[0].get('APP_ID')
        algorithm = rows[0].get('ALGORITHM')
        public_key = rows[0].get('PUBLIC_KEY')
        private_key = rows[0].get('PRIVATE_KEY')
        sym_key = rows[0].get('SYM_KEY')
        create_by = rows[0].get('CREATE_BY')
        create_time = rows[0].get('CREATE_TIME')
        cur_version = rows[0].get('CUR_VERSION')
        cur.execute(sql2, (app_id, algorithm, public_key, private_key, sym_key, create_by, create_time, cur_version))
        sql3 = f" delete from sys_key_generate where id = {id};"
        cur.execute(sql3)


# 多进程调用
def multi_process_bak(paralell_n, nums, db_host, db_port, db_user, db_pwd, db_name):
    processes = []
    for _ in range(paralell_n):
        process = multiprocessing.Process(target=insert_batch_bak,
                                          args=(nums, db_host, db_port, db_user, db_pwd, db_name))
        processes.append(process)
    for process in processes:
        process.start()
    # 等待所有进程完成
    for process in processes:
        process.join()


def multi_process_del(paralell_n, db_host, db_port, db_user, db_pwd, db_name):
    processes = []
    for _ in range(paralell_n):
        process = multiprocessing.Process(target=trans,
                                          args=(db_host, db_port, db_user, db_pwd, db_name))
        processes.append(process)
    for process in processes:
        process.start()
    # 等待所有进程完成
    for process in processes:
        process.join()


def once_del():
    paralell_n = int(input("请输入并发数: "))
    start = time.time()
    multi_process_del(paralell_n, db_host, db_port, db_user, db_pwd, db_name)
    end = time.time() - start
    show('sys_key')
    print(f'耗时{end:.2f}秒', f'tps:{(paralell_n / end):.2f} 行/s')


def once_proc():
    nums = int(input("请输入表行数: "))
    paralell_n = int(input("请输入并发数: "))

    # dates = generate_dates(paralell_n)
    # print(dates)
    start = time.time()
    multi_process_bak(paralell_n, nums, db_host, db_port, db_user, db_pwd, db_name)
    end = time.time() - start
    # if nums * paralell_n <= 10000:
    #     show('sys_key')
    show('sys_key_generate')  # 数据量太大不用
    print(f'耗时{end:.2f}秒', f'tps:{(nums * paralell_n / end):.2f} 行/s')


if __name__ == '__main__':
    if sys.platform == 'win32':
        freeze_support()  # linux 不需要
    # db_host = '127.0.0.1'
    # db_port = 5138
    # db_user = 'SYSDBA'
    # db_pwd = 'SYSDBA'
    # db_name = 'SYSTEM'
    # mode = 1
    db_host, db_port, db_user, db_pwd, db_name, mode = parse_args()
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)

    cur.execute('set max_loop_num to 0')
    cur.execute('set max_trans_modify to 0')
    cur.execute('set session_per_user to 0')
    if int(mode) == 2:
        once_del()
    # 目的： 从临时表中取出1w数据到正式表
    else:
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
