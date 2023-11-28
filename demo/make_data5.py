# -*- coding: utf-8 -*-
import argparse
import multiprocessing
import queue
import sys
import time
import uuid
from datetime import datetime, timedelta
from multiprocessing import freeze_support
# from random import seed

from faker import Faker

import xgcondb




def get_cur(db_host, db_port, db_user, db_pwd, db_name):
    conn = xgcondb.connect(host=db_host, port=db_port, database=db_name, user=db_user, password=db_pwd, charset="utf8")
    cur = conn.cursor()
    return cur


# 存储过程中不能有注释
def create_random_package():
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    pkg_header = """
    CREATE OR REPLACE PACKAGE random is
      function value(min_value bigint ,max_value bigint) return bigint ;
      FUNCTION string(length IN NUMBER) RETURN varchar2;
      FUNCTION chinese_string(length IN NUMBER) RETURN varchar2;
    END;
    """
    pkg_body = """
    CREATE OR REPLACE PACKAGE BODY random as
        function value(min_value bigint, max_value bigint) return bigint as
            tmp_value  double;
            ret_value  bigint;
        begin
            return mod(rand(),max_value-min_value)+min_value;
        end;
    
        FUNCTION string(length IN NUMBER) RETURN VARCHAR2 IS
           characters VARCHAR2(62) := 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
           random_string VARCHAR2(32767) := '';
        BEGIN
           FOR i IN 1..length LOOP
              random_string := random_string || SUBSTR(characters,mod(rand(),61)+1, 1);
           END LOOP;
           RETURN random_string;
        end;
    
        FUNCTION chinese_string(length IN NUMBER) RETURN VARCHAR2 is
          random_string VARCHAR2 := '';
          characters VARCHAR2(58) := '啊阿埃挨哎唉哀皑癌蔼矮艾碍爱隘鞍氨安俺按暗岸胺案肮昂盎凹敖熬翱袄傲奥懊澳芭捌扒叭吧笆八疤巴拔跋靶把耙坝霸罢爸白柏百摆';
        BEGIN
           FOR i IN 1..length LOOP
              random_string := random_string || SUBSTR(characters, CEIL(value(1, 58)), 1);
           END LOOP;
           RETURN random_string;
        end;

    end random;
    """
    cur.execute(pkg_header)
    cur.execute(pkg_body)


def drop_tb(table):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = f"drop table if exists {table} cascade"
    cur.execute(sql)


def create_product_tb():
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = f"""
    create table product5(
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


def show(table):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = f'select count(*) from {table}'
    data = cur.execute(sql)
    row = cur.fetchone()
    print(f'{table} : {row}')


def rebuild_tables():
    """
    重建表： 先删除后重建
    :return:
    """
    tables = ['product5']
    tables = [f'{db_user}.{i}' for i in tables]
    for table in tables:
        drop_tb(table)

    create_product_tb()


def generate_dates(n):
    current_date = datetime.now()
    date_list = [current_date + timedelta(days=i) for i in range(n)]
    date_strings = [date.strftime("%Y-%m-%d") for date in date_list]
    return date_strings


def insert_many(date, nums, db_host, db_port, db_user, db_pwd, db_name):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = "insert into product5 values(?,?,?,?,?,?,?)"
    fake = Faker(locale='zh_CN')
    Faker.seed()  # 使用不同的种子
    # print(uuid.uuid4(),type(uuid.uuid4()))
    rows = []
    for i in range(nums):
        chinese_text = fake.text(max_nb_chars=100)
        data = (str(uuid.uuid4()), f'零食大礼包{i}', chinese_text, date, date, fake.city(), fake.city())
        rows.append(data)
    print(len(rows), rows[:1])
    cur.executemany(sql, tuple(rows))


# 多进程调用
def multi_process(dates, nums, db_host, db_port, db_user, db_pwd, db_name):
    processes = []
    for date in dates:
        process = multiprocessing.Process(target=insert_many,
                                          args=(date, nums, db_host, db_port, db_user, db_pwd, db_name))
        processes.append(process)
    for process in processes:
        process.start()
    # 等待所有进程完成
    for process in processes:
        process.join()


def once_proc():
    nums = int(input("请输入表行数: "))
    days = int(input("请输入天数: "))

    dates = generate_dates(days)
    print(dates)
    start = time.time()
    multi_process(dates, nums, db_host, db_port, db_user, db_pwd, db_name)
    end = time.time() - start
    show('product5')
    print(f'耗时{end:.2f}秒', f'tps:{(nums * days / end):.2f} 行/s')


if __name__ == '__main__':
    freeze_support()
    db_host = '10.28.20.101'
    db_port = 6326
    db_user = 'SYSDBA'
    db_pwd = 'SYSDBA'
    db_name = 'SYSTEM'
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    create_random_package()
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

# todo  每天500w, 加一个中文字段1-100随机字符，一个月数据
