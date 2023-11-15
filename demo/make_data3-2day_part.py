# -*- coding: utf-8 -*-
import argparse
import multiprocessing
import sys
import time
from datetime import datetime, timedelta
from multiprocessing import freeze_support

import xgcondb


def get_cur(db_host, db_port, db_user, db_pwd, db_name):
    conn = xgcondb.connect(host=db_host, port=db_port, database=db_name, user=db_user, password=db_pwd, charset="utf8")
    cur = conn.cursor()
    return cur


# 参数解析前置，多进程才能不报错
def parse_args():
    parser = argparse.ArgumentParser(
        # description='这是一个数据库环境采集工具',
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
    create table sysdba.product(
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


def create_temp_proc(num):
    """
     num: 往临时表单次的插入数据
    :return:
    """
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = f"""
    create or replace procedure pr_test_insert(insert_num in int) is 
    declare
    TYPE t_var IS varray(30) OF VARCHAR;
    city_var t_var;                            
    add_num int;
    case_num int;
    begin 
    add_num:=0; 
    case_num:=0; 
    city_var.EXTEND(30);                         
    city_var(1):='北京';                          
    city_var(2):='上海';                        
    city_var(3):='成都';                           
    city_var(4):='青岛';                         
    city_var(5):='广州'; 
    city_var(6):='香港';
    city_var(7):='西安';  
    city_var(8):='拉萨';
    city_var(9):='杭州'; 
    city_var(10):='深圳';
    
    city_var(11):='汽车';
    city_var(12):='手机';
    city_var(13):='日用品';
    city_var(14):='电脑';
    city_var(15):='海鲜';
    city_var(16):='植物';
    city_var(17):='家具';
    city_var(18):='服装';
    city_var(19):='书籍';
    city_var(20):='饮料';
    
    for i in 1..insert_num loop 
     add_num:=random.value(1,10)::int;
     case_num:=random.value(11,20)::int;
     insert into product_test values (sys_guid(),'零食大礼包'||random.string(1),
                        random.chinese_string(random.value(1,100)),
                        sysdate,
                        sysdate,
                        city_var(add_num),
                        city_var(case_num)
     );
     if mod(i,10000)=0 then
        commit;
     end if;
     end loop;
    end pr_test_insert;
    """
    cur.execute("truncate table SYSDBA.product_test ")
    cur.execute(sql)
    cur.callproc('pr_test_insert', (num,), (1,))


def create_product_proc():
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = f"""
    create or replace procedure pr_insert(num in int,insert_date in date) is 
    begin 
        for i in 1..num loop 
        insert into product  
            select sys_uuid, product_name,random.chinese_string(random.value(1,100)),manufacture_date,insert_date,address,product_type from product_test ;
        commit;
        end loop ;
    end pr_insert;
    """
    cur.execute(sql)


def create_procduct_test():
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = 'create table product_test as select * from product '
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
    tables = ['product', 'product_test']
    tables = [f'{db_user}.{i}' for i in tables]
    for table in tables:
        drop_tb(table)

    create_product_tb()
    create_procduct_test()


def generate_dates(n):
    current_date = datetime.now()
    date_list = [current_date + timedelta(days=i) for i in range(n)]
    date_strings = [date.strftime("%Y-%m-%d") for date in date_list]
    return date_strings


def execute_proc_args(name):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    # print(cur.callproc("test_in", (20,), (1,)))
    cur.callproc(name, (200000,), (1,))


def execute_proc(name, rows, date, db_host, db_port, db_user, db_pwd, db_name):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    cur.callproc(name, (rows, date), (1, 1))


# 多进程调用
def multi_process(proc_name, rows, dates, db_host, db_port, db_user, db_pwd, db_name):
    processes = []
    for date in dates:
        process = multiprocessing.Process(target=execute_proc,
                                          args=(proc_name, rows, date, db_host, db_port, db_user, db_pwd, db_name))
        processes.append(process)
    for process in processes:
        process.start()
    # 等待所有进程完成
    for process in processes:
        process.join()


def once_proc():
    tmp_n = int(input("请输入临时表行数: "))
    rows = int(input("请输入临时表插入正式表次数: "))
    days = int(input("请输入天数: "))
    dates = generate_dates(days)
    print(dates)
    create_temp_proc(tmp_n)
    create_product_proc()
    start = time.time()
    multi_process('pr_insert', rows, dates, db_host, db_port, db_user, db_pwd, db_name)
    end = time.time() - start
    show('product3')
    show('product_test3')
    print(f'耗时{end:.2f}秒', f'tps:{(tmp_n * rows * days / end):.2f} 行/s')


if __name__ == '__main__':
    print(xgcondb.version())
    freeze_support()
    db_host = '10.28.20.101'
    db_port = 6325
    db_user = 'SYSDBA'
    db_pwd = 'SYSDBA'
    db_name = 'SYSTEM'
    # db_host, db_port, db_user, db_pwd, db_name = parse_args()
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
