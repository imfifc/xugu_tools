import argparse
import multiprocessing
import re
import sys
import time
from multiprocessing import freeze_support

import xgcondb


def get_cur(db_host, db_port, db_user, db_pwd, db_name):
    conn = xgcondb.connect(host=db_host, port=db_port, database=db_name, user=db_user, password=db_pwd, charset="utf8")
    cur = conn.cursor()
    return cur


# 用于数据库中已存在的存储过程，进行造数
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
    #     print(f'host: {host} port: {port} user: {user} password: {password} db: {db} ')

    return host, port, user, password, db


def execute_proc_args(name):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    # print(cur.callproc("test_in", (20,), (1,)))
    cur.callproc(name, (200000,), (1,))


def parse_str(input_string):
    # input_string = "123 45.67 abc 89.0 def 10 0"
    # 使用正则表达式匹配整数、浮点数和字符串(用|)对多个正则表达式区分
    pattern = re.compile(r".*?(\d+\.\d+|\d+|\w+)")
    matches = re.findall(pattern, input_string)
    # print(matches)
    res = []
    for match in matches:
        if "." in match:
            res.append(float(match))
        elif match.isdigit() or (match[0] in "+-" and match[1:].isdigit()):
            res.append(int(match))
        else:
            res.append(match)
    return res


def create_products_tb(hotspot):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = f"""
    create table if not exists sysdba.products(
    product_no varchar(50) not null,
    product_name varchar(200),
    product_introduce varchar(4000),
    manufacture_date date,
    sell_dates varchar(50),
    address varchar(200),
    product_type varchar(50)
    )HOTSPOT {hotspot};
    """
    cur.execute(sql)


def create_package():
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    pkg_header = """
    CREATE OR REPLACE PACKAGE random IS
      FUNCTION value(min_value bigint, max_value bigint) return bigint;
      FUNCTION string(length IN NUMBER) RETURN varchar2;
    END;
    """
    pkg_body = """
    CREATE OR REPLACE PACKAGE BODY random as
        function value(min_value bigint, max_value bigint) return bigint as
            div  bigint := power(2, 31)-1;
            tmp_value  double;
            ret_value  bigint;
        begin
            tmp_value := to_number(abs(rand())) / div * (max_value - min_value);
            ret_value := round(tmp_value, 0) + min_value;
            return ret_value;
        end;
    
        FUNCTION string(length IN NUMBER) RETURN VARCHAR2 IS
           characters VARCHAR2(62) := 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
           random_string VARCHAR2(32767) := '';
        BEGIN
           FOR i IN 1..length LOOP
              random_string := random_string || SUBSTR(characters, CEIL(rand_value(1, 62)), 1);
           END LOOP;
           RETURN random_string;
        end;
    
    end random;
    """
    cur.execute(pkg_header)
    cur.execute(pkg_body)


def create_proc():
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = """
    create or replace procedure p_test(insert_num in int) as
    declare
    TYPE t_var IS varray(30) OF VARCHAR;
    city_var t_var;                            
    add_num int;
    case_num int;
    time_num int; 
    begin
    add_num:=0; 
    case_num:=0; 
    time_num :=0; 
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
    time_num:=random.value(1,365*3)::int;
    insert into products values(sys_guid(),RANDOM.STRING(8),'零食大礼包'||RANDOM.STRING(1),getdate(sysdate+time_num),getdate(sysdate+time_num+60),city_var(add_num),city_var(case_num));
     if mod(i,10000)=0 then
        commit;
     end if;
    end loop;
    end;
    """
    cur.execute(sql)


def show(table):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = f'select count(*) from {table}'
    data = cur.execute(sql)
    row = cur.fetchone()
    print(f'{table} : {row}')


def drop_tb(table):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = f"drop table if exists {table} cascade"
    cur.execute(sql)


def execute_proc(name, db_host, db_port, db_user, db_pwd, db_name, *args):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    if len(args):
        cur.callproc(name, tuple(args), tuple(1 for i in range(len(args))))
    else:
        cur.callproc(name)


# 多进程调用
def multi_process(n, proc_name, db_host, db_port, db_user, db_pwd, db_name, *args):
    processes = []
    for i in range(n):
        process = multiprocessing.Process(target=execute_proc,
                                          args=(proc_name, db_host, db_port, db_user, db_pwd, db_name, *args))
        processes.append(process)
    for process in processes:
        process.start()
    # 等待所有进程完成
    for process in processes:
        process.join()


if __name__ == '__main__':
    freeze_support()
    print(xgcondb.version())
    db_host, db_port, db_user, db_pwd, db_name = parse_args()

    create_package()
    create_products_tb(20)
    create_proc()

    while True:
        start = time.time()
        proc_name = input("请输入存储过程名(默认p_test):").strip() or 'p_test'
        proc_nums = int(input("请输入存储过程的参数(默认10000):") or 10000)
        parallel_n = int(input("请输入并发数:"))
        # proc_nums = parse_str(proc_nums)
        multi_process(parallel_n, proc_name, db_host, db_port, db_user, db_pwd, db_name, proc_nums)
        end = time.time() - start
        show('products')
        print(f'耗时{end:.2f}秒', f'tps:{(parallel_n * proc_nums / end):.2f} 行/s')
        flag = input("是否需要清除表重建，(默认不重建) 请输入Y/N: ")
        if flag == 'Y' or flag == 'y':
            drop_tb('products')
            create_products_tb(20)
            create_proc()
            print('已重建表')
        q = input('\nPress q to exit…')
        if q == 'q' or q == 'Q':
            break
