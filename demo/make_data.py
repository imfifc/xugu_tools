import argparse
import concurrent.futures
import multiprocessing
import queue
import re
import sys
import time
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

    def executor(self, sql):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            data = cursor.execute(sql)
        except Exception as e:
            conn.rollback()
            print(f'执行异常；{sql},{e}')
            self.release_connection(conn)
            return None
        if cursor.rowcount:
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            conn.commit()
            self.release_connection(conn)
            return [dict(zip(column_names, row)) for row in rows]

    def call_proc(self, name, *args):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            # print(name, tuple(args), tuple(1 for i in range(len(args))))
            if len(args):
                cursor.callproc(name, tuple(args), tuple(1 for i in range(len(args))))
            else:
                cursor.callproc(name)
        except Exception as e:
            conn.rollback()
            print(f'执行异常；{name},{e}')
            self.release_connection(conn)
            return None
        conn.commit()
        self.release_connection(conn)
        return None

    def execute_func(self, name, *args):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            data = cursor.callfunc(name, tuple(args), tuple(1 for i in range(len(args))))
        except Exception as e:
            conn.rollback()
            print(f'执行异常；{name},{e}')
            self.release_connection(conn)
            return None
        conn.commit()
        self.release_connection(conn)
        return data


# 存储过程中不能有注释


def drop_tb(table):
    sql = f"drop table if exists {table} cascade"
    pool.executor(sql)


def add_index():
    sql1 = 'alter table SYSDBA.EVALUATIONS add constraint EVALUATIONS_PK primary key(EVALUATION_NO);'
    sql2 = 'alter table SYSDBA.LOGISTICS add constraint INDEX_KEY primary key(LOGISTICS_NO);'
    sql3 = 'alter table SYSDBA.ORDERS add constraint INDEX_KEY primary key(ORDER_NO);'
    sql4 = 'alter table SYSDBA.PRODUCTS add constraint PRODUCT_KEY primary key(PRODUCT_NO);'
    sql5 = 'alter table SYSDBA.PURCHASES add constraint PURCHASE_KEY primary key(PURCHASE_NO);'

    sql6 = 'create index "EVALUATIONS_PRODUCT_NO_IDX" on SYSDBA.EVALUATIONS("PRODUCT_NO") indextype is btree global ;'
    sql7 = 'create index "PEODUCT_NO_KEY" on SYSDBA.LOGISTICS("PRODUCT_NO") indextype is btree global ;'
    sql8 = 'create index "PRODECT_KEY" on SYSDBA.ORDERS("PRODUCT_NO") indextype is btree global ;'
    sql9 = 'create index "PRODUCT_MANUF_DATE_INDEX" on SYSDBA.PRODUCTS("MANUFACTURE_DATE") indextype is btree global ;'
    sql10 = 'create index "PRODECT_NO_KEY" on SYSDBA.PURCHASES("PRODUCT_NO") indextype is btree global ;'
    sqls = [sql1, sql2, sql3, sql4, sql5, sql6, sql7, sql8, sql9, sql10]
    for i in sqls:
        pool.executor(i)


def create_products_tb(hotspot):
    sql2 = f"""
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
    pool.executor(sql2)


def create_evaluations_tb(hotspot):
    sql2 = f'''
    create table sysdba.evaluations(
    evaluation_no varchar(50) not null,
    product_no varchar(50),
    product_batche varchar(50),
    product_evaluation varchar(4000),
    evaluation_type varchar(50),
    evaluation_date date,
    deal varchar(200),
    product_bathe clob
    ) HOTSPOT {hotspot} ;
    '''
    pool.executor(sql2)


def create_logistics_tb(hotspot):
    sql = f'''
    create table sysdba.logistics(
    logistics_no varchar(50) not null,
    product_no varchar(50),
    recipient_name varchar(50),
    sender_name varchar(50),
    order_no varchar(50),
    notes varchar(4000),
    send_date date,
    reach_date date,
    logistics_type varchar(50)
    ) HOTSPOT {hotspot};
    '''
    pool.executor(sql)


def create_order_tb(hotspot):
    sql = f"""
    create table sysdba.orders(
    order_no varchar(50) not null,
    employee_no varchar(50),
    order_name varchar(50),
    order_num integer,
    order_date date,
    order_address varchar(200),
    notes varchar(4000),
    product_no varchar(50),
    order_type varchar(50)
    ) HOTSPOT {hotspot};
    """
    pool.executor(sql)


def create_purchases_tb(hotspot):
    sql = f'''
    create table sysdba.purchases(
    purchase_no varchar(50) not null,
    product_no varchar(50),
    purchase_date date,
    purchase_num integer,
    purchase_price integer,
    factory varchar(200),
    address varchar(200)
    ) HOTSPOT {hotspot};
    '''
    pool.executor(sql)


def create_temp_proc(num):
    """
    :param num:  往临时表单次的插入数据
    :return:
    """
    sql = f"""
    CREATE OR REPLACE PROCEDURE SP_insert_356_DATA() IS
    i number;
    begin
     for i in 1..{num} loop 
    INSERT INTO SYSDBA.PRODUCTS_TEST
                VALUES ( sys_uuid AS product_no 
                       , DBMS_RANDOM.STRING('x', 8) AS product_name 
                       , CASE TRUNC(DBMS_RANDOM.VALUE(1, 6)) 
                             when 1 then '零食大礼包A'
                             when 2 then '零食大礼包B'
                             when 3 then '零食大礼包C'
                             when 4 then '零食大礼包D'
                             when 5 then '零食大礼包E'
                             ELSE '零食大礼包E'
                             END AS product_introduce
                        , to_date('2017-01-01 00:00:00','yyyy-mm-dd hh24:mi:ss')+i AS manufacture_date
                        , to_date('2018-04-01 00:00:00','yyyy-mm-dd hh24:mi:ss')+i AS sell_dates
                       , CASE TRUNC(DBMS_RANDOM.VALUE(1, 6)) 
                             when 1 then '北京'
                             when 2 then '上海'
                             when 3 then '深圳'
                             when 4 then  '广州'
                             when 5 then '成都'
                             else  '武汉'
                             END address
                       ,CASE TRUNC(DBMS_RANDOM.VALUE(1, 6)) 
                             when 1 then '食品'
                             when 2 then '饰品'
                             when 3 then '汽车'
                             when 4 then  '五金'
                             when 5 then '军工'
                             else  '海鲜'
                             END);
    end loop;
    commit;
    end SP_insert_356_DATA;
    """
    sql2 = 'create table  PRODUCTS_TEST as select * from products;'
    pool.executor("truncate table SYSDBA.PRODUCTS_TEST ")
    # show('SYSDBA.PRODUCTS_TEST')
    pool.executor(sql)
    pool.call_proc('SP_insert_356_DATA')


def create_product_proc(num):
    sql = f"""
    create or replace procedure sp_insert_data is
    begin
    for x in 1..{num} loop	
    insert  into  sysdba.products
        select  sys_uuid as product_no,product_name,product_introduce,manufacture_date,sysdate,address,product_type from sysdba.products_test s;
    commit;
     end loop;  
    end sp_insert_data;	
    """
    pool.executor(sql)


def create_procduct_test():
    sql = 'create table if not exists PRODUCTS_TEST as select * from products;'
    pool.executor(sql)


def show(table):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = f'select count(*) from {table}'
    data = cur.execute(sql)
    row = cur.fetchone()
    print(f'{table} : {row}')


# 多进程调用
def mul_proc_executor():
    with concurrent.futures.ProcessPoolExecutor(max_workers=5) as executor:
        # with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # with concurrent.futures(max_workers=10) as executor:
        # 提交带参数的任务给线程池
        args = ['p_test' for i in range(5)]
        # args = ['p_test']
        # results = executor.map(execute_proc, args)
        results = executor.map(execute_proc_args, args)
        for result in results:
            print(result)
        # results = [executor.submit(execute_proc, arg) for arg in args]

        # # 获取任务的结果
        # for future in concurrent.futures.as_completed(results):
        #     result = future.result()
        #     print(f"Task result: {result}")


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


def rebuild_tables():
    """
    重建表： 先删除后重建
    :return:
    """
    tables = ['products', 'evaluations', 'logistics', 'orders', 'purchases', 'products_test']
    tables = [f'{db_user}.{i}' for i in tables]
    for table in tables:
        drop_tb(table)

    create_products_tb(20)
    create_evaluations_tb(20)
    create_logistics_tb(20)
    create_order_tb(20)
    create_purchases_tb(20)
    create_procduct_test()


def execute_proc_args(name):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    # print(cur.callproc("test_in", (20,), (1,)))
    cur.callproc(name, (200000,), (1,))


def execute_proc(name, db_host, db_port, db_user, db_pwd, db_name, *args):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    if len(args):
        cur.callproc(name, tuple(args), tuple(1 for _ in range(len(args))))
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


def once_proc():
    tmp_n = int(input("请输入临时表行数: "))
    proc_nums = int(input("请输入临时表插入正式表的次数: "))
    parallel_n = int(input("请输入并发数: "))
    create_temp_proc(tmp_n)
    create_product_proc(proc_nums)
    start = time.time()
    multi_process(parallel_n, 'sp_insert_data', db_host, db_port, db_user, db_pwd, db_name)
    end = time.time() - start
    show('products')
    show('products_test')
    print(f'耗时{end:.2f}秒', f'tps:{(tmp_n * parallel_n * proc_nums / end):.2f} 行/s')


if __name__ == '__main__':
    print(xgcondb.version())
    freeze_support()
    # db_host = '10.28.23.174'
    # db_port = 5138
    # db_user = 'SYSDBA'
    # db_pwd = 'SYSDBA'
    # db_name = 'SYSTEM'
    db_host, db_port, db_user, db_pwd, db_name = parse_args()
    pool = ConnectionPool(
        max_connections=100,
        connection_params={
            "user": db_user,
            "password": db_pwd,
            "host": db_host,
            "port": db_port,
            "database": db_name,
            "charset": 'utf8',
        },
    )
    pool.executor('set max_loop_num to 0')
    pool.executor('set max_trans_modify to 0')
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
