import argparse
import csv
import os
import queue
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg2
from psycopg2 import extras

BASE_PATH = str(Path(__file__).parent)
timestands = time.strftime('%Y年%m月%d日%H%M%S', time.localtime())
dir = f'result_{timestands}'
EXCLUDE_DBS = ['pg_catalog', 'pg_toast', 'information_schema']


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
        return psycopg2.connect(**self.connection_params)

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
            cursor = conn.cursor(cursor_factory=extras.DictCursor)
            try:
                cursor.execute(sql)
            except Exception as e:
                print(f'执行异常；{sql},{e}')
                self.release_connection(conn)
                return None
            rows = cursor.fetchall()
            rows = [dict(row) for row in rows]
            self.release_connection(conn)
            return rows, tmp_sql

        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=extras.DictCursor)
        try:
            cursor.execute(data)
        except Exception as e:
            conn.rollback()
            print(f'执行异常；{data},{e}')
            self.release_connection(conn)
            return None
        rows = cursor.fetchall()
        conn.commit()
        self.release_connection(conn)
        return [dict(row) for row in rows]


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


def get_database_characters():
    """
    -- 1、查看数据库及数据库的字符集
    select datname,pg_encoding_to_char(encoding) from  pg_database;
    :return:
    """
    sql = """
    select datname "数据库",pg_encoding_to_char(encoding) "字符集" from  pg_database
    """
    data = pool.executor(sql)
    temp_sql = f'db--table--sql: --pg_database -- {sql}'
    write_csv('1.数据库字符集.csv', [(data, temp_sql)])


def get_db_objects():
    """
    -- 2、统计当前库中用户创建的对象及个数
    :return:
    """
    sql = """
    select 'database' as type,datname as db,count(*) from pg_catalog.pg_database group by datname
    union  all
    select 'schemas' as type,catalog_name as db,count(*) from information_schema.schemata 
        where schema_name not in ('pg_catalog','pg_toast','information_schema') group by catalog_name
    union  all
    select 'tables' as type,table_catalog as db,count(*) from information_schema.tables 
        where table_type='BASE TABLE' and table_schema not in ('pg_catalog','pg_toast','information_schema') group by table_catalog
    union  all
    select 'views' as type,table_catalog as db,count(*) from information_schema.views 
        where table_schema not in ('pg_catalog','pg_toast','information_schema') group by table_catalog
    union  all
    select 'sequences' as type,sequence_catalog as db,count(*) from information_schema.sequences group by sequence_catalog
    union  all
    select 'procedures' as type,specific_catalog as db,count(*) from information_schema.routines 
        where  routine_type='PROCEDURE' and specific_schema not in ('pg_catalog','pg_toast','information_schema') group by specific_catalog
    union  all
    select 'functions' as type,specific_catalog as db,count(*) from information_schema.routines 
        where routine_type='FUNCTION' and specific_schema not in ('pg_catalog','pg_toast','information_schema') group by specific_catalog
    union  all
    select 'triggers' as type,trigger_catalog as db,count(*) from information_schema.triggers 
        where trigger_schema  not in ('pg_catalog','pg_toast','information_schema') group by trigger_catalog
    union all
    select 'indexes' as type,s.catalog_name as db,count(*) from pg_catalog.pg_indexes pi 
        left join information_schema.schemata s on pi.schemaname=s.schema_name 
        where schemaname not in ('pg_catalog','pg_toast','information_schema') group by s.catalog_name
    union all
    select 'partition table' as type,current_database() as db,count(*) from (select distinct inhparent from pg_inherits) tb
    """
    data = pool.executor(sql)
    temp_sql = f'db--table--sql: -- -- {sql}'
    write_csv('2.数据库对象个数.csv', [(data, temp_sql)])


def get_db_size():
    """
    -- 3 库大小
    select datname as database_name, pg_size_pretty(pg_database_size(datname)) as database_size from pg_database
    :return:
    """
    sql = """
    select datname "数据库", pg_size_pretty(pg_database_size(datname)) "大小" from pg_database
    """
    data = pool.executor(sql)
    temp_sql = f'db--table--sql: -- pg_database -- {sql}'
    write_csv('3.每个库大小.csv', [(data, temp_sql)])


def get_table_row_count():
    """
    -- 4、统计表数据量(包括分区表中各分区的表数据量，父表数据量需要求和其对应分区表的数据量总和，可以使用select count(*) from 分区表父表 查询出分区表的总数据量)
    select schemaname,relname, sum(n_live_tup) as  nums  from pg_catalog.pg_stat_user_tables GROUP BY schemaname, relname order by schemaname,nums desc
    select schemaname,relname, n_live_tup as  nums  from pg_catalog.pg_stat_user_tables order by schemaname,nums desc
    :return:
    """
    sql = """
    select schemaname "模式",relname "表名", n_live_tup "行数"  from pg_catalog.pg_stat_user_tables order by schemaname,n_live_tup desc 
    """
    data = pool.executor(sql)
    temp_sql = f'db--table--sql: -- pg_catalog.pg_stat_user_tables -- {sql}'
    write_csv('4.每个表的行数.csv', [(data, temp_sql)])


def get_table_size():
    """
    --5、查看所有用户下的表对象使用空间大小
    select schemaname   "模式" ,relname "表", pg_size_pretty(pg_relation_size(relid)) "大小" from pg_stat_user_tables  order by schemaname,pg_relation_size(relid) desc;
    :return:
    """
    sql = """
    select schemaname   "模式" ,relname "表", pg_size_pretty(pg_relation_size(relid)) "大小" from pg_stat_user_tables  order by schemaname,pg_relation_size(relid) desc
    """
    data = pool.executor(sql)
    temp_sql = f'db--table--sql: -- pg_stat_user_tables -- {sql}'
    write_csv('5.每个表的大小.csv', [(data, temp_sql)])


def get_db_column():
    """
    --6、查看数据库表对象使用的列名称(关键字排查)
     select  distinct column_name "列名" from information_schema.columns where table_schema not in ('pg_catalog','information_schema','pg_toast') order by column_name
    :return:
    """
    sql = """
    select  distinct column_name  "列名" from information_schema.columns where table_schema not in ('pg_catalog','information_schema','pg_toast') order by column_name 
    """
    data = pool.executor(sql)
    temp_sql = f'db--table--sql: -- information_schema.columns -- {sql}'
    write_csv('6.每个库的所有列名(用于关键字排查).csv', [(data, temp_sql)])


def get_db_objects_type_and_nums():
    """
    --7、查看数据库表对象使用的数据类型
    select  data_type,count(*) from information_schema.columns where table_schema not in ('pg_catalog','information_schema','pg_toast') group by data_type order by count(*) desc
    :return:
    """
    sql = """
    select  data_type "数据类型" ,count(*) "个数" from information_schema.columns where table_schema not in ('pg_catalog','information_schema','pg_toast') group by data_type order by count(*) desc
    """
    data = pool.executor(sql)
    temp_sql = f'db--table--sql: -- information_schema.columns -- {sql}'
    write_csv('7.当前库的数据类型及个数.csv', [(data, temp_sql)])


def get_primary_keys_and_foreign_keys():
    """
    --8、列出所有的非系统数据所有表的主键信息和外键信息
    select
        kcu.constraint_schema  as 约束拥有者,
        pc.conname as 约束名称,
        kcu.table_schema as 表拥有者,
        kcu.table_name as 表名,
        kcu.column_name as 列名,
        case when kcu.table_name=ccu.table_name then null else ccu.table_schema end as 外键表拥有者,
        case when kcu.table_name=ccu.table_name then null else ccu.table_name end as 外键表名,
        case when kcu.table_name=ccu.table_name then null else  ccu.column_name end as 外键表列名,
        case pc.confupdtype
                when 'a' then '无动作'
                when 'r' then '限制'
                when 'c' then '级联'
                when 'n' then '设置为空'
                when 'd' then '设置为缺省'
                else null end as 约束更新规则,
        case pc.confdeltype
                when 'a' then '无动作'
                when 'r' then '限制'
                when 'c' then '级联'
                when 'n' then '设置为空'
                when 'd' then '设置为缺省'
                else null end as 约束删除规则,
        pc.contype as 约束类型,
        pc.convalidated as 约束状态
    from pg_catalog.pg_constraint pc
    inner join information_schema.key_column_usage kcu on kcu.constraint_name=pc.conname
    inner join information_schema.constraint_column_usage ccu on ccu.constraint_name=pc.conname
    where kcu.constraint_schema not in ('pg_catalog','information_schema','pg_toast');
    :return:
    """

    sql = """
        select
        kcu.constraint_schema  as 约束拥有者,
        pc.conname as 约束名称,
        kcu.table_schema as 表拥有者,
        kcu.table_name as 表名,
        kcu.column_name as 列名,
        case when kcu.table_name=ccu.table_name then null else ccu.table_schema end as 外键表拥有者,
        case when kcu.table_name=ccu.table_name then null else ccu.table_name end as 外键表名,
        case when kcu.table_name=ccu.table_name then null else  ccu.column_name end as 外键表列名,
        case pc.confupdtype
                when 'a' then '无动作'
                when 'r' then '限制'
                when 'c' then '级联'
                when 'n' then '设置为空'
                when 'd' then '设置为缺省'
                else null end as 约束更新规则,
        case pc.confdeltype
                when 'a' then '无动作'
                when 'r' then '限制'
                when 'c' then '级联'
                when 'n' then '设置为空'
                when 'd' then '设置为缺省'
                else null end as 约束删除规则,
        pc.contype as 约束类型,
        pc.convalidated as 约束状态
    from pg_catalog.pg_constraint pc
    inner join information_schema.key_column_usage kcu on kcu.constraint_name=pc.conname
    inner join information_schema.constraint_column_usage ccu on ccu.constraint_name=pc.conname
    where kcu.constraint_schema not in ('pg_catalog','information_schema','pg_toast')
    """
    data = pool.executor(sql)
    temp_sql = f'db--table--sql: --  -- {sql}'
    write_csv('8.主键及外键信息.csv', [(data, temp_sql)])


def summary():
    """
    -- 9 汇总信息： 库 表 行数
    :return:
    """
    sql = 'select current_database()'
    data = pool.executor(sql)
    current_db = data[0].get('current_database')
    sql2 = f"""
    select '数据库版本' type,substring(version() from '(.*?\d+\.\d+)\s') cnt
    union all
    select '数据库' type,count(*)::varchar from pg_database
    union all
    select '当前库模式' type,count(*) ::varchar cnt from information_schema.SCHEMATA   where schema_name not in ('pg_catalog','information_schema','pg_toast')
    union all
    select '表' type,count(*)::varchar cnt from information_schema.tables a where table_type != 'VIEW' and  table_schema not in ('pg_catalog','information_schema','pg_toast')
    union all
    select '触发器' type,count(*)::varchar cnt from information_schema.triggers where trigger_schema  not in ('pg_catalog','pg_toast','information_schema')
    union all
    select '存储过程' type,count(*)::varchar cnt from information_schema.routines where  routine_type='PROCEDURE' and specific_schema not in ('pg_catalog','pg_toast','information_schema')
    union all
    select '函数' type,count(*)::varchar cnt from information_schema.routines where routine_type='FUNCTION' and specific_schema not in ('pg_catalog','pg_toast','information_schema') 
    union all
    select '视图' type,count(*)::varchar cnt from information_schema.views where table_schema not in ('pg_catalog','pg_toast','information_schema') 
    union all 
    select '索引' type,count(indexname)::varchar cnt from pg_catalog.pg_indexes pi left join information_schema.schemata s on pi.schemaname=s.schema_name where schemaname not in ('pg_catalog','pg_toast','information_schema') 
    union all
    select '分区表' type,count(*)::varchar cnt from (select distinct inhparent from pg_inherits) tb
    union all 
    select   '时区' type, current_setting('TIMEZONE') "时区"
    union all 
    SELECT '用户' type ,count(*)::varchar  cnt FROM pg_user
    union all 
    select  '总行数' type, sum(n_live_tup)::varchar cnt FROM pg_stat_user_tables where schemaname not in ('pg_catalog','information_schema','pg_toast') 
    union all 
    select  '当前库大小' type, pg_size_pretty(pg_database_size(datname))::varchar cnt   FROM pg_database where datname ='{current_db}'
    """
    data = pool.executor(sql2)
    for i in data:
        print(f"{i.get('type')}: {i.get('cnt')}")
    # print(data)
    temp_sql = f'db--table--sql: -- -- {sql2}'
    write_csv('9.汇总信息.csv', [(data, temp_sql)])


def main(task_names):
    # print(len(task_names))
    with ThreadPoolExecutor(max_workers=len(task_names)) as executor:
        futures = executor.map(lambda func: func(), task_names)
        # 获取并处理任务的返回结果
        for future in futures:
            try:
                # for future in concurrent.futures.as_completed(futures):
                if future is not None:
                    future.result()
            # print(f"Function task returned: {result}")
            except Exception as e:
                print(f"Function task encountered an error: {e}")


def parse_args():
    program = rf"""
     ____           _                 ____   ___  _     
    |  _ \ ___  ___| |_ __ _ _ __ ___/ ___| / _ \| |    
    | |_) / _ \/ __| __/ _` | '__/ _ \___ \| | | | |    
    |  __/ (_) \__ \ || (_| | | |  __/___) | |_| | |___ 
    |_|   \___/|___/\__\__, |_|  \___|____/ \__\_\_____|
                       |___/                               power by xugu  v1.0.0
    """
    print(program)
    parser = argparse.ArgumentParser(
        # description='这是一个数据库环境采集工具',
        prefix_chars='-'
    )
    # 添加位置参数
    # parser.add_argument('input_file', help='输入文件的路径')
    # 添加可选参数
    # parser.add_argument('-o', '--output', help='输出文件的路径')
    parser.add_argument('-H', '--host', help='输入数据库ip地址')
    parser.add_argument('-P', '--port', help='Port number 数据库端口', type=int, default=5432)
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
    if host and port and user and password and db:
        print(f'host: {host} port: {port} user: {user} password: {password} db: {db}')

    return host, port, user, password, db


if __name__ == '__main__':
    # db_host = '10.28.23.197'
    # db_port = 5432
    # db_user = 'postgres'
    # db_pwd = 'postgres'
    # db_name = 'test'
    # # db_charset = 'utf8'
    #
    # pool = ConnectionPool(
    #     max_connections=100,
    #     connection_params={
    #         "user": db_user,
    #         "password": db_pwd,
    #         "host": db_host,
    #         "port": db_port,
    #         # "db"
    #         "database": db_name,
    #         # "charset": 'utf8',
    #     },
    # )
    host, port, user, password, db = parse_args()
    pool = ConnectionPool(
        max_connections=100,
        connection_params={
            "user": user,
            "password": password,
            "host": host,
            "port": port,
            # "db"
            "database": db,
            # "charset": 'utf8',
        },
    )

    os.path.exists(dir) or os.makedirs(dir)
    print(dir)
    start = time.time()
    tasknames = [get_database_characters, get_db_objects, get_db_size, get_table_row_count, get_table_size,
                 get_db_column,
                 get_db_objects_type_and_nums, get_primary_keys_and_foreign_keys]
    main(tasknames)
    summary()
    print(f'耗时: {time.time() - start:.2f} 秒')

