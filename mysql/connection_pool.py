import argparse
import csv
import os
import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import freeze_support
from pathlib import Path

import pymysql
from pymysql.cursors import DictCursor
from generate_assess_speed import get_speed

# 创建一个线程锁
lock = threading.Lock()
BASE_PATH = str(Path(__file__).parent)
timestands = time.strftime('%Y年%m月%d日%H%M%S', time.localtime())
dir = f'result_{timestands}'
EXCLUDE_DBS = ['information_schema', 'mysql', 'performance_schema', 'sys']


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
        return pymysql.connect(**self.connection_params)

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
        with self.get_connection() as conn:
            try:
                with conn.cursor(DictCursor) as cursor:
                    cursor.execute(sql)
                    result = cursor.fetchall()
                    conn.commit()
                    return result
            except Exception as e:
                conn.rollback()
                print(500, e)
        return None


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


def executor(data):
    if isinstance(data, tuple):
        sql, tmp_sql = data
        conn = pool.get_connection()
        cursor = conn.cursor(DictCursor)
        try:
            cursor.execute(sql)
        except Exception as e:
            print(f'执行异常；{sql},{e}')
            pool.release_connection(conn)
            return None
        rows = cursor.fetchall()
        pool.release_connection(conn)
        return rows, tmp_sql

    conn = pool.get_connection()
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(data)
    except Exception as e:
        print(f'执行异常；{data},{e}')
        pool.release_connection(conn)
        return None
    rows = cursor.fetchall()
    # print(rows)
    pool.release_connection(conn)
    return rows


def get_databases():
    sql = 'show databases;'
    databases = executor(sql)
    all_dbs = [i.get('Database') for i in databases]
    dbs = [i for i in all_dbs if i not in EXCLUDE_DBS]
    return dbs


def get_table_space():
    """
    -- 1、数据库表空间模式（on：独立表空间模式；off：系统表空间模式）
    SQL> show variables like 'innodb_file_per_table';
    :return:
    """
    sql = "show variables like 'innodb_file_per_table';"
    data = pool.executor(sql)
    # print(data)
    temp_sql = f'db--table--sql: -- -- {sql}'
    # write_csv('1.表空间模式.csv', data, temp_sql)
    write_csv('1.表空间模式.csv', [(data, temp_sql)])


def get_db_and_charset():
    """
    -- 2、数据库、表、列字符集
    -- 查看数据库及数据库的字符集
    SQL> show databases;
    SQL> show create database {数据库名称}\G;
    -- 查看指定数据库下表及表的字符集
    SQL> show tables from {数据库名称};
    SQL> show table status from {数据库名称} like {表名称};
    -- 查看表中所有列的字符集
    SQL> show full columns from {表名称};
    :return:
    """
    start = time.time()
    dbs = get_databases()
    tb_status = []
    tb_culumns = []
    tb_charsets = []
    db_charsets = []
    for db in dbs:
        sql1 = f'show create database `{db}`;'
        sql2 = f'show tables from `{db}`;'
        data = executor(sql1)
        tables = executor(sql2)
        tables = [i.get(f'Tables_in_{db}') for i in tables]
        temp_sql = f'db--table--sql: {db}-- -- {sql1}'
        db_charsets.append((data, temp_sql))
        # write_csv('2.数据库字符集.csv', data, temp_sql)
        for tb in tables:
            sql3 = f"show table status from `{db}` like '{tb}';"
            sql4 = f'show full columns from `{db}`.`{tb}`'
            sql5 = f'show create table  `{db}`.`{tb}`'
            # data = executor(sql3)
            temp_sql3 = f'db--table--sql: {db}--{tb}-- {sql3}'
            tb_status.append((sql3, temp_sql3))
            # write_csv('2.tb_status 表字符集.csv', data, temp_sql)
            # print(sql4)
            # data = executor(sql4)
            temp_sql4 = f'db--table--sql: {db}--{tb}-- {sql4}'
            tb_culumns.append((sql4, temp_sql4))
            # write_csv('2.tb_column 列字符集.csv', data, temp_sql)
            temp_sql5 = f'db--table--sql: {db}--{tb}-- {sql5}'
            tb_charsets.append((sql5, temp_sql5))

    # print(len(db_charsets), len(tb_status), len(tb_culumns))
    file_name1 = '2.数据库字符集.csv'
    # for data, temp_sql in db_charsets:
    write_csv(file_name1, db_charsets)
    file_name2 = '2.tb_status 表字符集.csv'
    with ThreadPoolExecutor(max_workers=5) as executors:
        futures2 = list(executors.map(executor, tb_status))
        write_csv(file_name2, futures2)

    file_name3 = '2.tb_column 列字符集.csv'
    with ThreadPoolExecutor(max_workers=5) as executors:
        futures3 = list(executors.map(executor, tb_culumns))
        write_csv(file_name3, futures3)

    file_name4 = '2.表字符集.csv'
    with ThreadPoolExecutor(max_workers=5) as executors:
        futures4 = list(executors.map(executor, tb_charsets))
        write_csv(file_name4, futures4)


def get_db_obj():
    """
    -- 3、统计数据库对象及个数
SQL> select 'database' type,schema_name db,count(*) cnt from information_schema.SCHEMATA  group by db
     union all
     select 'table' type,table_schema db,count(*) cnt from information_schema.TABLES a
             group by table_schema
     union all
     select 'events' type,event_schema db,count(*) cnt from information_schema.EVENTS b
            group by event_schema
     union all
     select 'triggers' type,trigger_schema db,count(*) cnt
            from information_schema.TRIGGERS c group by trigger_schema
     union all
     select 'procedure' type,routine_schema db,count(*) cnt from information_schema.ROUTINES d
            where routine_type='PROCEDURE' group by db
     union all
     select 'function' type,routine_schema db,count(*) cnt from information_schema.ROUTINES e
            where routine_type='FUNCTION' group by db
     union all
     select 'views' type,table_schema db,count(*) cnt
            from information_schema.views f group by table_schema
     union all
     select 'index' type,index_schema db,count(distinct index_name) cnt
            from information_schema.STATISTICS g group by db
     union all
     select 'partition table' type,table_schema db,count(*) cnt from information_schema.PARTITIONS p
            where partition_name is not null group by db;
    :return:
    """
    sql = """
       select 'database' type,schema_name db,count(*) cnt from information_schema.SCHEMATA where schema_name not in ('information_schema', 'mysql', 'performance_schema', 'sys')
    group by db
    union all
    select 'table' type,table_schema db,count(*) cnt from information_schema.TABLES a 
        where table_type != 'VIEW' and table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys') 
        group by table_schema
    union all
    select 'events' type,event_schema db,count(*) cnt from information_schema.EVENTS b where event_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
        group by event_schema
    union all
    select 'triggers' type,trigger_schema db,count(*) cnt
        from information_schema.TRIGGERS c where trigger_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
        group by trigger_schema
    union all
    select 'procedure' type,routine_schema db,count(*) cnt from information_schema.ROUTINES d
        where routine_type='PROCEDURE' and routine_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
        group by db
    union all
    select 'function' type,routine_schema db,count(*) cnt from information_schema.ROUTINES e
        where routine_type='FUNCTION' and routine_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
        group by db
    union all
    select 'views' type,table_schema db,count(*) cnt
        from information_schema.views f where table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
        group by table_schema
    union all
    select 'index' type,index_schema db,count(distinct index_name) cnt
        from information_schema.STATISTICS g  where index_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
        group by db
    union all
    select 'partition table' type,table_schema db,count(*) cnt from information_schema.PARTITIONS p
     where  partition_name is not null and  table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
     group by db;
    """
    data = executor(sql)
    # print(data)
    temp_sql = f'db--table--sql: -- -- {sql}'
    # print(data)
    write_csv('3.db对象及个数.csv', [(data, temp_sql)])


def count_table_culumns():
    """
    -- 4、统计表数据量
SQL> select table_name,table_rows from information_schema.tables
     where TABLE_SCHEMA = '{数据库名称}' order by table_name desc;
    :return:
    """
    dbs = get_databases()
    for db in dbs:
        sql = f"""
        select table_name,table_rows from information_schema.tables
        where TABLE_SCHEMA = '{db}' and table_type != 'VIEW' order by table_rows desc;
        """
        data = executor(sql)
        # print(data)
        temp_sql = f'db--table--sql: {db}-- -- {sql}'
        write_csv('4.每个表的行数.csv', [(data, temp_sql)])


def user_table_space():
    """
    --5、查看所有用户下的表对象使用空间大小
SQL> select table_schema,concat(round(sum((data_length + index_length)/1024/1024),2),'MB') as data from
     information_schema.TABLES group by table_schema;
    :return:
    """
    sql = '''
    select table_schema,concat(round(sum((data_length + index_length)/1024/1024),2),'MB') as data from
     information_schema.TABLES group by table_schema;
    '''
    data = executor(sql)
    # print(data)
    temp_sql = f'db--table--sql: -- -- {sql}'
    write_csv('5.每个库大小.csv', [(data, temp_sql)])


def get_tb_column():
    """
    --6、查看数据库表对象使用的列名称(关键字排查)
SQL> select column_name from information_schema.columns c
     where table_schema='{数据库名称}' group by column_name order by column_name desc;
    :return:
    """
    dbs = get_databases()
    for db in dbs:
        sql = f'''select column_name from information_schema.columns c
     where table_schema='{db}' group by column_name order by column_name desc;'''
        data = executor(sql)
        # print(data)
        temp_sql = f'db--table--sql: {db}-- -- {sql}'
        write_csv('6.表列名.csv', [(data, temp_sql)])


def get_db_columu_type_and_count():
    """
    --7、查看数据库表对象使用的数据类型
SQL> select data_type, count(*) cnt from information_schema.COLUMNS c
         where table_schema='{数据库名称}' group by data_type order by cnt desc;
    :return:
    """
    dbs = get_databases()
    for db in dbs:
        sql = f"""select data_type, count(*) cnt from information_schema.COLUMNS c
             where table_schema='{db}' group by data_type order by cnt desc;"""
        data = executor(sql)
        # print(data)
        temp_sql = f'db--table--sql: {db}-- -- {sql}'
        write_csv('7.每个库字段类型及个数.csv', [(data, temp_sql)])


def get_primary_key_and_foreige_key():
    """
    --8、列出所有的非系统数据所有表的主键信息和外键信息
    select
      o.constraint_schema 约束拥有者,
      o.constraint_name 约束名称,
      o.table_schema 表拥有者,
      o.table_name 表名,
      o.column_name 列名,
      o.referenced_table_schema 外键表拥有者,
      o.referenced_table_name 外键表名,
      o.referenced_column_name 外键表列名,
      o.update_rule 约束更新规则,
      o.delete_rule 约束删除规则,
      o.unique_constraint_name 唯一约束名称,
      t.constraint_type 约束类型
    from
    (
      select
        k.constraint_schema,
        k.constraint_name,
        k.table_schema,
        k.table_name,
        k.column_name,
        k.referenced_table_schema,
        k.referenced_table_name,
        k.referenced_column_name,
        r.update_rule,
        r.delete_rule,
        r.unique_constraint_name
      from
        information_schema.key_column_usage k
      left join information_schema.referential_constraints r on
        k.constraint_name = r.constraint_name
    ) as o
    inner join information_schema.table_constraints t on
      o.table_name = t.table_name
      and t.constraint_name = o.constraint_name
    where
      o.constraint_schema != 'mysql' and o.constraint_schema != 'sys';
    :return:
    """
    sql = '''
    select
      o.constraint_schema 约束拥有者,
      o.constraint_name 约束名称,
      o.table_schema 表拥有者,
      o.table_name 表名,
      o.column_name 列名,
      o.referenced_table_schema 外键表拥有者,
      o.referenced_table_name 外键表名,
      o.referenced_column_name 外键表列名,
      o.update_rule 约束更新规则,
      o.delete_rule 约束删除规则,
      o.unique_constraint_name 唯一约束名称,
      t.constraint_type 约束类型
    from
    (
      select
        k.constraint_schema,
        k.constraint_name,
        k.table_schema,
        k.table_name,
        k.column_name,
        k.referenced_table_schema,
        k.referenced_table_name,
        k.referenced_column_name,
        r.update_rule,
        r.delete_rule,
        r.unique_constraint_name
      from
        information_schema.key_column_usage k
      left join information_schema.referential_constraints r on
        k.constraint_name = r.constraint_name
    ) as o
    inner join information_schema.table_constraints t on
      o.table_name = t.table_name
      and t.constraint_name = o.constraint_name
    where
      o.constraint_schema != 'mysql' and o.constraint_schema != 'sys' and o.constraint_schema != 'performance_schema' and o.constraint_schema != 'information_schema'
        order by o.constraint_schema
    '''
    data = pool.executor(sql)
    temp_sql = f'db--table--sql: -- -- {sql}'
    write_csv('8.库的主键及外键.csv', [(data, temp_sql)])


def summary():
    """
    库 表 行数
    :return:
    """
    sql = """
    select '数据库版本' type,@@version cnt
     union all
     select '数据库' type,count(*) cnt from information_schema.SCHEMATA   where schema_name not in ('information_schema', 'mysql', 'performance_schema', 'sys')
     union all
     select '表' type,count(*) cnt from information_schema.TABLES a
            where table_type != 'VIEW' and  table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys') 
     union all
     select '定时作业' type,count(*) cnt from information_schema.EVENTS b where event_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
            
     union all
     select '触发器' type,count(*) cnt
            from information_schema.TRIGGERS c  where trigger_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
     union all
     select '存储过程' type,count(*) cnt from information_schema.ROUTINES d
            where routine_type='PROCEDURE' and routine_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
     union all
     select '函数' type,count(*) cnt from information_schema.ROUTINES e
            where routine_type='FUNCTION' and routine_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
     union all
     select '视图' type,count(*) cnt
            from information_schema.views f  where table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
     union all
     select '索引' type,count(distinct index_name) cnt
            from information_schema.STATISTICS g  where index_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
     union all
     select '分区表' type,count(*) cnt from information_schema.PARTITIONS p
            where partition_name is not null and  table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
     union all 
     select  '总行数' type, SUM(table_rows) as cnt
     from information_schema.TABLES
     where table_type != 'VIEW' and table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
     union all 
     select '总表空间大小' type,concat(round(sum((data_length + index_length)/1024/1024),2),'MB') as cnt from
     information_schema.TABLES where table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
     union all 
     select '时区' type, @@global.time_zone as cnt
     union all 
     select '用户' type ,count(user) as cnt from mysql.user;
    """
    data = pool.executor(sql)
    # print(data)
    temp_sql = f'db--table--sql: -- -- {sql}'
    # print(data)
    for i in data:
        print(f"{i.get('type')}: {i.get('cnt')}")

    write_csv('9.汇总信息.csv', [(data, temp_sql)])


def user_privilege():
    """
    获取数据库用户权限
    :return:
    """
    sql = """
    SELECT
                user,
                host,
                Select_priv,Insert_priv,Update_priv,Delete_priv,Create_priv,
                Drop_priv,Reload_priv,Shutdown_priv,Process_priv,
                File_priv,Grant_priv,References_priv,Index_priv,Alter_priv,
                Show_db_priv,Super_priv,Create_tmp_table_priv,
                Lock_tables_priv,Execute_priv,
                Repl_slave_priv,Repl_client_priv,
                Create_view_priv,Show_view_priv,
                Create_routine_priv,Alter_routine_priv,
                Create_user_priv,Event_priv,
                Trigger_priv,Create_tablespace_priv,
                max_questions,
                max_updates,
                max_connections as '最大连接数',
                max_user_connections as '最大用户连接数'
              FROM mysql.user   
    """
    data = pool.executor(sql)
    temp_sql = f'db--table--sql: -- -- {sql}'
    write_csv('用户及用户权限.csv', [(data, temp_sql)])


def keywords():
    """
    获取关键字
    :return:
    """
    sql = 'show tables from information_schema;'
    sql1 = """
    SELECT word as "关键字",reserved as "是否保留" FROM information_schema.keywords order by reserved desc;
    """
    data = pool.executor(sql)
    tables = {i.get('Tables_in_information_schema') for i in data}
    # 判断是否有关键字表
    if 'keywords' in tables or 'KEYWORDS' in tables:
        data1 = pool.executor(sql1)
        temp_sql = f'db--table--sql: -- -- {sql1}'
        write_csv('关键字.csv', [(data1, temp_sql)])


def db_statistics():
    """
    数据库的统计信息
    :return:
    """
    sql = "SELECT * FROM  information_schema.statistics where index_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')"
    data = pool.executor(sql)
    temp_sql = f'db--table--sql: -- -- {sql}'
    write_csv('数据统库计信息.csv', [(data, temp_sql)])


def status_variables():
    """
    数据库的统计信息
    :return:
    """
    sql1 = "SHOW GLOBAL STATUS"
    sql2 = "SHOW GLOBAL VARIABLES"
    data1 = pool.executor(sql1)
    data2 = pool.executor(sql2)
    temp_sql1 = f'db--table--sql: -- -- {sql1}'
    temp_sql2 = f'db--table--sql: -- -- {sql2}'
    write_csv('show status.csv', [(data1, temp_sql1)])
    write_csv('show variables.csv', [(data2, temp_sql2)])


def get_tablespace(is_speed=True):
    """
    获取表空间大小
    select
        table_schema '库名',
        table_name as '表名',
        round(((data_length + index_length) / 1024 / 1024), 2) as '大小(MB)'
    from
        information_schema.tables
    where
         table_type != 'VIEW' and table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
    group by table_schema,table_name,'大小(MB)'
    :return:
    """
    sql = """
   select 
        table_schema '库名',
        table_name as '表名',
        round((sum(data_length + index_length) / 1024 / 1024), 2) as '大小(MB)'
    from 
        information_schema.tables 
    where 
         table_type != 'VIEW' and table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys') 
    group by table_schema,table_name
    """
    datas = pool.executor(sql)
    temp_sql = f'db--table--sql: -- -- {sql}'
    if is_speed:
        speed = get_speed()
        for i in datas:
            i.update({
                '迁移后大小(MB)': float(i.get('大小(MB)')) * 1.1,
                '迁移所需时间(秒)': round(float(i.get('大小(MB)')) / speed * 18, 2)
            })
    # print(datas)
    write_csv('5.每张表大小.csv', [(datas, temp_sql)])


def get_event_job():
    """
    show events
    show create event idc.event_one
    :return:
    """
    sql = "select event_schema,event_name from information_schema.events where event_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')"
    jobs = pool.executor(sql)
    events = []
    for i in jobs:
        db = i.get('EVENT_SCHEMA') or i.get('event_schema')
        event_name = i.get('EVENT_NAME') or i.get('event_name')
        sql2 = f'show create event `{db}`.`{event_name}`'
        data = pool.executor(sql2)
        temp_sql2 = f'db--table--sql: {db}-- -- {sql2}'
        events.append((data, temp_sql2))
    write_csv('定时作业 show events.csv', events)


def get_triggers():
    """
    show triggers
    select trigger_schema,trigger_name from information_schema.triggers
    show create trigger trigger_one
    :return:
    """
    sql = "select trigger_schema,trigger_name from information_schema.triggers where trigger_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')"
    triggers = pool.executor(sql)
    res = []
    for i in triggers:
        db = i.get('TRIGGER_SCHEMA') or i.get('trigger_schema')
        trigger_name = i.get('TRIGGER_NAME') or i.get('trigger_name')
        sql2 = f'show create trigger `{db}`.`{trigger_name}`'
        data = pool.executor(sql2)
        temp_sql2 = f'db--table--sql: {db}-- -- {sql2}'
        res.append((data, temp_sql2))
    write_csv('触发器 show triggers.csv', res)


def get_procedure():
    """
    show procedure status
    select routine_schema,routine_name from information_schema.routines  where routine_type='PROCEDURE'
    show create procedure  idc.get_data
    :return:
    """
    sql = "select routine_schema,routine_name from information_schema.routines  where routine_type='PROCEDURE' and routine_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')"
    procedures = pool.executor(sql)
    res = []
    for i in procedures:
        db = i.get('ROUTINE_SCHEMA') or i.get('routine_schema')
        procedure_name = i.get('ROUTINE_NAME') or i.get('routine_name')
        sql2 = f'show create procedure `{db}`.`{procedure_name}`'
        data = executor(sql2)
        temp_sql2 = f'db--table--sql: {db}-- -- {sql2}'
        res.append((data, temp_sql2))
    write_csv('存储过程 show procedure status.csv', res)


def get_function():
    """
    select routine_schema,routine_name from information_schema.routines  where routine_type='FUNCTION'
    show create function idc.func_1
    :return:
    """
    sql = "select routine_schema,routine_name from information_schema.routines  where routine_type='FUNCTION' and routine_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')"
    funcs = pool.executor(sql)
    res = []
    for i in funcs:
        db = i.get('ROUTINE_SCHEMA') or i.get('routine_schema')
        func_name = i.get('ROUTINE_NAME') or i.get('routine_name')
        sql2 = f'show create function `{db}`.`{func_name}`'
        data = executor(sql2)
        temp_sql2 = f'db--table--sql: {db}-- -- {sql2}'
        res.append((data, temp_sql2))
    write_csv('函数 .csv', res)


def get_view():
    """
    select table_schema,table_name from information_schema.views
    show create view idc.view_one
    :return:
    """
    sql = "select table_schema,table_name from information_schema.views where table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')"
    funcs = pool.executor(sql)
    res = []
    for i in funcs:
        db = i.get('TABLE_SCHEMA') or i.get('table_schema')
        view_name = i.get('TABLE_NAME') or i.get('table_name')
        sql2 = f'show create view `{db}`.`{view_name}`'
        data = executor(sql2)
        temp_sql2 = f'db--table--sql: {db}-- -- {sql2}'
        res.append((data, temp_sql2))
    write_csv('视图 .csv', res)


# db_host = '127.0.0.1'
# db_user = 'root'
# db_pwd = '123456'
# db_port = 3306
# # db_name = 'idc'
# db_charset = 'utf8'
#
# pool = ConnectionPool(
#     max_connections=100,
#     connection_params={
#         "user": db_user,
#         "password": db_pwd,
#         "host": db_host,
#         "port": db_port,
#         # "database": db_name,
#         "charset": 'utf8',
#     },
# )
# os.path.exists(dir) or os.makedirs(dir)
# print(dir)  # 54s
# # get_db_obj()
# # #
# # get_table_space()
# #
# # get_db_and_charset()
# keywords()
# crontab()
# get_db_and_charset()
# status_variables()
# db_statistics()


def migrate_post_db_charset():
    """
    -- 1、查看数据库及数据库的字符集
SQL> SELECT DB_NAME,CHAR_SET,TIME_ZONE,ONLINE FROM DBA_DATABASES WHERE DB_NAME='{数据库名称}';
    :return:
    """
    dbs = get_databases()
    # print(len(dbs), dbs)
    for db in dbs:
        sql = f"SELECT DB_NAME,CHAR_SET,TIME_ZONE,ONLINE FROM DBA_DATABASES WHERE DB_NAME='{db}';"
        data = pool.executor(sql)
        # print(data)


# 迁移后数据
# migrate_post_db_charset()

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
                               _
     _ __ ___  _   _ ___  __ _| |
    | '_ ` _ \| | | / __|/ _` | |
    | | | | | | |_| \__ \ (_| | |
    |_| |_| |_|\__, |___/\__, |_|
               |___/        |_|      power by xugu  v1.0.0

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
    parser.add_argument('-P', '--port', help='Port number 数据库端口', type=int)
    parser.add_argument('-u', '--user', help='输入数据库 用户')
    parser.add_argument('-p', '--pwd', help='输入数据库密码')
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
    verbose = args.verbose

    # 在这里可以根据解析的参数执行相应的操作
    if len(sys.argv) == 1:
        host = input("请输入ip: ")
        port = int(input("请输入端口: "))
        user = input("请输入用户: ")
        password = input("请输入密码: ")
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
    if host and port and user and password:
        print(f'host: {host} port: {port} user: {user} password: {password}')

    return host, port, user, password


if __name__ == "__main__":
    if sys.platform == 'win32':
        freeze_support()  # linux 不需要
    # host = '10.28.23.207'
    # port = 3306
    # user = 'root'
    # password = 'Admin@123'
    # db = 'SYSTEM'
    # db_charset = 'utf8'
    host, port, user, password = parse_args()
    pool = ConnectionPool(
        max_connections=100,
        connection_params={
            "user": user,
            "password": password,
            "host": host,
            "port": port,
            # "database": db_name,
            "charset": 'utf8',
        },
    )
    os.path.exists(dir) or os.makedirs(dir)
    print(dir)
    start = time.time()
    task_names = [get_table_space, get_db_obj, user_table_space, get_tb_column, count_table_culumns,
                  user_privilege, keywords, get_procedure, get_function, get_view, get_db_columu_type_and_count,
                  db_statistics, status_variables, get_event_job, get_triggers]
    get_db_and_charset()
    # get_primary_key_and_foreige_key()
    main(task_names)
    get_tablespace(is_speed=False)
    summary()
    print(f'耗时: {time.time() - start:.2f} 秒')
    is_speed = input('\n请确定是否需要评估迁移时间(默认否) Y/N : ')
    if is_speed.lower().strip() == 'y':
        get_tablespace(is_speed=True)
    input('\nPress Enter to exit…')


# pyinstaller -c -F   --clean  --hidden-import=xgcondb   --hidden-import=PyMySQL    connection_pool.py
