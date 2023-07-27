import queue
import pymysql
import csv
import datetime
import time
from pymysql.cursors import DictCursor
from concurrent.futures import ThreadPoolExecutor, wait


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
        conn = self.get_connection()
        try:
            cursor = conn.cursor(DictCursor)
            cursor.execute(sql)
            cursor.close()
            conn.commit()
            # print(200, cursor.fetchall())
            return cursor.fetchall()
        except Exception as e:
            conn.rollback()
            print(500, e)
        self.release_connection(conn)
        return None


# 使用连接池
db_host = '192.168.2.212'
db_user = 'ecology'
db_pwd = 'Weaver@2023'
db_port = 3306
db_name = 'ecology'
db_charset = 'utf8'

pool = ConnectionPool(
    max_connections=100,
    connection_params={
        "user": db_user,
        "password": db_pwd,
        "host": db_host,
        "port": db_port,
        "database": db_name,
        "charset": db_charset,
    },
)

csv_file_path = 'db.csv'


def write_csv(filename, data: [{}], sql=None):
    # 获取所有字段名，这里假设所有字典中的键相同
    fieldnames = data[0].keys()
    with open(filename, 'a+', newline='', encoding='utf-8') as f:
        write = csv.writer(f)
        if sql is not None:
            write.writerow([sql])

        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    # print("写入成功！")


def executor(sql):
    conn = pool.get_connection()
    cursor = conn.cursor(DictCursor)
    cursor.execute(sql)
    rows = cursor.fetchall()
    # print(rows)
    pool.release_connection(conn)
    return rows


def get_databases():
    sql = 'show databases;'
    databases = executor(sql)
    dbs = [i.get('Database') for i in databases]
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
    write_csv('1.表空间模式.csv', data, temp_sql)


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

    dbs = get_databases()
    db_charsets = []
    for db in dbs:
        sql1 = f'show create database {db};'
        sql2 = f'show tables from {db};'
        data = executor(sql1)
        print(data)

        # tables = executor(sql2)
        # tables = [i.get(f'Tables_in_{db}') for i in tables]
        temp_sql = f'db--table--sql: {db}-- -- {sql1}'
        db_charsets.append((data, temp_sql))

        # write_csv('2.数据库字符集.csv', data, temp_sql)
    # print(tables)
    # for tb in tables:
    #     sql3 = f"show table status from `{db}` like '{tb}';"
    #     sql4 = f'show full columns from {db}.{tb}'
    #     data = executor(sql3)
    #     temp_sql = f'db--table--sql: {db}--{tb}-- {sql3}'
    #     write_csv('2.tb_status 表字符集.csv', data, temp_sql)
    #     # print(sql4)
    #     data = executor(sql4)
    #     temp_sql = f'db--table--sql: {db}--{tb}-- {sql4}'
    #     write_csv('2.tb_column 列字符集.csv', data, temp_sql)
    file_name = '2.数据库字符集.csv'
    with ThreadPoolExecutor() as executors:
        futures = [executors.submit(write_csv, file_name, data, temp_sql) for data, temp_sql in db_charsets]
        # 等待所有任务完成
        wait(futures)

get_db_and_charset()


def get_db_obj():
    """
    -- 3、统计数据库对象及个数
SQL> select 'database' type,schema_name db,count(*) cnt from information_schema.SCHEMATA  group by db
     union all
     select 'table' type,table_schema db,count(*) cnt from information_schema.TABLES a
            where table_type='BASE TABLE' group by table_schema
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
    select 'database' type,schema_name db,count(*) cnt from information_schema.SCHEMATA  group by db
     union all
     select 'table' type,table_schema db,count(*) cnt from information_schema.TABLES a
            where table_type='BASE TABLE' group by table_schema
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
    """
    data = executor(sql)
    # print(data)
    temp_sql = f'db--table--sql: -- -- {sql}'
    write_csv('3.db对象及个数.csv', data, temp_sql)


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
        where TABLE_SCHEMA = '{db}' order by table_rows desc;"""
        data = executor(sql)
        # print(data)
        temp_sql = f'db--table--sql: {db}-- -- {sql}'
        write_csv('4.每个表的行数.csv', data, temp_sql)


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
    write_csv('5.表空间大小.csv', data, temp_sql)


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
        write_csv('6.表列名.csv', data, temp_sql)


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
        data = pool.executor(sql)
        # print(data)
        temp_sql = f'db--table--sql: {db}-- -- {sql}'
        write_csv('7.每个库字段类型及个数.csv', data, temp_sql)


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
      o.constraint_schema != 'mysql' and o.constraint_schema != 'sys';
    '''
    data = pool.executor(sql)
    temp_sql = f'db--table--sql: -- -- {sql}'
    write_csv('8.库的主键及外键.csv', data, temp_sql)


def summary():
    """
    库 表 行数
    :return:
    """
    sql = """
select '数据库' type,count(*) cnt from information_schema.SCHEMATA  
     union all
     select '表' type,count(*) cnt from information_schema.TABLES a
            where table_type='BASE TABLE' 
     union all
     select '事件' type,count(*) cnt from information_schema.EVENTS b
            
     union all
     select '触发器' type,count(*) cnt
            from information_schema.TRIGGERS c 
     union all
     select '存储过程' type,count(*) cnt from information_schema.ROUTINES d
            where routine_type='PROCEDURE' 
     union all
     select '函数' type,count(*) cnt from information_schema.ROUTINES e
            where routine_type='FUNCTION' 
     union all
     select '视图' type,count(*) cnt
            from information_schema.views f 
     union all
     select '索引' type,count(distinct index_name) cnt
            from information_schema.STATISTICS g 
     union all
     select '分区表' type,count(*) cnt from information_schema.PARTITIONS p
            where partition_name is not null 
     union all 
     select  '总行数' type, SUM(table_rows) as cnt
     from information_schema.TABLES
     where table_type='BASE TABLE'
     union all 
     select '总表空间大小' type,concat(round(sum((data_length + index_length)/1024/1024),2),'MB') as cnt from
     information_schema.TABLES
    """
    data = pool.executor(sql)
    # print(data)
    temp_sql = f'db--table--sql: -- -- {sql}'
    write_csv('9.汇总信息.csv', data, temp_sql)


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


# try:
#     sql = "show variables like 'innodb_file_per_table';"
# print('数据库表空间模式')
# executor(sql)
# except Exception as e:
#     print("Error:", e)
#
# finally:
#     # 关闭所有连接
#     pool.close_all_connections()

# get_table_space()
# get_db_and_charset()
# get_db_obj()
# count_table_culumns()
# user_table_space()
# get_tb_column()
# get_db_columu_type_and_count()
# get_primary_key_and_foreige_key()
#
# summary()


# 迁移后数据
# migrate_post_db_charset()
def main(task_names):
    print(len(task_names))
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

# if __name__ == "__main__":
#     task_names = [get_table_space, get_db_and_charset, get_db_obj, count_table_culumns, user_table_space, get_tb_column,
#                   get_db_columu_type_and_count, get_primary_key_and_foreige_key, summary]
#     start = time.time()
#     print(start)
#     main(task_names)
#     print(time.time() - start)
