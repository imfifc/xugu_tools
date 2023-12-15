import argparse
import csv
import os
import queue
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import freeze_support
from pathlib import Path
from generate_assess_speed import get_speed
import xgcondb

BASE_PATH = str(Path(__file__).parent)
timestands = time.strftime('%Y年%m月%d日%H%M%S', time.localtime())
dir = f'result_{timestands}'
EXCLUDE_DBS = ['SYSSSO', 'SYSAUDITOR']


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

    def executor(self, data):
        if isinstance(data, tuple):
            sql, tmp_sql = data
            conn = self.get_connection()
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
            except Exception as e:
                print(f'执行异常；{sql},{e}')
                self.release_connection(conn)
                return None
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            rows = [dict(zip(column_names, row)) for row in rows]
            self.release_connection(conn)
            return rows, tmp_sql

        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(data)
        except Exception as e:
            conn.rollback()
            print(f'执行异常；{data},{e}')
            self.release_connection(conn)
            return None
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        conn.commit()
        self.release_connection(conn)
        return [dict(zip(column_names, row)) for row in rows]


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


def get_db_charsets():
    """
    1、查看数据库及数据库的字符集
     select db_name,char_set,time_zone,online from dba_databases
    :return:
    """
    sql = '  select db_name "数据库",char_set "字符集",time_zone "时区",online "在线" from dba_databases '
    data = pool.executor(sql)
    temp_sql = f'db--table--sql: -- dba_databases -- {sql}'
    write_csv('1.数据库字符集.csv', [(data, temp_sql)])


def get_db_object():
    """
    2、统计数据库当前库中用户创建的对象及个数
    :return:
    """
    sql = """
    select 'database' as type, db_name as db,count(*) from dba_databases group by db_name
    union  all
    select 'schemas' as type,d.db_name as db,count(*) from dba_schemas s left join sys_databases d on d.db_id=s.db_id where s.schema_name not in ('SYSSSO','SYSAUDITOR') group by d.db_name
    union  all
    select 'tables' as type,d.db_name as db,count(*) from dba_tables t left join sys_databases d on d.db_id=t.db_id group by d.db_name
    union  all
    select 'views' as type,d.db_name as db,count(*) from dba_views v left join sys_databases d on d.db_id=v.db_id group by d.db_name
    union  all
    select 'sequences' as type,d.db_name as db,count(*) from dba_sequences s left join sys_databases d on d.db_id=s.db_id group by d.db_name
    union  all
    select 'package head' as type,d.db_name as db,count(*) from dba_packages p left join sys_databases d on d.db_id=p.db_id where p.spec is not null group by d.db_name
    union  all
    select 'package body' as type,d.db_name as db,count(*) from dba_packages p left join sys_databases d on d.db_id=p.db_id where p.body is not null group by d.db_name
    union  all
    select 'procedure' as type,d.db_name as db,count(*) from dba_procedures p left join sys_databases d on d.db_id=p.db_id where p.ret_type is null group by d.db_name
    union  all
    select 'function' as type,d.db_name as db,count(*) from dba_procedures p left join sys_databases d on d.db_id=p.db_id where p.ret_type is not null group by d.db_name
    union  all
    select 'trigger' as type,d.db_name as db,count(*) from dba_triggers t left join sys_databases d on d.db_id=t.db_id group by d.db_name
    union  all
    select 'synonym' as type,d.db_name as db,count(*) from dba_synonyms s left join sys_databases d on d.db_id=s.db_id group by d.db_name
    union  all
    select 'udt type' as type,d.db_name as db,count(*) from dba_types t left join sys_databases d on d.db_id=t.db_id group by d.db_name
    union  all
    select 'job' as type,d.db_name as db,count(*) from dba_jobs j left join sys_databases d on d.db_id=j.db_id group by d.db_name
    union  all
    select 'indexes' as type,d.db_name as db,count(*) from dba_indexes i left join sys_databases d on d.db_id=i.db_id group by d.db_name
    union  all
    select 'index_partitions' as type,d.db_name as db,count(*) from dba_idx_partis i left join sys_databases d on d.db_id=i.db_id group by d.db_name
    union  all
    select 'table_partitions' as type,d.db_name as db,count(*) from dba_partis p left join sys_databases d on d.db_id=p.db_id group by d.db_name
    """
    if pool.connection_params.get('database').lower().strip() != 'system':
        sql = """
        select 'database' as type, db_name as db,count(*) from dba_databases group by db_name
        union  all
        select 'schemas' as type,d.db_name as db,count(*) from dba_schemas s left join dba_databases d on d.db_id=s.db_id where s.schema_name not in ('SYSSSO','SYSAUDITOR') group by d.db_name
        union  all
        select 'tables' as type,d.db_name as db,count(*) from dba_tables t left join dba_databases d on d.db_id=t.db_id group by d.db_name
        union  all
        select 'views' as type,d.db_name as db,count(*) from dba_views v left join dba_databases d on d.db_id=v.db_id group by d.db_name
        union  all
        select 'sequences' as type,d.db_name as db,count(*) from dba_sequences s left join dba_databases d on d.db_id=s.db_id group by d.db_name
        union  all
        select 'package head' as type,d.db_name as db,count(*) from dba_packages p left join dba_databases d on d.db_id=p.db_id where p.spec is not null group by d.db_name
        union  all
        select 'package body' as type,d.db_name as db,count(*) from dba_packages p left join dba_databases d on d.db_id=p.db_id where p.body is not null group by d.db_name
        union  all
        select 'procedure' as type,d.db_name as db,count(*) from dba_procedures p left join dba_databases d on d.db_id=p.db_id where p.ret_type is null group by d.db_name
        union  all
        select 'function' as type,d.db_name as db,count(*) from dba_procedures p left join dba_databases d on d.db_id=p.db_id where p.ret_type is not null group by d.db_name
        union  all
        select 'trigger' as type,d.db_name as db,count(*) from dba_triggers t left join dba_databases d on d.db_id=t.db_id group by d.db_name
        union  all
        select 'synonym' as type,d.db_name as db,count(*) from dba_synonyms s left join dba_databases d on d.db_id=s.db_id group by d.db_name
        union  all
        select 'udt type' as type,d.db_name as db,count(*) from dba_types t left join dba_databases d on d.db_id=t.db_id group by d.db_name
        union  all
        select 'job' as type,d.db_name as db,count(*) from dba_jobs j left join dba_databases d on d.db_id=j.db_id group by d.db_name
        union  all
        select 'indexes' as type,d.db_name as db,count(*) from dba_indexes i left join dba_databases d on d.db_id=i.db_id group by d.db_name
        union  all
        select 'index_partitions' as type,d.db_name as db,count(*) from dba_idx_partis i left join dba_databases d on d.db_id=i.db_id group by d.db_name
        union  all
        select 'table_partitions' as type,d.db_name as db,count(*) from dba_partis p left join dba_databases d on d.db_id=p.db_id group by d.db_name
        """
    data = pool.executor(sql)
    temp_sql = f'db--table--sql: --  -- {sql}'
    write_csv('2.数据库对象及个数.csv', [(data, temp_sql)])


def get_db_all_size():
    """
    3.整个集群大小
    :return:
    """
    sql = "select sum(sd.curr_size)/1024 || 'G' from sys_all_tablespaces st join sys_all_datafiles sd on st.space_id=sd.space_id and st.nodeid=sd.nodeid"
    if pool.connection_params.get('database').lower().strip() != 'system':
        sql = "select sum(sd.curr_size)/1024 || 'G' from dba_tablespaces st join dba_datafiles sd on st.space_id=sd.space_id and st.nodeid=sd.nodeid"
    data = pool.executor(sql)
    temp_sql = f'db--table--sql: --  -- {sql}'
    write_csv('3.集群大小.csv', [(data, temp_sql)])
    sql2 = "select sd.nodeid,sum(sd.curr_size) || 'M' from sys_all_tablespaces st join sys_all_datafiles sd on st.space_id=sd.space_id and st.nodeid=sd.nodeid  group by sd.nodeid"
    if pool.connection_params.get('database').lower().strip() != 'system':
        sql2 = "select sd.nodeid,sum(sd.curr_size)/1024 || 'G' from dba_tablespaces st join dba_datafiles sd on st.space_id=sd.space_id and st.nodeid=sd.nodeid  group by sd.nodeid"
    temp_sql2 = f'db--table--sql: --  -- {sql2}'
    data2 = pool.executor(sql)
    write_csv('3.集群个节点大小.csv', [(data2, temp_sql2)])


def get_db_size():
    """
    4. 查看数据库大小
    :return:
    """
    sql = "select db_name, count(*)*8 ||'MB' as cnt  from sys_gstores gs left join sys_databases d on d.db_id=gs.db_id  group by d.db_name"
    if pool.connection_params.get('database').lower().strip() != 'system':
        return None
    data = pool.executor(sql)
    temp_sql = f'db--table--sql: --  -- {sql}'
    write_csv('4.库大小.csv', [(data, temp_sql)])


def get_table_size(is_speed=True):
    """
    5.表大小
    :return:
    """
    sql = """
    select d.db_name,s.schema_name,t.table_name,count(*)*8 as "大小(MB)",count(*)*8*1 as "迁移后大小(MB)" from sys_schemas s,sys_tables t,sys_gstores g,sys_databases d 
    where g.obj_id=t.table_id and s.schema_id=t.schema_id and s.db_id=t.db_id and g.db_id=d.db_id  and t.db_id=d.db_id 
    group by t.table_name,s.schema_name,d.db_name order by d.db_name,s.schema_name,count(*) desc; 
    """
    if pool.connection_params.get('database').lower().strip() != 'system':
        return None
    data = pool.executor(sql)
    temp_sql = f'db--table--sql: --  -- {sql}'
    if is_speed:
        speed = get_speed()
        # print(speed)
        for i in data:
            i.update({
                '迁移所需时间(秒)': round(i.get('迁移后大小(MB)') / speed, 2)
            })

    # print(data)
    write_csv('5.查看表大小.csv', [(data, temp_sql)])
    return True


def get_table_nums():
    """
    6.表行数
    select db_name "库",schema_name "模式",table_name "表" , a.row_num "行数" from sys_tables t  join sys_schemas s on t.schema_id=s.schema_id and t.db_id=s.db_id inner join sys_databases d on t.db_id=d.db_id
    left join (select gsto_no,row_num from sys_stores) a on a.gsto_no=t.gsto_no
    order by db_name,schema_name,a.row_num desc
    :return:
    """
    sql = """
    select db_name "库",schema_name "模式",table_name "表" , a.row_num "行数" from sys_tables t  join sys_schemas s on t.schema_id=s.schema_id and t.db_id=s.db_id inner join sys_databases d on t.db_id=d.db_id 
    left join (select gsto_no,row_num from sys_stores) a on a.gsto_no=t.gsto_no 
    order by db_name,schema_name,a.row_num desc
    """
    if pool.connection_params.get('database').lower().strip() != 'system':
        return None
    data = pool.executor(sql)
    temp_sql = f'db--table--sql: --  -- {sql}'
    write_csv('6.表行数.csv', [(data, temp_sql)])


def get_foreign_key_and_primary_key():
    """
    --    7.主键和外键信息
    select ind.db_id,sch.schema_name,obj.obj_name ,ind.cons_name,
    case when cons_type='f' then '外键'
    when cons_type='r' then '引用外键'
    when cons_type='c' then '值检查'
    when cons_type='d' then '默认值'
    when cons_type='u' then '唯一值'
    when cons_type='p' then '主键'
    end cons_type
    from sys_constraints ind,sys_objects obj,sys_schemas sch
    where ind.table_id=obj.obj_id
    and ind.db_id=obj.db_id
    and  obj.schema_id=sch.schema_id
    and  obj.db_id=sch.db_id
    :return:
    """
    sql = """
    select
        db.db_name as 数据库名,
        ch.schema_name as 模式名,
        t.table_name as 表名,
        index_name as 索引名,
        (case index_type
              when 0 then 'btree'
          when 1 then 'rtree'
          when 2 then 'fulltext'
          when 3 then 'bitmap'
          when 4 then 'union'
        end) as 索引类型,
        is_primary as 是否是主键索引,
        is_unique as 是否是唯一索引,
        field_num as 索引字段数,
        replace(keys,  '"',  '') as 索引字段
      from
        dba_indexes as c
      left join dba_tables t on
        c.table_id = t.table_id
      left join dba_databases db on
        c.db_id = db.db_id
      left join dba_schemas ch on
        ch.user_id = t.user_id and ch.db_id = db.db_id
    """
    data = pool.executor(sql)
    temp_sql = f'db--table--sql: --  -- {sql}'
    write_csv('7.主键及外键信息.csv', [(data, temp_sql)])


def summary():
    """
    8.概要信息
    select '数据库版本' type,version cnt
    union all
    select '数据库' type,count(*)::varchar from sys_databases
    union all
    select '当前库模式' type,count(*) ::varchar cnt from dba_schemas s left join sys_databases d on d.db_id=s.db_id where s.schema_name not in ('SYSSSO','SYSAUDITOR')
    union all
    select '表' type,count(*)::varchar cnt from  dba_tables t left join sys_databases d on d.db_id=t.db_id
    union all
    select '触发器' type,count(*)::varchar cnt from dba_triggers t left join sys_databases d on d.db_id=t.db_id group by d.db_name
    union all
    select '存储过程' type,count(*)::varchar cnt from dba_procedures p left join sys_databases d on d.db_id=p.db_id where p.ret_type is null group by d.db_name
    union all
    select '函数' type,count(*)::varchar cnt from dba_procedures p left join sys_databases d on d.db_id=p.db_id where p.ret_type is not null group by d.db_name
    union all
    select '视图' type,count(*)::varchar cnt from dba_views v left join sys_databases d on d.db_id=v.db_id group by d.db_name
    union all
    select '索引' type,count(*)::varchar cnt from  dba_indexes i left join sys_databases d on d.db_id=i.db_id group by d.db_name
    union all
    select '分区表' type,count(*)::varchar cnt from dba_partis p left join sys_databases d on d.db_id=p.db_id group by d.db_name
    union all
    select   '时区' type, var_value cnt from sys_vars where var_name ='def_timezone'
    union all
    SELECT '用户' type ,count(*)::varchar  cnt FROM dba_users
    union all
    select  '当前库总行数' type,  sum(s.row_num)::varchar cnt from dba_tables t join (select gsto_no,row_num from sys_stores) s on s.gsto_no=t.gsto_no
    union all
    select  '当前库大小' type,  count(*)*8 ||'MB' as cnt  from sys_gstores gs left join sys_databases d on d.db_id=gs.db_id  where db_name=current_database
    union all
    select  '集群大小' type, sum(sd.curr_size)/1024 || 'G' cnt from sys_all_tablespaces st join sys_all_datafiles sd on st.space_id=sd.space_id and st.nodeid=sd.nodeid
    :return:
    """
    sql = """
    select '数据库版本' type,version cnt
    union all
    select '数据库' type,count(*)::varchar from sys_databases
    union all
    select '当前库模式' type,count(*) ::varchar cnt from dba_schemas s left join sys_databases d on d.db_id=s.db_id 
    union all
    select '表' type,count(*)::varchar cnt from  dba_tables t left join sys_databases d on d.db_id=t.db_id
    union all
    select '触发器' type,count(*)::varchar cnt from dba_triggers t left join sys_databases d on d.db_id=t.db_id group by d.db_name
    union all
    select '存储过程' type,count(*)::varchar cnt from dba_procedures p left join sys_databases d on d.db_id=p.db_id where p.ret_type is null group by d.db_name
    union all
    select '函数' type,count(*)::varchar cnt from dba_procedures p left join sys_databases d on d.db_id=p.db_id where p.ret_type is not null group by d.db_name
    union all
    select '视图' type,count(*)::varchar cnt from dba_views v left join sys_databases d on d.db_id=v.db_id group by d.db_name
    union all
    select '索引' type,count(*)::varchar cnt from  dba_indexes i left join sys_databases d on d.db_id=i.db_id group by d.db_name
    union all
    select '分区表' type,count(*)::varchar cnt from dba_partis p left join sys_databases d on d.db_id=p.db_id group by d.db_name
    union all
    select   '时区' type, var_value cnt from sys_vars where var_name ='def_timezone'
    union all
    SELECT '用户' type ,count(*)::varchar  cnt FROM dba_users
    union all
    select  '当前库总行数' type,  sum(s.row_num)::varchar cnt from dba_tables t join (select gsto_no,row_num from sys_stores) s on s.gsto_no=t.gsto_no
    union all
    select  '当前库大小' type,  count(*)*8 ||'MB' as cnt  from sys_gstores gs left join sys_databases d on d.db_id=gs.db_id  where db_name=current_database
    union all  
    select  '集群大小' type, sum(sd.curr_size)/1024 || 'G' cnt from sys_all_tablespaces st join sys_all_datafiles sd on st.space_id=sd.space_id and st.nodeid=sd.nodeid
    """
    if pool.connection_params.get('database').lower().strip() != 'system':
        sql = """
            select '数据库版本' type,version cnt
            union all
            select '数据库' type,count(*)::varchar from dba_databases
            union all
            select '当前库模式' type,count(*) ::varchar cnt from dba_schemas s left join dba_databases d on d.db_id=s.db_id 
            union all
            select '表' type,count(*)::varchar cnt from  dba_tables t left join dba_databases d on d.db_id=t.db_id
            union all
            select '触发器' type,count(*)::varchar cnt from dba_triggers t left join dba_databases d on d.db_id=t.db_id group by d.db_name
            union all
            select '存储过程' type,count(*)::varchar cnt from dba_procedures p left join dba_databases d on d.db_id=p.db_id where p.ret_type is null group by d.db_name
            union all
            select '函数' type,count(*)::varchar cnt from dba_procedures p left join dba_databases d on d.db_id=p.db_id where p.ret_type is not null group by d.db_name
            union all
            select '视图' type,count(*)::varchar cnt from dba_views v left join dba_databases d on d.db_id=v.db_id group by d.db_name
            union all
            select '索引' type,count(*)::varchar cnt from  dba_indexes i left join dba_databases d on d.db_id=i.db_id group by d.db_name
            union all
            select '分区表' type,count(*)::varchar cnt from dba_partis p left join dba_databases d on d.db_id=p.db_id group by d.db_name
            union all 
            SELECT '用户' type ,count(*)::varchar  cnt FROM dba_users
            union all
            select  '集群大小' type, sum(sd.curr_size)/1024 || 'G' cnt from dba_tablespaces st join dba_datafiles sd on st.space_id=sd.space_id and st.nodeid=sd.nodeid
        """
    data = pool.executor(sql)
    for i in data:
        print(f"{i.get('TYPE')}: {i.get('CNT')}")
    # print(data)
    temp_sql = f'db--table--sql: -- -- {sql}'
    write_csv('8.汇总信息.csv', [(data, temp_sql)])


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
    __  ___   _  __ _ _   _ 
    \ \/ / | | |/ _` | | | |
     >  <| |_| | (_| | |_| |
    /_/\_\\__,_|\__, |\__,_|
                |___/             power by xugu  v1.0.0
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
    if host and port and user and password and db:
        print(f'host: {host} port: {port} user: {user} password: {password} db: {db} \n')

    return host, port, user, password, db


if __name__ == '__main__':
    if sys.platform == 'win32':
        freeze_support()  # linux 不需要
    # host = '127.0.0.1'
    # port = 5138
    # user = 'SYSDBA'
    # password = 'SYSDBA'
    # db = 'SYSTEM'
    # db_charset = 'utf8'
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
    tasknames = [get_db_charsets, get_db_object, get_db_all_size, get_db_size, get_table_nums,
                 get_foreign_key_and_primary_key]
    main(tasknames)
    res = get_table_size(is_speed=False)
    summary()
    print(f'耗时: {time.time() - start:.2f} 秒')
    if res:
        is_speed = input('\n请确定是否需要评估迁移时间(默认否) Y/N : ')
        if is_speed.lower().strip() == 'y':
            get_table_size(is_speed=True)
    input('\nPress Enter to exit…')
