import argparse
import sys
from concurrent.futures import ThreadPoolExecutor

import cx_Oracle
import csv
import os
import time

EXCEPT_USERS = ['SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM', 'DBSNMP', 'APPQOSSYS', 'WMSYS']


class OracleConnectionPool:
    def __init__(self, user, password, dsn, min_connections=2, max_connections=100):
        self.user = user
        self.password = password
        self.dsn = dsn
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.pool = None

    def create_pool(self):
        self.pool = cx_Oracle.SessionPool(
            self.user, self.password, self.dsn,
            min=self.min_connections, max=self.max_connections,
            increment=1, threaded=True, encoding="UTF-8",
            getmode=cx_Oracle.SPOOL_ATTRVAL_WAIT
        )

    def get_connection(self):
        if self.pool is None:
            self.create_pool()
        connection = self.pool.acquire()
        return connection

    def release_connection(self, connection):
        self.pool.release(connection)

    def close_pool(self):
        if self.pool is not None:
            self.pool.close()

    def executor(self, sql):
        conn = self.get_connection()
        with conn.cursor() as cursor:
            try:
                cursor.execute(sql)
            except Exception as e:
                conn.rollback()
                print(f'执行异常:{sql},{e}')
                self.release_connection(conn)
                return None
            column_names = [desc[0] for desc in cursor.description]
            results = [dict(zip(column_names, row)) for row in cursor.fetchall()]
        conn.commit()
        self.release_connection(conn)
        return results


# def use_privilege_conn(sql):
#     """
#     使用sysdba 模式
#     :param sql:
#     :return:
#     """
#     with cx_Oracle.connect(pre_user, pre_password, dsn, mode=cx_Oracle.SYSDBA) as conn:
#         cur = conn.cursor()
#         try:
#             cur.execute(sql)
#             # cur.rowfactory = make_dict_factory(cur)
#             columns = [col[0] for col in cur.description]
#             cur.rowfactory = lambda *args: dict(zip(columns, args))
#             data = cur.fetchall()
#             # print(data)
#             return data
#         except Exception as e:
#             print("查询失败", e)
#         conn.commit()


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


def check_dba_privelege():
    """
    查询当前用户有哪些权限
    DBA
    RESOURCE
    CONNECT
    """
    sql = "select granted_role from user_role_privs"
    res = pool.executor(sql)
    res = [v.lower() for i in res for _, v in i.items()]
    # print(res)
    if 'dba' in res:
        return True
    return False


def get_databases():
    sql_user = "select username from dba_users WHERE ACCOUNT_STATUS ='OPEN' order by username"
    users = pool.executor(sql_user)
    users = [i.get('USERNAME') for i in users]
    return users


def get_charset():
    """
    -- 1、查询数据库编码格式
    SQL> select * from v$nls_parameters a where a.PARAMETER='NLS_CHARACTERSET';
    :return:
    """
    sql = "select * from v$nls_parameters a where a.PARAMETER='NLS_CHARACTERSET'"
    data = pool.executor(sql)
    temp_sql = f'schema--table--sql: -- -- {sql}'
    write_csv('1.编码格式.csv', [(data, temp_sql)])


def get_objects_count():
    """
    -- 2、统计数据库中的对象及个数
-- 查看指定用户下的各对象类型和数目，例如用户名为 USER
SQL>  select object_type,count(*) from all_objects where owner='USER' group by object_type;
    :return:
    """
    if check_dba_privelege():
        dba_user = "SELECT username FROM dba_users WHERE  username !='SYS' and  username !='SYSTEM' and account_status = 'OPEN' order by username"
        # sql_user = "select username from dba_users WHERE ACCOUNT_STATUS ='OPEN' order by username"
        users = pool.executor(dba_user)
        users = [v for i in users for _, v in i.items()]
        db_objects = []
        for user in users:
            sql = f"""select object_type "对象类型",count(*) "个数" from dba_objects where owner='{user}' group by object_type order by count(*) desc """
            res = pool.executor(sql)
            tmp_sql = f'schema--table--sql: {user}--dba_objects-- {sql}'
            db_objects.append((res, tmp_sql))
    else:
        sql_user = "SELECT username FROM all_users  WHERE  username not in ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM','DBSNMP','APPQOSSYS', 'ORDSYS', 'EXFSYS', 'WMSYS', 'CTXSYS', 'ANONYMOUS', 'CTXSYS', 'MDSYS', 'LBACSYS', 'DMSYS', 'TSMSYS', 'OLAPSYS')"
        users = pool.executor(sql_user)
        users = [v for i in users for _, v in i.items() if v not in EXCEPT_USERS]
        db_objects = []
        for user in users:
            sql = f"""select object_type "对象类型",count(*) "个数"  from all_objects where owner='{user}' group by object_type"""
            res = pool.executor(sql)
            tmp_sql = f'schema--table--sql: {user}--dba_objects-- {sql}'
            db_objects.append((res, tmp_sql))

    file_name1 = '2.数据库对象及个数.csv'
    write_csv(file_name1, db_objects)


def get_table_statistic():
    """
    -- 3、统计表的数据量
    -- 首先分析该模式的所有表，手动更新dba_tables的统计信息
    SQL> BEGIN
                    FOR i IN (SELECT table_name FROM dba_tables WHERE owner='USER') LOOP
                      EXECUTE IMMEDIATE ('analyze table '|| i.table_name ||' compute statistics');
                    END LOOP;
             END;
             /
    -- 统计各表的数据量
    SQL> SELECT table_name,num_rows FROM dba_tables WHERE OWNER='USER';

    SELECT table_name "表名",num_rows "行数" FROM dba_tables WHERE OWNER='SYS'and num_rows is not null order by num_rows desc
    :return:
    """

    dba_sql = '''
    SELECT owner "模式名" ,table_name "表名",num_rows "行数" FROM dba_tables WHERE  owner IN (SELECT username FROM dba_users 
    WHERE  username !='SYS' and  username !='SYSTEM' and account_status = 'OPEN')  order by owner,num_rows desc
    '''
    common_sql = '''
    SELECT owner "模式名" ,table_name "表名",num_rows "行数" FROM all_tables 
    WHERE  owner not in ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM','DBSNMP','APPQOSSYS', 'ORDSYS', 'EXFSYS', 'WMSYS', 'CTXSYS', 'ANONYMOUS', 'CTXSYS', 'MDSYS', 'LBACSYS', 'DMSYS', 'TSMSYS', 'OLAPSYS')
    and num_rows is not null order by owner,num_rows desc
    '''
    if check_dba_privelege():
        data = pool.executor(dba_sql)
        temp_sql = f'schema--table--sql: --dba_tables-- {dba_sql}'
    else:
        data = pool.executor(common_sql)
        temp_sql = f'schema--table--sql: --dba_tables-- {common_sql}'
    file_name1 = '3.表及行数.csv'
    write_csv(file_name1, [(data, temp_sql)])


def get_schema_space():
    """
    -- 4、统计所有用户下的表对象使用空间大小
SELECT OWNER, concat(round(sum(bytes)/1024/1024,2),'MB') AS data FROM dba_segments
       WHERE SEGMENT_TYPE LIKE 'TABLE%' GROUP BY OWNER ORDER BY round(sum(bytes)/1024/1024,2) desc;
    :return:
    """
    dba_sql = """
    SELECT OWNER as "用户/模式", concat(round(sum(bytes)/1024/1024,2),'MB') AS "大小" FROM dba_segments WHERE  owner IN (SELECT username FROM dba_users 
      WHERE  username !='SYS' and  username !='SYSTEM' and account_status = 'OPEN') and SEGMENT_TYPE LIKE 'TABLE%' GROUP BY OWNER ORDER BY round(sum(bytes)/1024/1024,2) desc
    """
    common_sql = '''
        SELECT '当前用户/模式' "type", concat(round(sum(bytes)/1024/1024,2),'MB') AS "大小" FROM user_segments
	    WHERE SEGMENT_TYPE LIKE 'TABLE%'  ORDER BY round(sum(bytes)/1024/1024,2) desc
	 '''
    if check_dba_privelege():
        data = pool.executor(dba_sql)
        temp_sql = f'schema--table--sql: --dba_segments-- {dba_sql}'
    else:
        data = pool.executor(common_sql)
        temp_sql = f'schema--table--sql: --dba_segments-- {common_sql}'
    write_csv('4.每个模式的大小.csv', [(data, temp_sql)])


def get_table_space():
    """
    获取用户模式下的表大小
    :return:
    """
    dba_sql = """
    SELECT OWNER as "模式", segment_name as "表名",concat(round(sum(bytes)/1024/1024,2),'MB') as "表大小"
    FROM dba_segments
    WHERE SEGMENT_NAME NOT LIKE 'BIN$%' and  SEGMENT_TYPE LIKE 'TABLE%' 
    AND OWNER  IN (SELECT username FROM dba_users  WHERE  username !='SYS' and  username !='SYSTEM' and account_status = 'OPEN')
    GROUP BY OWNER ,segment_name order by owner, round(sum(bytes)/1024/1024,2) desc
    """
    common_sql = '''
   	SELECT  segment_name as "表名",concat(round(sum(bytes)/1024/1024,2),'MB') as "表大小"
    FROM user_segments
    WHERE SEGMENT_NAME NOT LIKE 'BIN$%' and  SEGMENT_TYPE LIKE 'TABLE%'  group by segment_name
    order by  round(sum(bytes)/1024/1024,2) desc
    '''
    if check_dba_privelege():
        data = pool.executor(dba_sql)
        temp_sql = f'schema--table--sql: --dba_segments-- {dba_sql}'
    else:
        data = pool.executor(common_sql)
        temp_sql = f'schema--table--sql: --user_segments-- {common_sql}'

    write_csv('9.每张表大小.csv', [(data, temp_sql)])


def get_table_column():
    """
    -- 5、查看数据库表对象使用的列名称(关键字排查)
    SQL> SELECT  column_name FROM dba_tab_columns WHERE owner='USER'
     GROUP BY column_name ORDER BY column_name desc;
    :return:
    """
    dba_sql = '''
    SELECT distinct column_name FROM dba_tab_columns WHERE owner in (SELECT username FROM dba_users 
    WHERE  username !='SYS' and  username !='SYSTEM' and account_status = 'OPEN')  ORDER BY column_name
    '''
    common_sql = '''
    SELECT distinct column_name FROM all_tab_columns WHERE owner in (SELECT username FROM all_users 
    WHERE  username not in  ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM','DBSNMP','APPQOSSYS', 'ORDSYS', 'EXFSYS', 'WMSYS', 'CTXSYS', 'ANONYMOUS', 'CTXSYS', 'MDSYS', 'LBACSYS', 'DMSYS', 'TSMSYS', 'OLAPSYS'))
	ORDER BY column_name
    '''

    if check_dba_privelege():
        data = pool.executor(dba_sql)
        temp_sql = f'schema--table--sql: --dba_tab_columns-- {dba_sql}'
    else:
        data = pool.executor(common_sql)
        temp_sql = f'schema--table--sql: --all_tab_columns-- {common_sql}'
    write_csv('5.表列名.csv', [(data, temp_sql)])


def get_table_column_type():
    """
    -- 6、查看数据库表对象使用的数据类型
    SQL> SELECT  DATA_TYPE,count(*) FROM dba_tab_columns WHERE OWNER='USER' GROUP BY DATA_TYPE ORDER BY  COUNT(*) desc;
    :return:
    """
    dba_sql = """
    SELECT  DATA_TYPE "数据类型",count(*) "个数" FROM dba_tab_columns WHERE owner in (SELECT username FROM dba_users 
    WHERE  username !='SYS' and  username !='SYSTEM' and account_status = 'OPEN')  GROUP BY DATA_TYPE ORDER BY count(*) desc
    """
    common_sql = """
	SELECT  DATA_TYPE "数据类型",count(*) "个数" FROM all_tab_columns WHERE  owner in (SELECT username FROM all_users 
    WHERE  username not in ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM','DBSNMP','APPQOSSYS', 'ORDSYS', 'EXFSYS', 'WMSYS', 'CTXSYS', 'ANONYMOUS', 'CTXSYS', 'MDSYS', 'LBACSYS', 'DMSYS', 'TSMSYS', 'OLAPSYS')) 
    GROUP BY DATA_TYPE ORDER BY count(*) desc
    """
    if check_dba_privelege():
        data = pool.executor(dba_sql)
        temp_sql = f'schema--table--sql: --dba_tab_columns-- {dba_sql}'
    else:
        data = pool.executor(common_sql)
        temp_sql = f'schema--table--sql: --all_tab_columns-- {common_sql}'

    write_csv('6.表列字段类型及数量.csv', [(data, temp_sql)])


def get_constraint():
    """
    -- 7、查询创建用户模式下的主键信息和外键信息
    SQL> SELECT  c.OWNER AS 约束拥有者,
                    c.CONSTRAINT_name AS 约束名称,
                    c.TABLE_NAME AS 表名,
                    cc.COLUMN_NAME AS 列名,
                    c.R_OWNER AS 外键表拥有者,
                    d.TABLE_NAME AS 外键表名,
                    dd.COLUMN_NAME AS 外键表列名,
                    c.DELETE_RULE AS 约束删除规则,
                    c.R_CONSTRAINT_NAME AS 唯一约束名称,
                    c.CONSTRAINT_TYPE AS 约束类型,
                    c.STATUS AS 约束状态
             FROM dba_constraints c LEFT JOIN DBA_CONS_COLUMNS cc ON c.OWNER=cc.OWNER AND c.CONSTRAINT_NAME=cc.CONSTRAINT_NAME
             LEFT JOIN dba_constraints d  ON c.R_OWNER=d.OWNER AND c.R_CONSTRAINT_NAME=d.CONSTRAINT_NAME
             LEFT JOIN DBA_CONS_COLUMNS dd ON d.OWNER=dd.OWNER AND d.CONSTRAINT_NAME=dd.CONSTRAINT_NAME
             WHERE c.OWNER IN (
             SELECT username FROM dba_users WHERE created>=(SELECT created FROM v$database) AND common='NO'   AND username!='HR')
           ORDER BY c.OWNER;
        :return:
    """
    dba_sql = """
    SELECT  c.OWNER AS 约束拥有者,
            c.CONSTRAINT_name AS 约束名称,
            c.TABLE_NAME AS 表名,
            cc.COLUMN_NAME AS 列名,
            c.R_OWNER AS 外键表拥有者,
            d.TABLE_NAME AS 外键表名,
            cc.COLUMN_NAME AS 外键表列名,
            c.DELETE_RULE AS 约束删除规则,
            c.R_CONSTRAINT_NAME AS 唯一约束名称,
            c.CONSTRAINT_TYPE AS 约束类型,
            c.STATUS AS 约束状态
     FROM dba_constraints c 
     LEFT JOIN dba_cons_columns cc ON c.OWNER=cc.OWNER AND c.CONSTRAINT_NAME=cc.CONSTRAINT_NAME
     LEFT JOIN dba_constraints d  ON c.R_OWNER=d.OWNER AND c.R_CONSTRAINT_NAME=d.CONSTRAINT_NAME
     WHERE c.OWNER IN (
      SELECT username FROM dba_users WHERE username !='SYS' and  username !='SYSTEM' and account_status = 'OPEN' and created>=(SELECT created FROM v$database) AND username!='HR')
    ORDER BY c.OWNER
    """
    common_sql = """
    SELECT  c.OWNER AS 约束拥有者,
            c.CONSTRAINT_name AS 约束名称,
            c.TABLE_NAME AS 表名,
            cc.COLUMN_NAME AS 列名,
            c.R_OWNER AS 外键表拥有者,
            d.TABLE_NAME AS 外键表名,
            cc.COLUMN_NAME AS 外键表列名,
            c.DELETE_RULE AS 约束删除规则,
            c.R_CONSTRAINT_NAME AS 唯一约束名称,
            c.CONSTRAINT_TYPE AS 约束类型,
            c.STATUS AS 约束状态
     FROM all_constraints c 
     LEFT JOIN all_cons_columns cc ON c.OWNER=cc.OWNER AND c.CONSTRAINT_NAME=cc.CONSTRAINT_NAME
     LEFT JOIN all_constraints d  ON c.R_OWNER=d.OWNER AND c.R_CONSTRAINT_NAME=d.CONSTRAINT_NAME
     WHERE c.OWNER IN (
     SELECT username FROM all_users WHERE  username not in ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM','DBSNMP','APPQOSSYS', 'ORDSYS', 'EXFSYS', 'WMSYS', 'CTXSYS', 'ANONYMOUS', 'CTXSYS', 'MDSYS', 'LBACSYS', 'DMSYS', 'TSMSYS', 'OLAPSYS'))
    ORDER BY c.OWNER
    """
    if check_dba_privelege():
        data = pool.executor(dba_sql)
        temp_sql = f'schema--table--sql: -- -- {dba_sql}'
    else:
        data = pool.executor(common_sql)
        temp_sql = f'schema--table--sql: -- -- {common_sql}'
    write_csv('7.每个用户模式下的表约束.csv', [(data, temp_sql)])


def sumary():
    dba_sql = """

    SELECT '数据库版本' type, banner AS cnt FROM v$version WHERE banner LIKE 'Oracle%'
    UNION ALL
    SELECT '用户模式' type, to_char(COUNT(*)) AS cnt FROM dba_users 
		 where username in (SELECT username FROM dba_users WHERE username !='SYS' and  username !='SYSTEM' and account_status = 'OPEN')
    UNION ALL
    select '用户' type ,TO_CHAR(count(*)) cnt from dba_users 
		 where username in (SELECT username FROM dba_users WHERE username !='SYS' and  username !='SYSTEM' and account_status = 'OPEN')
    UNION ALL
    select '表' type,to_char(count(*)) cnt  from dba_tables  
		 where owner in  (SELECT username FROM dba_users WHERE username !='SYS' and  username !='SYSTEM' and account_status = 'OPEN')
    UNION ALL
    SELECT '定时作业' type ,TO_CHAR(count(*)) cnt FROM dba_scheduler_jobs
				 where owner in  (SELECT username FROM dba_users WHERE username !='SYS' and  username !='SYSTEM' and account_status = 'OPEN')
    UNION ALL
    SELECT '函数' type ,TO_CHAR(count(*)) cnt FROM dba_objects WHERE object_type = 'FUNCTION'
						 and  owner in  (SELECT username FROM dba_users WHERE username !='SYS' and  username !='SYSTEM' and account_status = 'OPEN')
    UNION ALL
    SELECT '存储过程' type ,TO_CHAR(count(*)) cnt FROM dba_objects WHERE object_type = 'PROCEDURE'
						 and owner in  (SELECT username FROM dba_users WHERE username !='SYS' and  username !='SYSTEM' and account_status = 'OPEN')
    UNION ALL
    SELECT '触发器' type ,TO_CHAR(count(*)) cnt FROM dba_triggers
								 where owner in  (SELECT username FROM dba_users WHERE username !='SYS' and  username !='SYSTEM' and account_status = 'OPEN')
    UNION ALL
    SELECT '视图' type ,TO_CHAR(count(*)) cnt  FROM dba_views
		where owner in  (SELECT username FROM dba_users WHERE username !='SYS' and  username !='SYSTEM' and account_status = 'OPEN')
    UNION ALL
    SELECT '索引' type ,TO_CHAR(count(*))  FROM dba_indexes
		where owner in  (SELECT username FROM dba_users WHERE username !='SYS' and  username !='SYSTEM' and account_status = 'OPEN')
    UNION ALL
    SELECT '总表空间大小' type, SUM(bytes) / (1024 * 1024) || 'MB' AS size_mb FROM dba_segments
		where owner in  (SELECT username FROM dba_users WHERE username !='SYS' and  username !='SYSTEM' and account_status = 'OPEN')
    UNION ALL
    SELECT '所有表总行数' type ,TO_CHAR(sum(num_rows)) cnt FROM dba_tables
			 where owner in  (SELECT username FROM dba_users WHERE username !='SYS' and  username !='SYSTEM' and account_status = 'OPEN')
    UNION ALL
    SELECT '分区表' type ,TO_CHAR(count(*)) FROM dba_part_tables
		where owner in  (SELECT username FROM dba_users WHERE username !='SYS' and  username !='SYSTEM' and account_status = 'OPEN')
    UNION ALL
    SELECT 'DB时区' type,DBTIMEZONE cnt FROM dual
    UNION ALL
    SELECT 'session时区' type,SESSIONTIMEZONE cnt FROM dual
    """
    common_sql = """
		    SELECT '数据库版本' type, banner AS cnt FROM v$version WHERE banner LIKE 'Oracle%'
    UNION ALL
    SELECT '用户模式' type, to_char(COUNT(*)) AS cnt FROM all_users  WHERE  username not in ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM','DBSNMP','APPQOSSYS', 'ORDSYS', 'EXFSYS', 'WMSYS', 'CTXSYS', 'ANONYMOUS', 'CTXSYS', 'MDSYS', 'LBACSYS', 'DMSYS', 'TSMSYS', 'OLAPSYS')
    UNION ALL
    select '用户' type ,TO_CHAR(count(*)) cnt from all_users WHERE  username not in ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM','DBSNMP','APPQOSSYS', 'ORDSYS', 'EXFSYS', 'WMSYS', 'CTXSYS', 'ANONYMOUS', 'CTXSYS', 'MDSYS', 'LBACSYS', 'DMSYS', 'TSMSYS', 'OLAPSYS')
    UNION ALL
    select '表' type,to_char(count(*)) cnt  from all_tables where owner not in ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM','DBSNMP','APPQOSSYS', 'ORDSYS', 'EXFSYS', 'WMSYS', 'CTXSYS', 'ANONYMOUS', 'CTXSYS', 'MDSYS', 'LBACSYS', 'DMSYS', 'TSMSYS', 'OLAPSYS')
    UNION ALL
    SELECT '定时作业' type ,TO_CHAR(count(*)) cnt FROM all_scheduler_jobs where owner not in ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM','DBSNMP','APPQOSSYS', 'ORDSYS', 'EXFSYS', 'WMSYS', 'CTXSYS', 'ANONYMOUS', 'CTXSYS', 'MDSYS', 'LBACSYS', 'DMSYS', 'TSMSYS', 'OLAPSYS')
    UNION ALL
    SELECT '函数' type ,TO_CHAR(count(*)) cnt FROM all_objects WHERE object_type = 'FUNCTION' and owner not in ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM','DBSNMP','APPQOSSYS', 'ORDSYS', 'EXFSYS', 'WMSYS', 'CTXSYS', 'ANONYMOUS', 'CTXSYS', 'MDSYS', 'LBACSYS', 'DMSYS', 'TSMSYS', 'OLAPSYS')
    UNION ALL
    SELECT '存储过程' type ,TO_CHAR(count(*)) cnt FROM all_objects WHERE object_type = 'PROCEDURE' and owner not in ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM','DBSNMP','APPQOSSYS', 'ORDSYS', 'EXFSYS', 'WMSYS', 'CTXSYS', 'ANONYMOUS', 'CTXSYS', 'MDSYS', 'LBACSYS', 'DMSYS', 'TSMSYS', 'OLAPSYS')
    UNION ALL
    SELECT '触发器' type ,TO_CHAR(count(*)) cnt FROM all_triggers where owner not in ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM','DBSNMP','APPQOSSYS', 'ORDSYS', 'EXFSYS', 'WMSYS', 'CTXSYS', 'ANONYMOUS', 'CTXSYS', 'MDSYS', 'LBACSYS', 'DMSYS', 'TSMSYS', 'OLAPSYS')
    UNION ALL
    SELECT '视图' type ,TO_CHAR(count(*)) cnt  FROM all_views where owner not in ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM','DBSNMP','APPQOSSYS', 'ORDSYS', 'EXFSYS', 'WMSYS', 'CTXSYS', 'ANONYMOUS', 'CTXSYS', 'MDSYS', 'LBACSYS', 'DMSYS', 'TSMSYS', 'OLAPSYS')
    UNION ALL
    SELECT '索引' type ,TO_CHAR(count(*))  FROM all_indexes where owner not in ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM','DBSNMP','APPQOSSYS', 'ORDSYS', 'EXFSYS', 'WMSYS', 'CTXSYS', 'ANONYMOUS', 'CTXSYS', 'MDSYS', 'LBACSYS', 'DMSYS', 'TSMSYS', 'OLAPSYS')
    UNION ALL
    SELECT '总表空间大小' type, SUM(bytes) / (1024 * 1024) || 'MB' AS size_mb FROM user_segments
    UNION ALL
    SELECT '所有表总行数' type ,TO_CHAR(sum(num_rows)) cnt FROM all_tables where owner not in ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM','DBSNMP','APPQOSSYS', 'ORDSYS', 'EXFSYS', 'WMSYS', 'CTXSYS', 'ANONYMOUS', 'CTXSYS', 'MDSYS', 'LBACSYS', 'DMSYS', 'TSMSYS', 'OLAPSYS')
    UNION ALL
    SELECT '分区表' type ,TO_CHAR(count(*)) FROM all_part_tables where owner not in ('SYS', 'SYSTEM', 'OUTLN', 'DIP', 'ORACLE_OCM','DBSNMP','APPQOSSYS', 'ORDSYS', 'EXFSYS', 'WMSYS', 'CTXSYS', 'ANONYMOUS', 'CTXSYS', 'MDSYS', 'LBACSYS', 'DMSYS', 'TSMSYS', 'OLAPSYS')
    UNION ALL
    SELECT 'DB时区' type,DBTIMEZONE cnt FROM dual
    UNION ALL
    SELECT 'session时区' type,SESSIONTIMEZONE cnt FROM dual
    """
    if check_dba_privelege():
        data = pool.executor(dba_sql)
        temp_sql = f'schema--table--sql: -- -- {dba_sql}'
        for i in data:
            print(f"{i.get('TYPE')}: {i.get('CNT')}")
    else:
        data = pool.executor(common_sql)
        temp_sql = f'schema--table--sql: -- -- {common_sql}'
        for i in data:
            print(f"{i.get('TYPE')}: {i.get('CNT')}")
    write_csv('8.概要信息.csv', [(data, temp_sql)])


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
         ___  _ __ __ _  ___| | ___ 
        / _ \| '__/ _` |/ __| |/ _ \
       | (_) | | | (_| | (__| |  __/
        \___/|_|  \__,_|\___|_|\___|    power by xugu  v1.0.0 

       """
    print(program)
    parser = argparse.ArgumentParser(
        # description='这是一个数据库环境采集工具',
        prefix_chars='-'
    )
    parser.add_argument('-H', '--host', help='输入数据库ip地址')
    parser.add_argument('-P', '--port', help='Port number 数据库端口', type=int, default=1521)
    parser.add_argument('-u', '--user', help='输入数据库 用户')
    parser.add_argument('-p', '--pwd', help='输入数据库密码')
    parser.add_argument('-s', '--service_name', help='输入服务名')
    args = parser.parse_args()
    # 访问解析后的参数
    # input_file = args.input_file
    # output_file = args.output
    host = args.host
    port = args.port
    user = args.user
    password = args.pwd
    service_name = args.service_name
    dsn = f'{host}:{port}/{service_name}'

    if len(sys.argv) == 1:
        host = input("请输入ip: ")
        port = input("请输入端口: ")
        user = input("请输入用户: ")
        password = input("请输入密码: ")
        service_name = input("请输入服务名: ")
        dsn = f'{host}:{port}/{service_name}'
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
    if not service_name:
        parser.print_help()
        raise Exception('请输入服务名!!!\n')
    if host and port and user and password and service_name:
        print(f'dsn: {dsn} user: {user} password: {password}')
    return host, port, user, password, dsn


if __name__ == "__main__":
    # pre_user = 'sys'
    # pre_password = 'rootroot'
    host = '10.28.23.225'
    port = 1521
    user = "test"
    password = "test"
    dsn = '10.28.23.225:1521/ORCL'

    # sql2 = 'select * from "SCOTT"."SALGRADE"'
    # host, port, user, password, dsn = parse_args()
    pool = OracleConnectionPool(user, password, dsn)
    timestands = time.strftime('%Y-%m-%d-%H%M%S', time.localtime())
    dir = f'oracle_result_{timestands}'
    os.path.exists(dir) or os.makedirs(dir)
    print(dir)
    start = time.time()
    # main(task_names)
    check_dba_privelege()
    get_charset()
    get_objects_count()
    get_table_statistic()
    get_table_space()
    get_schema_space()
    get_table_column()
    get_table_column_type()
    get_constraint()
    sumary()
    print(f'耗时: {time.time() - start:.2f} 秒')
    q = input('\nPress q to exit…')
    if q == 'q' or q == 'Q':
        pass
