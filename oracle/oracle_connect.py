import cx_Oracle
import csv
import os
import time


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
            cursor.execute(sql)
            column_names = [desc[0] for desc in cursor.description]
            results = [dict(zip(column_names, row)) for row in cursor.fetchall()]
        conn.commit()
        self.release_connection(conn)
        return results


def use_privilege_conn(sql):
    """
    使用sysdba 模式
    :param sql:
    :return:
    """
    with cx_Oracle.connect(pre_user, pre_password, dsn, mode=cx_Oracle.SYSDBA) as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            # cur.rowfactory = make_dict_factory(cur)
            columns = [col[0] for col in cur.description]
            cur.rowfactory = lambda *args: dict(zip(columns, args))
            data = cur.fetchall()
            # print(data)
            return data
        except Exception as e:
            print("查询失败", e)
        conn.commit()


def write_csv(filename, data):
    """

    :param filename:
    :param data: [(res, tmp_sql),(res, tmp_sql)...]  res:[{},{}...]
    :return:
    """
    filename = os.path.join(dir, filename)
    with open(filename, 'a+', newline='', encoding='utf-8') as f:
        if data[0][0]:
            fieldnames = data[0][0][0].keys()
            writer1 = csv.DictWriter(f, fieldnames=fieldnames)
            write = csv.writer(f)
            for i in data:
                if i[0]:
                    write.writerow([i[1]])
                    fieldnames = i[0][0].keys()
                    writer1.fieldnames = fieldnames
                    writer1.writeheader()
                    writer1.writerows(i[0])


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
    sql_user = "select username from dba_users WHERE ACCOUNT_STATUS ='OPEN' order by username"
    users = pool.executor(sql_user)
    users = [i.get('USERNAME') for i in users]
    print(len(users), users)
    db_objects = []
    for user in users:
        sql = f"""select object_type,count(*)  from dba_objects where owner='{user}' group by object_type"""
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
    users = get_databases()
    db_tables = []
    for user in users:
        sql = f'''SELECT table_name ,num_rows  FROM dba_tables WHERE OWNER='{user}'and num_rows is not null order by num_rows desc'''
        res = pool.executor(sql)
        tmp_sql = f'schema--table--sql: {user}--dba_tables-- {sql}'
        db_tables.append((res, tmp_sql))
    file_name1 = '3.表及行数.csv'
    write_csv(file_name1, db_tables)


def get_table_space():
    """
    -- 4、统计所有用户下的表对象使用空间大小
SELECT OWNER, concat(round(sum(bytes)/1024/1024,2),'MB') AS data FROM dba_segments
       WHERE SEGMENT_TYPE LIKE 'TABLE%' GROUP BY OWNER ORDER BY round(sum(bytes)/1024/1024,2) desc;
    :return:
    """
    sql = """
    SELECT OWNER, concat(round(sum(bytes)/1024/1024,2),'MB') AS data FROM dba_segments
       WHERE SEGMENT_TYPE LIKE 'TABLE%' GROUP BY OWNER ORDER BY round(sum(bytes)/1024/1024,2) desc
    """
    data = pool.executor(sql)
    temp_sql = f'schema--table--sql: --dba_segments-- {sql}'
    write_csv('4.所有用户的表空间大小.csv', [(data, temp_sql)])


def get_table_column():
    """
    -- 5、查看数据库表对象使用的列名称(关键字排查)
SQL> SELECT  column_name FROM dba_tab_columns WHERE owner='USER'
     GROUP BY column_name ORDER BY column_name desc;
    :return:
    """
    sql = """
    
    """
    users = get_databases()
    columns = []
    for user in users:
        sql = f"""SELECT  column_name FROM dba_tab_columns WHERE owner='{user}'
                    GROUP BY column_name ORDER BY column_name """
        data = pool.executor(sql)
        temp_sql = f'schema--table--sql: --dba_tab_columns-- {sql}'
        columns.append((data, temp_sql))
    write_csv('5.表列名.csv', columns)


def get_table_column_type():
    """
    -- 6、查看数据库表对象使用的数据类型
    SQL> SELECT  DATA_TYPE,count(*) FROM dba_tab_columns WHERE OWNER='USER' GROUP BY DATA_TYPE ORDER BY  COUNT(*) desc;
    :return:
    """
    users = get_databases()
    column_types = []
    for user in users:
        sql = f'''SELECT  DATA_TYPE,count(*) AS num FROM dba_tab_columns WHERE OWNER='{user}' GROUP BY DATA_TYPE ORDER BY num desc'''
        data = pool.executor(sql)
        temp_sql = f'schema--table--sql: --dba_tab_columns-- {sql}'
        column_types.append((data, temp_sql))
    write_csv('6.表列字段类型及数量.csv', column_types)


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
    sql = '''
    SELECT  c.OWNER AS 约束拥有者,
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
     SELECT username FROM dba_users WHERE created>=(SELECT created FROM v$database) AND username!='HR')
   ORDER BY c.OWNER
    '''
    data = pool.executor(sql)
    temp_sql = f'schema--table--sql: -- -- {sql}'
    write_csv('7.每个用户模式下的表约束.csv', [(data, temp_sql)])


if __name__ == "__main__":
    pre_user = 'sys'
    pre_password = 'rootroot'
    user = "u1"
    password = "123456"
    dsn = '192.168.2.217:1521/ORCL'

    sql2 = 'select * from "SCOTT"."SALGRADE"'

    pool = OracleConnectionPool(user, password, dsn)
    timestands = time.strftime('%Y年%m月%d日%H%M%S', time.localtime())
    dir = f'result_{timestands}'
    os.path.exists(dir) or os.makedirs(dir)
    # try:
    #     data = pool.executor(sql2)
    #     print(data)
    # except Exception as e:
    #     print("Error:", e)
    # finally:
    #     pool.close_pool()

    # use_privilege_conn(sql2)
    # get_charset()
    # get_objects_count()
    # get_table_statistic()
    # get_table_space()
    get_table_column()
    get_table_column_type()
    get_constraint()
