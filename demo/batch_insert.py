from datetime import datetime, timedelta
import os
import time
from concurrent.futures import ThreadPoolExecutor, wait

import xgcondb

conn = xgcondb.connect(host="10.28.23.174", port="5138", database="SYSTEM", user="SYSDBA", password="SYSDBA")
cur = conn.cursor()
start = time.time()


def show(table):
    cur.execute(f"select count(*) from {table} ")
    row = cur.fetchone()
    print(f"{table}:{row}")


def execute():
    # conn.autocommit(False)# 默认自动提交
    cur.execute("drop table if exists test")
    cur.execute("create table test(a int,b varchar,c date,d varchar,e numeric(8,4) );")
    cur.execute("insert into test values(1, 'xugu', '2017-05-26', 'kkk', '3.3432')")
    cur.execute("insert into test values(%i,'%s','%s','%s',%f);" % (2, 'xugu', '2017-05-26', 'yyy', 9.3432))
    sql = "insert into test values(?,?,?,?,?);"
    rows = (3, 'xugu', '2017-05-26', 'yyy', '3.3432')
    cur.execute(sql, rows)
    show('test')


# execute()
def execute2():
    # 设置非自动提交，默认自动提交
    # conn.autocommit(False)
    cur.execute("drop table if exists test2")
    cur.execute("create table test2(a bigint,b boolean,c boolean,d varchar);")
    cur.execute("insert into test2 values(?,?,?,?);", (234, False, True, None))
    cur.execute("select * from test2;")
    rows = cur.fetchall()
    print(rows)
    cur.execute("drop table if exists test")
    cur.execute("create table test(a int, b varchar(10),c char(100),d datetime,e double,f numeric(18,4));")
    cur.execute("insert into test values(-1,'xugu','ouguan','2017-05-26',12.5,12323423.3432);")
    cur.execute(
        "insert into test values(%i,'%s','%s','%s',%f,%f);" % (0, 'xugu', 'ouguan', '2017-05-26', 12.5, 12323423.3432))
    # # execute() 使用参数形式执行SQL语句
    # 备注：目前支持的Python的数据类型包括：整形，字符型，浮点型，boolean，None
    sql = "insert into test values(?,?,?,?,?,?);"
    # 第二个参数表示数据库行数据，建议使用Tuple类型，支持List，不支持dict
    cur.execute(sql, (1, 'xugu', 'ouguan', '2017-04-27', 12.5, 12323423.3432))
    cur.execute(sql, [2, 'xugu', 'ouguan', '2017-06-27', 12.5, 12323423.3432])
    # # executemany() 批量执行,备注：使用方式和支持类型参考execute()，区别：executemany()执行insert时是用preparestatment
    rows = ((3, 'xugu', 'ouguan', '2017-05-26', '12.5', '12323423.3432'),
            ('4', 'xugu', 'ouguan', '2017-05-26', '12.5', '12323423.3432'))
    cur.executemany(sql, rows)
    cur.executemany("select * from dual;")
    # cur.executemany(sql, (5, 'xugu', 'ouguan', '2017-07-27', 112.5, 12323423.3432))
    cur.executemany(sql, rows)
    cur.execute("select * from test;")
    try:
        for i in cur.fetchall():
            print(i)
    except Exception as e:
        print(e)


# execute2()


def execute_many():
    """ executemany()执 行insert时 是 用preparestatment 虽然是prepare 但是确是一次一次的提交的，大量insert 反而最慢"""
    # cur.execute("drop table if exists test")
    cur.execute("drop table if exists test")
    cur.execute("create table test(a int,b varchar,c date,d varchar,e numeric(8,4) );")
    rows = []
    for i in range(1000):
        row = (i, 'xugu', '2017-05-26', f'{i}', f'{i}')
        rows.append(row)
    rows = tuple(rows)
    sql = "insert into test values(?,?,?,?,?);"
    cur.executemany(sql, rows)
    print(cur.rowcount)
    # cur.executemany("select * from dual;")
    # cur.executemany(sql, (5, 'xugu', '2017-07-27', "dd", 4223.3432))
    show('test')


# execute_many()  # 1000: 23s


def executebatch():
    """目前超过10000条就失败,
    目前能跟jdbc 的prepare批量插入速度相比， python 最快的插入方式
    """
    cur.execute("drop table if exists update_tab;")
    cur.execute("create  table update_tab(d1 int,d2 varchar);")
    t_list_1 = []
    t_list_2 = []
    name = 'Python'
    for i in range(32500):
        # 单次批量执行改变的行数不得超过数据库设置的单个事务最大变更数,
        t_list_1.append(i)
        t_list_2.append(name + str(i))
    # print(len(t_list_1))
    cur.executebatch('insert into update_tab values(?,?);', (t_list_1, t_list_2))
    show('update_tab')


executebatch()  # 1w行: 1.3s


def main():
    # print(len(task_names))
    # with ThreadPoolExecutor(max_workers=100) as executor:
    #     futures = executor.map(lambda func: func(), task_names)
    #     # 获取并处理任务的返回结果
    #     for future in futures:
    #         try:
    #             # for future in concurrent.futures.as_completed(futures):
    #             if future is not None:
    #                 future.result()
    #         # print(f"Function task returned: {result}")
    #         except Exception as e:
    #             print(f"Function task encountered an error: {e}")
    with ThreadPoolExecutor(max_workers=100) as executor:
        # 提交任务给线程池
        task_ids = list(range(10000))
        futures = [executor.submit(insert_into, task_id) for task_id in task_ids]

        # 等待所有任务完成
        wait(futures)

    print("All tasks are done")


cur.close()
conn.close()
print(time.time() - start)
