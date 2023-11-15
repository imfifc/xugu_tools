import psycopg2
from psycopg2 import extras
# 连接到数据库
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="postgres",
    host="10.28.23.197",
    port="5432"
)

# 创建一个游标对象使用它执行SQL语句
cur = conn.cursor(cursor_factory=extras.DictCursor)

# 执行SQL查询
cur.execute("select * from pg_database")

# 获取查询结果
rows = cur.fetchall()

# 打印查询结果
for row in rows:
    # print(dict(row))
    print(dict(row))
# 关闭游标和连接
cur.close()
conn.close()


# import multiprocessing
# from multiprocessing import freeze_support
#
# import psycopg2
#
#
# def worker(num):
#     # conn = psycopg2.connect(database="testdb", user="postgres", password="pwd", host="127.0.0.1", port="5432")
#     conn = psycopg2.connect(
#         dbname="postgres",
#         user="postgres",
#         password="postgres",
#         host="10.28.23.197",
#         port="5432"
#     )
#     cur = conn.cursor()
#     print(num)
#     # print('Worker {} connected to DB'.format(num))
#     cur.execute("select count(*) from pg_database")
#     # do some queries...
#     cur.close()
#     conn.close()
#
#
# if __name__ == '__main__':
#     freeze_support()
#     jobs = []
#     for i in range(5):
#         p = multiprocessing.Process(target=worker, args=(i,))
#         jobs.append(p)
#         p.start()
#
#     for proc in jobs:
#         proc.join()
