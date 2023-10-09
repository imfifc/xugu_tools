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
    print(row)
# 关闭游标和连接
cur.close()
conn.close()
