import cx_Oracle
import threading

# user = 'sys'
# password = 'rootroot'
# host = '192.168.2.217:1521'
# service_name = 'ORCL'
# dsn = '192.168.2.217:1521/ORCL;privilege=SYSDBA'
# dsn = '192.168.2.217:1521/ORCL'

# dsn = cx_Oracle.makedsn("localhost", 1521, service_name="ORCL")
# dsn_with_privilege = dsn + ';privilege=SYSDBA'
# conn = cx_Oracle.connect(user, password, dsn, mode=cx_Oracle.SYSDBA)

# 连接信息
user = "u1"
password = "123456"
# dsn = '192.168.2.217:1521/ORCL;privilege=SYSDBA'  # 数据源名称，格式为: host:port/service_name
dsn = '192.168.2.217:1521/ORCL'  # 数据源名称，格式为: host:port/service_name

pool = cx_Oracle.SessionPool(user, password, dsn,
                             min=2, max=5, increment=1, threaded=True, encoding="UTF-8",
                             getmode=cx_Oracle.SPOOL_ATTRVAL_WAIT)

# 从连接池中获取一个连接
connection = pool.acquire()

# 使用连接执行查询
# with connection.cursor() as cursor:
cursor = connection.cursor()
sql = 'SELECT NAME, BYTES FROM V$SGAINFO'
sql2 = 'select * from "SCOTT"."SALGRADE"'
cursor.execute(sql2)
result = cursor.fetchall()
for row in result:
    print(row)

cursor.close()
pool.release(connection)
pool.close()


def Query():
    conn = pool.acquire()
    cur = conn.cursor()
    for i in range(4):
        cur.execute(sql2)
        res = cur.fetchall()
        print("Thread", threading.current_thread().name, "fetched sequence =", res)
    cur.close()
    pool.release(conn)


# numberOfThreads = 1
# threadArray = []
#
# for i in range(numberOfThreads):
#     thread = threading.Thread(name='#' + str(i), target=Query)
#     threadArray.append(thread)
#     thread.start()
#
# for t in threadArray:
#     t.join()
#
# print("All done!")

Query()
