import xgcondb

print(xgcondb.version())
# conn = xgcondb.connect(host=" 10.28.23.146", port="5138", database="SYSTEM", user="SYSDBA", password="SYSDBA",
#                        charset="GBK")
conn = xgcondb.connect(host=" 10.28.23.146", port="5138", database="PUB_DB_UTF8", user="DB_USER", password="QAZ_1234",
                       charset="utf8")

cur = conn.cursor()  # 创建一个游标对象
# cur.execute("create table test2(a int,b boolean,c boolean);")
# cur.execute("select * from sys_databases;")
# sql = "select * from dba_tables ;"
# cur.execute(sql)
# conn.commit()
# sql = ''
# cur.execute("select * from dba_users;")
# print(cur.fetchall())
# for i in cur.description:
#     print(i)
# for i in cur.fetchall():
#     print(i)

cur.execute("CREATE TABLE if not  exists TAB_DESCRIPTION_TEST1(A INT, B INT, C VARCHAR, D DATETIME, E NUMBER(4,2))")
print(cur.description)
cur.execute("insert into TAB_DESCRIPTION_TEST1 values(1,2,'hh','2023-09-07',10.22);")
cur.execute("SELECT * FROM TAB_DESCRIPTION_TEST1;")

conn.commit()

# print(cur.description)
# print(cur.fetchall())

column_names = [desc[0] for desc in cur.description]
results = [dict(zip(column_names, row)) for row in cur.fetchall()]
# print(results)
for i in results:
    print(i)

cur.close()
conn.close()
