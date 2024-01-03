import xgcondb

# conn = xgcondb.connect(host="127.0.0.1", port="5138", database="SYSTEM", user="SYSDBA", password="SYSDBA")
# cur = conn.cursor()
# cur.execute("create table update_tab(d1 int,d2 varchar);")
# t_list_1 = []
# t_list_2 = []
# name = 'Python'
# for i in range(10):
#     t_list_1.append(i)
#     t_list_2.append(name + str(i))
# cur.executebatch('insert into update_tab values(?,?);', (t_list_1, t_list_2))
# cur.execute("select * from update_tab;")
# row3 = cur.fetchall()
# print(row3)
# cur.execute("drop table update_tab;")
# cur.close()
# conn.close()

# 连接字符串请视情况而定，输入自己本地的数据库服务器IP地址、端口等
conn = xgcondb.connect(host="10.28.20.101", port="5136", database="SYSTEM", user="SYSDBA", password="SYSDBA")
cur = conn.cursor()
# 大对象导入
cur.execute("select count(*) from all_tables where table_name='TEST_LOB';")
row = cur.fetchone()
if row[0] == 1:
    cur.execute("drop table test_lob;")
cur.execute("create table test_lob(col1 int,  col2 blob);")

blob_buf = open("/root/Z_RADR_I_Z9513_20231103153041_O_DOR_SAD_CAP_FMT.bin.bz2", "rb").read()
print(len(blob_buf))
cur.cleartype()
# cur.setinputtype((xgcondb.XG_C_INTEGER, xgcondb.XG_C_BLOB))
t1 = []
t2 = []
for i in range(10):
    t1.append(i)
    t2.append(blob_buf)
cur.executebatch("insert into test_lob values(?,?);", (t1, t2))
print('done')
cur.cleartype()
cur.execute("select * from test_lob;")
row = cur.fetchalll()
print(row)
# print("CLOB length is", row[0])
# print("BLOB length is", row[1])

# 大对象导出
# cur.execute("select * from test_lob;")
# row = cur.fetchone()
# blob_fd = open("./xg_lob/getBlob.jpg", "wb+")
# blob_fd.write(row[1])
# if len(blob_buf) == len(row[1]):
#     print("")
#     print("The large object was successfully exported with the same length of data")
