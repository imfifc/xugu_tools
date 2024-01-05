# -*- coding: utf-8 -*-
import time
import xgcondb

# 连接字符串请视情况而定，输入自己本地的数据库服务器IP地址、端口等
conn = xgcondb.connect(host="192.168.31.101", port="5138", database="SYSTEM", user="SYSDBA", password="SYSDBA")
cur = conn.cursor()
cur.execute("select count(*) from all_tables where table_name='TEST_LOB';")
row = cur.fetchone()
if row[0] == 1:
    cur.execute("drop table test_lob;")
cur.execute("create table test_lob(col1 int,  col2 blob);")
# path = input("输入blob文件绝对路径: ")
# path2 = os.path.isfile(path)
blob_buf = open('/root/Z_RADR_I_Z9513_20231103153041_O_DOR_SAD_CAP_FMT.bin.bz2', "rb").read()
cur.cleartype()
cur.setinputtype((xgcondb.XG_C_INTEGER, xgcondb.XG_C_BLOB))


# 大对象导入

def execute_batch():
    start = time.time()
    rows = []
    for i in range(1000):
        data = (i, blob_buf)
        rows.append(data)
    p0, p1 = zip(*rows)
    print(len(rows))
    cur.executebatch("insert into test_lob values(?,?);", (list(p0), list(p1)))
    print(cur.rowcount)
    print(f'耗时{(time.time() - start):.2f}秒')


#     500 * 35M  : 143s   1000:293s

def execute_many():
    """ executemany()执 行insert时 是 用preparestatment 虽然是prepare 但是确是一次一次的提交的，大量insert 反而最慢"""
    start = time.time()
    rows = []
    for i in range(1000):
        data = (i, blob_buf)
        rows.append(data)
    sql = "insert into test_lob values(?,?);"
    cur.executemany(sql, tuple(rows))
    print(cur.rowcount)
    print(f'耗时{(time.time() - start):.2f}秒')


#     500 * 35M  : 196s   1000:313s


execute_many()

# def insert_many_radr(day, path, table, db_config):
#     cur = get_cur(db_config)
#     sql = f"insert into {table} values(?,?,?,sysdate,?,?,?)"
#     file_name = os.path.basename(path)
#     blob_buf = open(path, "rb").read()
#     cur.cleartype()
#     cur.setinputtype((xgcondb.XG_C_CHAR, xgcondb.XG_C_CHAR, xgcondb.XG_C_CHAR, xgcondb.XG_C_DATETIME,
#                       xgcondb.XG_C_DATETIME, xgcondb.XG_C_CHAR, xgcondb.XG_C_BLOB))
#     day = datetime.strptime(day, '%Y-%m-%d %H:%M:%S')
#     print(day, type(day))
#     min_datas = [str(day + timedelta(minutes=j * 6)) for j in range(240)]  # 240个6分钟，即24小时
#     site = file_name.split("_")[3]
#     rows = []
#     for i in min_datas:
#         min_str = datetime.strptime(i, '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M%S')
#         updated_file_name = re.sub(r'\d{5,}', min_str, file_name)
#         data = ('Z', updated_file_name, site, i, 'bin.bz2', blob_buf)
#         rows.append(data)
#     data = zip(*rows)
#     p1, p2, p3, p4, p5, p6 = data
#     p1, p2, p3, p4, p5, p6 = list(p1), list(p2), list(p3), list(p4), list(p5), list(p6)
#     cur.executebatch(sql, (p1, p2, p3, p4, p5, p6))
#     # cur.executemany(sql, tuple(rows))


# 大对象导出
# cur.execute("select * from test_lob;")
# row = cur.fetchone()
# blob_fd = open("./xg_lob/getBlob.jpg", "wb+")
# blob_fd.write(row[1])
# if len(blob_buf) == len(row[1]):
#     print("")
#     print("The large object was successfully exported with the same length of data")
