import xgcondb

conn = xgcondb.connect(host="127.0.0.1", port="5138", database="SYSTEM", user="SYSDBA", password="SYSDBA")
cur = conn.cursor()
cur.execute('drop table test')
cur.execute('create table test(arg1 int, arg2 varchar);')

# 存储过程和存储函数 有结果集 才需要setinputtype,setinputsizes, 个数要一致
# ---------------------存储过程提取结果集--------------------
cur.execute('''
CREATE or replace procedure pro_test(col1 int,col2 OUT SYS_REFCURSOR) as \
declare \
par1 int; \
str2 varchar; \
begin \
par1:=col1; \
for i in 1..par1 loop \
        str2:='insert into test values('||i||',''||par2||'');'; \
        execute immediate str2; \
end loop; \
OPEN col2 FOR SELECT * FROM test; \
end;''')
cur.setinputtype((xgcondb.XG_C_INTEGER, xgcondb.XG_C_REFCUR))
cur.setinputsizes((4, 10))
print(cur.callproc('pro_test', (5, 'refcur'), (1, 2)))
row = cur.fetchall()
for i in row:
    print(i)
cur.clearsize()
cur.cleartype()
cur.execute('drop procedure pro_test;')
cur.execute('drop procedure fun_test;')

# ---------------------存储函数提取结果集--------------------
cur.execute('''
CREATE or replace function fun_test(col1 int,col2 OUT SYS_REFCURSOR) return varchar as \
declare \
par1 int; \
str2 varchar; \
begin \
par1:=col1; \
for i in 1..par1 loop \
        str2:='insert into test values('||i||',''||par2||'');'; \
        execute immediate str2; \
end loop; \
OPEN col2 FOR SELECT * FROM test; \
return 111; \
end;''')
cur.setinputtype((xgcondb.XG_C_INTEGER, xgcondb.XG_C_REFCUR, xgcondb.XG_C_CHAR))
cur.setinputsizes((4, 10, 200))  # 此方法用于设置参数缓存空间大小,最后一个参数是函数返回值的预定义大小
print(cur.callfunc('fun_test', (3, 'refcur'), (1, 2)))
row = cur.fetchall()
for i in row:
    print(i)
cur.clearsize()
cur.close()
conn.close()
