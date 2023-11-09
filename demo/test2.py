import xgcondb

print(xgcondb.version())
# conn = xgcondb.connect(host=" 10.28.23.146", port="5138", database="SYSTEM", user="SYSDBA", password="SYSDBA",
#                        charset="GBK")
conn = xgcondb.connect(host=" 127.0.0.1", port="5138", database="SYSTEM", user="SYSDBA", password="SYSDBA",
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

# cur.execute("CREATE TABLE if not  exists TAB_DESCRIPTION_TEST1(A INT, B INT, C VARCHAR, D DATETIME, E NUMBER(4,2))")
# print(cur.description)
# cur.execute("insert into TAB_DESCRIPTION_TEST1 values(1,2,'hh','2023-09-07',10.22);")
# cur.execute("SELECT * FROM TAB_DESCRIPTION_TEST1;")
#
# conn.commit()
#
# # print(cur.description)
# # print(cur.fetchall())
#
# column_names = [desc[0] for desc in cur.description]
# results = [dict(zip(column_names, row)) for row in cur.fetchall()]
# # print(results)
# for i in results:
#     print(i)
#
# cur.close()
# conn.close()


def show():
    conn = xgcondb.connect(host="10.28.23.174", port="5138", database="SYSTEM", user="SYSDBA", password="SYSDBA",
                           charset="utf8")
    cur = conn.cursor()
    sql = 'select db_name "数据库",char_set "字符集",time_zone "时区",online "在线" from dba_databases'
    cur.execute(sql)
    # print(results)
    for i in cur.fetchall():
        print(i)


# show()
# def test_proc():
#     conn = xgcondb.connect(host="10.28.23.174", port="5138", database="SYSTEM", user="SYSDBA", password="SYSDBA",
#                            charset="utf8")
#     cur = conn.cursor()
#     cur.execute("select count(*) from all_tables where table_name='TEST_EXEC';")
#     row = cur.fetchone()
#     if row[0] == 0:
#         cur.execute("create table test_exec(col1 int, col2 varchar, col3 numeric(13,6));")
#     else:
#         cur.execute("truncate table test_exec;")
#     # 无参数存储过程
#     cur.execute("""
#     CREATE OR REPLACE PROCEDURE exec_proc_test1() IS
#     DECLARE
#     str VARCHAR;
#     BEGIN
#             FOR i IN 1..9 LOOP
#                     str:='insert into test_exec(col1,col2) values('||i||',''a'||i||''')';
#                     EXECUTE IMMEDIATE str;
#             END LOOP;
#     END;
#     """)
#
#     row = cur.callproc("exec_proc_test1")
#     print("Execute the parameterless stored procedure to return the result set:", row)
def test_proc(num):
    conn = xgcondb.connect(host="10.28.23.174", port="5138", database="SYSTEM", user="SYSDBA", password="SYSDBA",
                           charset="utf8")
    cur = conn.cursor()
    # cur.execute("select count(*) from all_tables where table_name='TEST_EXEC';")
    # row = cur.fetchone()
    # if row[0] == 0:
    #     cur.execute("create table test_exec(col1 int, col2 varchar, col3 numeric(13,6));")
    # else:
    #     cur.execute("truncate table test_exec;")
    # 无参数存储过程
    sql = """
            CREATE OR REPLACE PROCEDURE exec_proc_test1() IS 
    DECLARE 
    str VARCHAR; 
    BEGIN 
            FOR i IN 1..9 LOOP 
                    str:='insert into test_exec(col1,col2) values('||i||',''bb'||i||''')'; 
                    EXECUTE IMMEDIATE str; 
            END LOOP; 
    END;
    """
    sql2 = f"""
     CREATE OR REPLACE PROCEDURE SP_insert_356_DATA() IS
    i number;
    begin
     for i in 1..{num} loop 
    INSERT INTO SYSDBA.PRODUCTS_TEST
                VALUES ( sys_uuid AS product_no 
                       , DBMS_RANDOM.STRING('x', 8) AS product_name 
                       , CASE TRUNC(DBMS_RANDOM.VALUE(1, 6)) 
                             when 1 then '零食大礼包A'
                             when 2 then '零食大礼包B'
                             when 3 then '零食大礼包C'
                             when 4 then '零食大礼包D'
                             when 5 then '零食大礼包E'
                             ELSE '零食大礼包E'
                             END AS product_introduce
                        , to_date('2017-01-01 00:00:00','yyyy-mm-dd hh24:mi:ss')+i AS manufacture_date
                        , to_date('2018-04-01 00:00:00','yyyy-mm-dd hh24:mi:ss')+i AS sell_dates
                       , CASE TRUNC(DBMS_RANDOM.VALUE(1, 6)) 
                             when 1 then '北京'
                             when 2 then '上海'
                             when 3 then '深圳'
                             when 4 then  '广州'
                             when 5 then '成都'
                             else  '武汉'
                             END address
                       ,CASE TRUNC(DBMS_RANDOM.VALUE(1, 6)) 
                             when 1 then '食品'
                             when 2 then '饰品'
                             when 3 then '汽车'
                             when 4 then  '五金'
                             when 5 then '军工'
                             else  '海鲜'
                             END);
    end loop;
    commit;
    end SP_insert_356_DATA;
    """
    cur.execute(sql2)

    row = cur.callproc("SP_insert_356_DATA")
    print("Execute the parameterless stored procedure to return the result set:", row)


# test_proc(200)

