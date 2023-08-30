import getpass
import oracledb

# pw = getpass.getpass("Enter password: ")

connection = oracledb.connect(
    user="test",
    password='test',
    dsn="192.168.17.129:1521/oa")

print("Successfully connected to Oracle Database")

cursor = connection.cursor()

# Create a table

cursor.execute("""
    begin
        execute immediate 'drop table todoitem';
        exception when others then if sqlcode <> -942 then raise; end if;
    end;
    """)

cursor.execute("""
    create table todoitem (
        id number generated always as identity,
        description varchar2(4000),
        creation_ts timestamp with time zone default current_timestamp,
        done number(1,0),
        primary key (id))""")

# Insert some data

rows = [("Task 1", 0),
        ("Task 2", 0),
        ("Task 3", 1),
        ("Task 4", 0),
        ("Task 5", 1)]

cursor.executemany("insert into todoitem (description, done) values(:1, :2)", rows)
print(cursor.rowcount, "Rows Inserted")

connection.commit()
cursor.execute('select description, done from todoitem')
# res 转字典格式
for col in cursor.description:
    print(col.name)
columns = [col.name for col in cursor.description]
cursor.rowfactory = lambda *args: dict(zip(columns, args))
res = cursor.fetchall()
print(res)
# # Now query the rows back
# for row in res:
#     if (row[1]):
#         print(row[0], "is done")
#     else:
#         print(row[0], "is NOT done")
