import asyncio
import csv
import time
import pymysql
from pymysql.cursors import DictCursor

# Assuming you have an executor function that can execute SQL queries asynchronously
# and return results (executor is not shown in the provided code)

class ConnectionPool:
    def __init__(self, max_connections, connection_params):
        self.max_connections = max_connections
        self.connection_params = connection_params
        self._pool = asyncio.Queue(maxsize=max_connections)
        self._initialize_pool()

    def _initialize_pool(self):
        for _ in range(self.max_connections):
            connection = self._create_connection()
            self._pool.put_nowait(connection)

    def _create_connection(self):
        return pymysql.connect(**self.connection_params)

    async def get_connection(self):
        return await self._pool.get()

    def release_connection(self, connection):
        self._pool.put_nowait(connection)

    def close_all_connections(self):
        while not self._pool.empty():
            connection = self._pool.get_nowait()
            connection.close()

    async def executor(self, sql):
        async with self.get_connection() as conn:
            try:
                async with conn.cursor(DictCursor) as cursor:
                    await cursor.execute(sql)
                    result = await cursor.fetchall()
                    await conn.commit()
                    return result
            except Exception as e:
                await conn.rollback()
                print(500, e)
        return None


# 使用连接池
db_host = '192.168.2.212'
db_user = 'ecology'
db_pwd = 'Weaver@2023'
db_port = 3306
db_name = 'ecology'
db_charset = 'utf8'

# db_host = '127.0.0.1'
# db_user = 'root'
# db_pwd = '123456'
# db_port = 3306
# # db_name = 'idc'
# db_charset = 'utf8'

pool = ConnectionPool(
    max_connections=100,
    connection_params={
        "user": db_user,
        "password": db_pwd,
        "host": db_host,
        "port": db_port,
        # "database": db_name,
        "charset": db_charset,
    },
)


async def get_databases():
    sql = 'show databases;'
    databases = await pool.executor(sql)
    return [i.get('Database') for i in databases]


async def get_database_charset(db):
    sql = f'show create database {db};'
    data = await pool.executor(sql)
    temp_sql = f'db--table--sql: {db}-- -- {sql}'
    return data, temp_sql


async def get_table_status(db, tb):
    sql = f"show table status from `{db}` like '{tb}';"
    data = await pool.executor(sql)
    temp_sql = f'db--table--sql: {db}--{tb}-- {sql}'
    return data, temp_sql


async def get_table_columns(db, tb):
    sql = f'show full columns from {db}.{tb}'
    data = await pool.executor(sql)
    temp_sql = f'db--table--sql: {db}--{tb}-- {sql}'
    return data, temp_sql


async def write_csv(file_name, data, temp_sql):
    with open(file_name, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([temp_sql])
        writer.writerows(data)


async def main():
    dbs = await get_databases()

    db_charsets = await asyncio.gather(*(get_database_charset(db) for db in dbs))

    tb_status = []
    tb_culumns = []
    for db in dbs:
        tables = await pool.executor(f'show tables from {db};')
        tables = [i.get(f'Tables_in_{db}') for i in tables]
        tb_status.extend(await asyncio.gather(*(get_table_status(db, tb) for tb in tables)))
        tb_culumns.extend(await asyncio.gather(*(get_table_columns(db, tb) for tb in tables)))

    file_name1 = '2.数据库字符集.csv'
    await write_csv(file_name1, [data for data, temp_sql in db_charsets], [temp_sql for data, temp_sql in db_charsets])

    file_name2 = '2.tb_status 表字符集.csv'
    await write_csv(file_name2, [data for data, temp_sql in tb_status], [temp_sql for data, temp_sql in tb_status])

    file_name3 = '2.tb_column 列字符集.csv'
    await write_csv(file_name3, [data for data, temp_sql in tb_culumns], [temp_sql for data, temp_sql in tb_culumns])


if __name__ == "__main__":
    start = time.time()
    asyncio.run(main())
    print(time.time() - start)
