import psycopg2
from psycopg2 import extras
import queue


class ConnectionPool:
    def __init__(self, max_connections, connection_params):
        self.max_connections = max_connections
        self.connection_params = connection_params
        self._pool = queue.Queue(maxsize=max_connections)
        self._initialize_pool()

    def _initialize_pool(self):
        for _ in range(self.max_connections):
            connection = self._create_connection()
            self._pool.put(connection)

    def _create_connection(self):
        return psycopg2.connect(**self.connection_params)

    def get_connection(self):
        try:
            return self._pool.get(block=False)
        except queue.Empty:
            raise Exception("Connection pool is empty. Try again later.")

    def release_connection(self, connection):
        self._pool.put(connection)

    def close_all_connections(self):
        while not self._pool.empty():
            connection = self._pool.get()
            connection.close()

    def executor(self, data):
        if isinstance(data, tuple):
            sql, tmp_sql = data
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=extras.DictCursor)
            try:
                cursor.execute(sql)
            except Exception as e:
                print(f'执行异常；{sql},{e}')
                self.release_connection(conn)
                return None
            rows = cursor.fetchall()
            rows = [dict(row) for row in rows]
            self.release_connection(conn)
            return rows, tmp_sql

        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=extras.DictCursor)
        try:
            cursor.execute(data)
        except Exception as e:
            conn.rollback()
            print(f'执行异常；{data},{e}')
            self.release_connection(conn)
            return None
        rows = cursor.fetchall()
        conn.commit()
        self.release_connection(conn)
        return [dict(row) for row in rows]


db_host = '10.28.23.197'
db_port = 5432
db_user = 'postgres'
db_pwd = 'postgres'
db_name = 'postgres'
# db_charset = 'utf8'

pool = ConnectionPool(
    max_connections=100,
    connection_params={
        "user": db_user,
        "password": db_pwd,
        "host": db_host,
        "port": db_port,
        # "db"
        # "database": db_name,
        # "charset": 'utf8',
    },
)

while 1:
    data = pool.executor("select * from pg_database")
    print(data)
