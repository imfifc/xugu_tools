# -*- coding:utf-8 -*-

import pymysql
from pymysql.cursors import DictCursor
# from lib.aes import AESHelper

cursor_dict = {
    "dict": pymysql.cursors.DictCursor
}


class MySQL(object):
    def __init__(self, host, port, user, passwd, db='', cursor=None):
        self._host = str(host).strip()
        self._user = str(user).strip()
        self._passwd = passwd
        self._db = str(db).strip()
        self._conn = None
        self._cursor = cursor

        if isinstance(port, str):
            self._port = int(str(port).strip())
        else:
            self._port = port

    def connect_db(self):
        self._conn = pymysql.connect(host=self._host, port=self._port, user=self._user,
                                     passwd=self._passwd, db=self._db, charset="utf8")
        if not isinstance(self._conn, pymysql.connections.Connection):
            raise Exception(self._conn)

    def cursor(self):
        return self._conn.cursor(cursor=cursor_dict.get(self._cursor))

    def fetch_data(self, sql):
        if self._conn is None:
            try:
                self.connect_db()
            except Exception as err:
                return 500, err
        try:
            cur = self.cursor()
            cur.execute(sql)
            data = cur.fetchall()
            cur.close()
            self._conn.commit()
            return 200, data
        except Exception as err:
            self._conn.rollback()
            return 500, err

    def executer(self, sql):
        if self._conn is None:
            self.connect_db()
        try:
            cur = self.cursor()
            cur.execute(sql)
            cur.close()
            self._conn.commit()
            return 200, "OK"
        except Exception as err:
            self._conn.rollback()
            return 500, err
