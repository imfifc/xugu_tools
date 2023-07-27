from mysql import MySQL

db_host = '192.168.2.212'
db_user = 'ecology'
db_pwd = 'Weaver@2023'
db_port = 3306
db_name = 'ecology'
db_charset = 'utf8'

connection_params = {
                        "user": db_user,
                        "password": db_pwd,
                        "host": db_host,
                        "port": db_port,
                        "database": db_name,
                        "charset": db_charset,
                    },

conn = MySQL(**connection_params)
conn.executer('show databases;')
