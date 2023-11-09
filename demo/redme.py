# pyinstaller -c -F  --clean  --hidden-import=xgcondb     .\connection_pool.py
# connection_pool.exe -H 10.28.23.174 -P5138 -uSYSDBA -p SYSDBA -d SYSTEM

# 存储过程不要有注释,连接串编码必须为utf8
# todo 加索引，清除表及数据,先清除事务,arm随机函数
'''
todo: 多少人，多少天， 目标库的大小
''''''
