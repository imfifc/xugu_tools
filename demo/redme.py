# pyinstaller -c -F  --clean  --hidden-import=xgcondb     .\connection_pool.py
# connection_pool.exe -H  10.28.20.101 -P6326 -uSYSDBA -p SYSDBA -d SYSTEM

# pyinstaller -c -F  --clean  --hidden-import=xgcondb   --hidden-import=faker make_data5.py
# pyinstaller -c -F  --clean  --hidden-import=xgcondb   --hidden-import=faker connection_pool.py

# linux 需要,  cp libxugusql.so /usr/lib64/
# 存储过程不要有注释,连接串编码必须为utf8
#  加索引，清除表及数据,先清除事务,arm随机函数
'''
todo: 多少人，多少天， 目标库的大小
''''''
