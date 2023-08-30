"""
python oracle_connect.py -H 192.168.2.217  -u u1 -p123456 -s ORCL
python oracle_connect.py -H localhost -u jack -p123456 -s XEPDB1

###打包
# pyinstaller -c -F --hidden-import=altgraph --hidden-import=lib --hidden-import=pefile --hidden-import=PyMySQL  --hidden-import=pywin32-ctypes   .\connection_pool.py
# pyinstaller -c -F --hidden-import=cx_Oracle    .\connection_pool.py


pyinstaller -c -F --hidden-import=cx_Oracle oracle_connect.py


oracle_connect.exe -H 192.168.17.129 -u test -p test -s oa

#error   pyinstaller -c -F --add-data "D:\llearn\xugu\oracle\dll;dependency"  --hidden-import=cx_Oracle oracle_connect.py
"""


"""
下载ddl 实例客户端库 https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html
添加oci.dll 动态二进制库
win 分号;
pyinstaller -c -F --hidden-import=cx_Oracle --clean --add-binary "D:\llearn\xugu\oracle\dll;." oracle_connect.py

linux 冒号:
pyinstaller -c -F --clean --hidden-import=cx_Oracle --add-binary=“./instantclient_18_5/lib/*:.”  oracle_connect.py

"""
