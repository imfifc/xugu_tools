"""
python oracle_connect.py -H 192.168.2.217  -u u1 -p123456 -s ORCL


###打包
# pyinstaller -c -F --hidden-import=altgraph --hidden-import=lib --hidden-import=pefile --hidden-import=PyMySQL  --hidden-import=pywin32-ctypes   .\connection_pool.py
# pyinstaller -c -F --hidden-import=cx_Oracle    .\connection_pool.py


pyinstaller -c -F --hidden-import=cx_Oracle oracle_connect.py

"""
