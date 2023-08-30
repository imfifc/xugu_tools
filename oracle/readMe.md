python oracle_connect.py -H 192.168.2.217  -u u1 -p123456 -s ORCL


python oracle_connect.py  -H localhost -u jack -p123456 -s XEPDB1


### 打包

```shell
pyinstaller -c -F --hidden-import=altgraph --hidden-import=lib --hidden-import=pefile --hidden-import=PyMySQL  --hidden-import=pywin32-ctypes   .\connection_pool.py
pyinstaller -c -F --hidden-import=cx_Oracle    .\connection_pool.py
pyinstaller -c -F --hidden-import=cx_Oracle oracle_connect.py

oracle_connect.exe -H 192.168.17.129 -u test -p test -s oa


```



### pyinstaller 打包oracle 报错

```shell
报错类型:ConnectionError: DPI-1047: Cannot locate a 64-bit Oracle Client library: "libclntsh.so: cannot open shared object file: No such file or directory". See https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html for help
```
### 解决方法： 将oracle客户端lib库打包放进打包环境
1. 下载instantclient-basiclite-windows.x64-21.10.0.0.0dbru.zip   # https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html
2. 将解压后的*.dll 文件放入D:\llearn\xugu\oracle\dll 
3. 将oracle客户端lib库打包放进python环境


######pyinstaller添加oci.dll 动态二进制库 
win
```
pyinstaller -c -F --hidden-import=cx_Oracle --clean --add-binary "D:\llearn\xugu\oracle\dll;." oracle_connect.py
```
linux

```
pyinstaller -c -F --clean --hidden-import=cx_Oracle --add-binary="./instantclient_18_5/lib/*:."  oracle_connect.py

```
