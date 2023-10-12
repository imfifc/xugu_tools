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
pyinstaller -c -F --clean --hidden-import=cx_Oracle --add-binary="./instantclient_19_20/lib/*:."  oracle_connect.py

```


linux instantclient 下载:
https://download.oracle.com/otn_software/linux/instantclient/1920000/instantclient-basiclite-linux.x64-19.20.0.0.0dbru.zip
解压后把所有的so文件 放在这个目录下， 然后用pyinstaller打包
instantclient_19_20/lib/
(venv) [root@oracle-centos7-instance1 lib]# tree
.
├── libclntshcore.so.19.1
├── libclntsh.so.19.1
├── libipc1.so
├── libmql1.so
├── libnnz19.so
├── libocci.so.19.1
├── libociicus.so
├── libocijdbc19.so
└── liboramysql19.so

导出为环境变量:
export LD_LIBRARY_PATH=/tmp/xugu/oracle/instantclient_19_20:$LD_LIBRARY_PATH  

将环境变量LD_LIBRARY_PATH设置为Instant Client版本的适当目录。 例如：
export LD_LIBRARY_PATH=/opt/oracle/instantclient_19_6:$LD_LIBRARY_PATH


报错: Cannot locate a 64-bit Oracle Client library: "libaio.so.1
则安装libaio:
yum install -y libaio-0.3.109-13.el7.x86_64