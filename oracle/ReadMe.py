"""
python oracle_connect.py -H 192.168.2.217  -u u1 -p123456 -s ORCL
python oracle_connect.py -H localhost -u jack -p123456 -s XEPDB1

###打包
# pyinstaller -c -F --hidden-import=altgraph --hidden-import=lib --hidden-import=pefile --hidden-import=PyMySQL  --hidden-import=pywin32-ctypes   .\connection_pool.py
# pyinstaller -c -F --hidden-import=cx_Oracle    .\connection_pool.py


pyinstaller -c -F --hidden-import=cx_Oracle oracle_connect.py


oracle_connect.exe -H 10.28.23.225 -P1521 -u test -p test -s orcl

#error   pyinstaller -c -F --add-data "D:\llearn\xugu\oracle\dll;dependency"  --hidden-import=cx_Oracle oracle_connect.py
"""

###  pyinstaller 打包oracle 报错
#  报错类型:ConnectionError: DPI-1047: Cannot locate a 64-bit Oracle Client library: "libclntsh.so: cannot open shared object file: No such file or directory". See https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html for help

'''
### 解决方法： 将oracle客户端lib库打包放进打包环境
1. 下载instantclient-basiclite-windows.x64-21.10.0.0.0dbru.zip   # https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html
2. 将解压后的*.dll 文件放入D:\llearn\xugu\oracle\dll 
3. 将oracle客户端lib库打包放进python环境
'''

"""windows_oracle 打包
下载ddl 实例客户端库 https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html
添加oci.dll 动态二进制库，instantclient_19版本不支持win7,需要下载低版本
win7: 如果报错：找不到MSVCR120.dll，则需要下载vcredist_x64.exe。 Visual C++ Redistributable Packages， 下载地址 https://www.microsoft.com/zh-cn/download/details.aspx?id=40784
win 分号;
pyinstaller -c -F --clean --hidden-import=cx_Oracle --clean --add-binary "D:\llearn\xugu\oracle\dll;." oracle_connect.py

linux 冒号:
pyinstaller -c -F --clean --hidden-import=cx_Oracle --add-binary="./instantclient_18_5/lib/*:."  oracle_connect.py

"""

# linux 打包:
"""
linux instantclient 下载:
https://download.oracle.com/otn_software/linux/instantclient/1920000/instantclient-basiclite-linux.x64-19.20.0.0.0dbru.zip
导出为环境变量:
export LD_LIBRARY_PATH=/tmp/xugu/oracle/instantclient_19_20:$LD_LIBRARY_PATH  

将环境变量LD_LIBRARY_PATH设置为Instant Client版本的适当目录。
export LD_LIBRARY_PATH=/opt/oracle/instantclient_19_6:$LD_LIBRARY_PATH

export LD_LIBRARY_PATH=/tmp/xugu/oracle/so:$LD_LIBRARY_PATH


报错: Cannot locate a 64-bit Oracle Client library: "libaio.so.1
则安装libaio:
yum install -y libaio-0.3.109-13.el7.x86_64

/tmp/xugu/oracle/so/ 目录下的执行文件，一般为so库
libclntshcore.so.19.1
libclntsh.so.19.1
libipc1.so
libmql1.so
libnnz19.so
libocci.so.19.1
libociicus.so
libocijdbc19.so
liboramysql19.so


linux_oracle 打包:
pyinstaller -c -F --clean --hidden-import=cx_Oracle --add-binary="/tmp/xugu/oracle/so/*:."  oracle_connect.py
"""
