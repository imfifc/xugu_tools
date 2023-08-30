import cx_Oracle

import os


def check_env():
    # print(f'os.environ["PATH"]: {os.environ["PATH"]}')
    print(f'os.system("where oci.dll"): {os.system("where oci.dll")}')
    print(11, os.system('where oracore.dll'))
    print(22, os.system('where  orauts.dll'))
    print(33, os.system('where ucrtbased.dll'))


check_env()
# 不加这个库路径 就报错
cx_Oracle.init_oracle_client(lib_dir=r"D:\llearn\xugu\oracle\dll")

ver = cx_Oracle.clientversion()
# print(cx_Oracle.init_oracle_client())
print(ver)

# import cx_Oracle
# import sys
# import os
#
# try:
#     if sys.platform.startswith("darwin"):
#         lib_dir = os.path.join(os.environ.get("HOME"), "Downloads",
#                                "instantclient_19_8")
#         cx_Oracle.init_oracle_client(lib_dir=lib_dir)
#     elif sys.platform.startswith("win32"):
#         lib_dir = r"C:\oracle\instantclient_19_9"
#         cx_Oracle.init_oracle_client(lib_dir=lib_dir)
# except Exception as err:
#     print("Whoops!")
#     print(err)
#     sys.exit(1)
