@echo off
rem 为注释行
REM 需要在目标端建立同样的表名及表结构
setlocal enabledelayedexpansion
REM 定义表名数组
set "tables=test1 test2 test3 test4 test5 test6"

REM D:\test\SYSDBA.%%i 为含XX.exp的目录，目录要全英文; SYSDBA.%%i 为指定表
for %%i in (%tables%) do (
  java -jar xg-fetl.jar impt D:\test\SYSDBA.%%i SYSDBA.%%i
)
endlocal



