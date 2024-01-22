@echo off
setlocal enabledelayedexpansion

rem rem是bat脚本中的注释
rem tables 就是批量表名
set "tables=test1 test2 test3 test4 test5 test6"

rem D:\test 为导出目录，全英文; SYSDBA.%%i 为指定表; 其余需要改for循环中的sql语句
for %%i in (%tables%) do (
  java -jar xg-fetl.jar selectexp D:\test SYSDBA.%%i "select * from SYSDBA.%%i limit 100"
)

endlocal


