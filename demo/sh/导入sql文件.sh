# 对多个sql 文件进行导入 ；sqldir为导入的目录
sqldir="." && for sqlfile  in `find ${sqldir} -type f -name "*.sql"`;do echo ${sqlfile} &&  cat ${sqlfile} | ./xgconsole  nssl 127.0.0.1 5138 system SYSDBA SYSDBA   ; done




# 对单个sql文件进行导入
cat ${sqlfile} | ./xgconsole  nssl 127.0.0.1 5138 system SYSDBA SYSDBA