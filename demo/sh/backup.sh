logfile="/tmp/crontab.log" && \
bkfile="/BACKUP/ecology2_`/usr/bin/date '+%Y-%m-%d_%H%M%S'`.exp"  && \
echo "bkfile is ${bkfile}" >> ${logfile} && \
echo -e "use ecology2;\nbackup database to '${bkfile}';\nexit;" >> ${logfile} && \
echo -e "use ecology2;\nbackup database to '${bkfile}';\nexit;"   |   /usr/sbin/xgconsole nssl 127.0.0.1 5138 system SYSDBA SYSDBA

#delete files one month ago.

ls -t /xugu_db/XuguServer/XHOME/BACKUP/ecology2_*.exp| sed -n '60,$p'  |xargs rm -rvf

find /xugu_db/XuguServer/XHOME/BACKUP/ecology2_*.exp >>  ${logfile} ;

/usr/bin/sync