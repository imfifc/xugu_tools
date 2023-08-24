data = [([{
    'OBJECT_TYPE': 'SEQUENCE',
    'COUNT(*)': 1
}, {
    'OBJECT_TYPE': 'TRIGGER',
    'COUNT(*)': 1
}, {
    'OBJECT_TYPE': 'SYNONYM',
    'COUNT(*)': 1
}, {
    'OBJECT_TYPE': 'TABLE',
    'COUNT(*)': 10
}, {
    'OBJECT_TYPE': 'INDEX',
    'COUNT(*)': 12
}, {
    'OBJECT_TYPE': 'FUNCTION',
    'COUNT(*)': 1
}],
         "schema--table--sql: BENCHMARKSQL--dba_objects-- select object_type,count(*)  from dba_objects where owner='BENCHMARKSQL' group by object_type"),
    ([{
        'OBJECT_TYPE': 'LOB',
        'COUNT(*)': 2
    }, {
        'OBJECT_TYPE': 'INDEX',
        'COUNT(*)': 40000
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 20002
    }, {
        'OBJECT_TYPE': 'SYNONYM',
        'COUNT(*)': 1
    }],
     "schema--table--sql: DTS_TEST--dba_objects-- select object_type,count(*)  from dba_objects where owner='DTS_TEST' group by object_type"),
    ([{
        'OBJECT_TYPE': 'INDEX PARTITION',
        'COUNT(*)': 1239
    }, {
        'OBJECT_TYPE': 'TABLE SUBPARTITION',
        'COUNT(*)': 76
    }, {
        'OBJECT_TYPE': 'TABLE PARTITION',
        'COUNT(*)': 940
    }, {
        'OBJECT_TYPE': 'SEQUENCE',
        'COUNT(*)': 4
    }, {
        'OBJECT_TYPE': 'PROCEDURE',
        'COUNT(*)': 66
    }, {
        'OBJECT_TYPE': 'LOB PARTITION',
        'COUNT(*)': 32
    }, {
        'OBJECT_TYPE': 'DATABASE LINK',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'INDEX SUBPARTITION',
        'COUNT(*)': 68
    }, {
        'OBJECT_TYPE': 'LOB',
        'COUNT(*)': 105
    }, {
        'OBJECT_TYPE': 'PACKAGE',
        'COUNT(*)': 29
    }, {
        'OBJECT_TYPE': 'PACKAGE BODY',
        'COUNT(*)': 28
    }, {
        'OBJECT_TYPE': 'TYPE BODY',
        'COUNT(*)': 5
    }, {
        'OBJECT_TYPE': 'TRIGGER',
        'COUNT(*)': 8
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 5534
    }, {
        'OBJECT_TYPE': 'INDEX',
        'COUNT(*)': 2238
    }, {
        'OBJECT_TYPE': 'VIEW',
        'COUNT(*)': 6
    }, {
        'OBJECT_TYPE': 'FUNCTION',
        'COUNT(*)': 20
    }, {
        'OBJECT_TYPE': 'TYPE',
        'COUNT(*)': 15
    }],
     "schema--table--sql: FMIS9912--dba_objects-- select object_type,count(*)  from dba_objects where owner='FMIS9912' group by object_type"),
    ([{
        'OBJECT_TYPE': 'LOB',
        'COUNT(*)': 384
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 2128
    }, {
        'OBJECT_TYPE': 'VIEW',
        'COUNT(*)': 2
    }],
     "schema--table--sql: HOSPITAL_YIB--dba_objects-- select object_type,count(*)  from dba_objects where owner='HOSPITAL_YIB' group by object_type"),
    ([],
     "schema--table--sql: HR--dba_objects-- select object_type,count(*)  from dba_objects where owner='HR' group by object_type"),
    ([{
        'OBJECT_TYPE': 'SEQUENCE',
        'COUNT(*)': 3461
    }, {
        'OBJECT_TYPE': 'LOB',
        'COUNT(*)': 4
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 201
    }],
     "schema--table--sql: HZERO--dba_objects-- select object_type,count(*)  from dba_objects where owner='HZERO' group by object_type"),
    ([{
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 1
    }],
     "schema--table--sql: JACK--dba_objects-- select object_type,count(*)  from dba_objects where owner='JACK' group by object_type"),
    ([{
        'OBJECT_TYPE': 'PROCEDURE',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 3
    }],
     "schema--table--sql: LIN--dba_objects-- select object_type,count(*)  from dba_objects where owner='LIN' group by object_type"),
    ([],
     "schema--table--sql: LIS--dba_objects-- select object_type,count(*)  from dba_objects where owner='LIS' group by object_type"),
    ([{
        'OBJECT_TYPE': 'TABLE PARTITION',
        'COUNT(*)': 10
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'INDEX',
        'COUNT(*)': 1
    }],
     "schema--table--sql: MIG_ORACLE--dba_objects-- select object_type,count(*)  from dba_objects where owner='MIG_ORACLE' group by object_type"),
    ([{
        'OBJECT_TYPE': 'PACKAGE BODY',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'PACKAGE',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 25
    }, {
        'OBJECT_TYPE': 'FUNCTION',
        'COUNT(*)': 2
    }],
     "schema--table--sql: OE--dba_objects-- select object_type,count(*)  from dba_objects where owner='OE' group by object_type"),
    ([],
     "schema--table--sql: SA--dba_objects-- select object_type,count(*)  from dba_objects where owner='SA' group by object_type"),
    ([{
        'OBJECT_TYPE': 'PACKAGE',
        'COUNT(*)': 2
    }, {
        'OBJECT_TYPE': 'PACKAGE BODY',
        'COUNT(*)': 2
    }, {
        'OBJECT_TYPE': 'TRIGGER',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 2
    }],
     "schema--table--sql: SCHEMAOWNER--dba_objects-- select object_type,count(*)  from dba_objects where owner='SCHEMAOWNER' group by object_type"),
    ([{
        'OBJECT_TYPE': 'PACKAGE',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'PACKAGE BODY',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'INDEX',
        'COUNT(*)': 2
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 5
    }],
     "schema--table--sql: SCOTT--dba_objects-- select object_type,count(*)  from dba_objects where owner='SCOTT' group by object_type"),
    ([{
        'OBJECT_TYPE': 'PACKAGE',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'PACKAGE BODY',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'LOB',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'TRIGGER',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'FUNCTION',
        'COUNT(*)': 2
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 2
    }],
     "schema--table--sql: SCOTT_B--dba_objects-- select object_type,count(*)  from dba_objects where owner='SCOTT_B' group by object_type"),
    ([],
     "schema--table--sql: SECUSR--dba_objects-- select object_type,count(*)  from dba_objects where owner='SECUSR' group by object_type"),
    ([{
        'OBJECT_TYPE': 'LOB',
        'COUNT(*)': 2
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 66
    }, {
        'OBJECT_TYPE': 'INDEX',
        'COUNT(*)': 45
    }],
     "schema--table--sql: SYNCER--dba_objects-- select object_type,count(*)  from dba_objects where owner='SYNCER' group by object_type"),
    ([{
        'OBJECT_TYPE': 'EDITION',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'INDEX PARTITION',
        'COUNT(*)': 212
    }, {
        'OBJECT_TYPE': 'CONSUMER GROUP',
        'COUNT(*)': 25
    }, {
        'OBJECT_TYPE': 'TABLE SUBPARTITION',
        'COUNT(*)': 4
    }, {
        'OBJECT_TYPE': 'SEQUENCE',
        'COUNT(*)': 135
    }, {
        'OBJECT_TYPE': 'TABLE PARTITION',
        'COUNT(*)': 209
    }, {
        'OBJECT_TYPE': 'SCHEDULE',
        'COUNT(*)': 3
    }, {
        'OBJECT_TYPE': 'QUEUE',
        'COUNT(*)': 17
    }, {
        'OBJECT_TYPE': 'RULE',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'JAVA DATA',
        'COUNT(*)': 328
    }, {
        'OBJECT_TYPE': 'PROCEDURE',
        'COUNT(*)': 101
    }, {
        'OBJECT_TYPE': 'OPERATOR',
        'COUNT(*)': 7
    }, {
        'OBJECT_TYPE': 'LOB PARTITION',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'DESTINATION',
        'COUNT(*)': 2
    }, {
        'OBJECT_TYPE': 'WINDOW',
        'COUNT(*)': 9
    }, {
        'OBJECT_TYPE': 'SCHEDULER GROUP',
        'COUNT(*)': 4
    }, {
        'OBJECT_TYPE': 'LOB',
        'COUNT(*)': 153
    }, {
        'OBJECT_TYPE': 'PACKAGE',
        'COUNT(*)': 623
    }, {
        'OBJECT_TYPE': 'PACKAGE BODY',
        'COUNT(*)': 597
    }, {
        'OBJECT_TYPE': 'LIBRARY',
        'COUNT(*)': 144
    }, {
        'OBJECT_TYPE': 'PROGRAM',
        'COUNT(*)': 19
    }, {
        'OBJECT_TYPE': 'RULE SET',
        'COUNT(*)': 13
    }, {
        'OBJECT_TYPE': 'CONTEXT',
        'COUNT(*)': 10
    }, {
        'OBJECT_TYPE': 'TYPE BODY',
        'COUNT(*)': 113
    }, {
        'OBJECT_TYPE': 'JAVA RESOURCE',
        'COUNT(*)': 760
    }, {
        'OBJECT_TYPE': 'TRIGGER',
        'COUNT(*)': 11
    }, {
        'OBJECT_TYPE': 'JOB CLASS',
        'COUNT(*)': 13
    }, {
        'OBJECT_TYPE': 'UNDEFINED',
        'COUNT(*)': 9
    }, {
        'OBJECT_TYPE': 'DIRECTORY',
        'COUNT(*)': 5
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 972
    }, {
        'OBJECT_TYPE': 'INDEX',
        'COUNT(*)': 996
    }, {
        'OBJECT_TYPE': 'VIEW',
        'COUNT(*)': 3748
    }, {
        'OBJECT_TYPE': 'FUNCTION',
        'COUNT(*)': 99
    }, {
        'OBJECT_TYPE': 'SYNONYM',
        'COUNT(*)': 9
    }, {
        'OBJECT_TYPE': 'JAVA CLASS',
        'COUNT(*)': 20444
    }, {
        'OBJECT_TYPE': 'JAVA SOURCE',
        'COUNT(*)': 2
    }, {
        'OBJECT_TYPE': 'CLUSTER',
        'COUNT(*)': 10
    }, {
        'OBJECT_TYPE': 'TYPE',
        'COUNT(*)': 1306
    }, {
        'OBJECT_TYPE': 'RESOURCE PLAN',
        'COUNT(*)': 10
    }, {
        'OBJECT_TYPE': 'JOB',
        'COUNT(*)': 10
    }, {
        'OBJECT_TYPE': 'EVALUATION CONTEXT',
        'COUNT(*)': 9
    }],
     "schema--table--sql: SYS--dba_objects-- select object_type,count(*)  from dba_objects where owner='SYS' group by object_type"),
    ([{
        'OBJECT_TYPE': 'INDEX PARTITION',
        'COUNT(*)': 52
    }, {
        'OBJECT_TYPE': 'SEQUENCE',
        'COUNT(*)': 20
    }, {
        'OBJECT_TYPE': 'TABLE PARTITION',
        'COUNT(*)': 39
    }, {
        'OBJECT_TYPE': 'QUEUE',
        'COUNT(*)': 4
    }, {
        'OBJECT_TYPE': 'PROCEDURE',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'LOB',
        'COUNT(*)': 22
    }, {
        'OBJECT_TYPE': 'PACKAGE',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'PACKAGE BODY',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'TRIGGER',
        'COUNT(*)': 2
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 156
    }, {
        'OBJECT_TYPE': 'INDEX',
        'COUNT(*)': 210
    }, {
        'OBJECT_TYPE': 'SYNONYM',
        'COUNT(*)': 8
    }, {
        'OBJECT_TYPE': 'VIEW',
        'COUNT(*)': 12
    }, {
        'OBJECT_TYPE': 'TYPE',
        'COUNT(*)': 1
    }],
     "schema--table--sql: SYSTEM--dba_objects-- select object_type,count(*)  from dba_objects where owner='SYSTEM' group by object_type"),
    ([],
     "schema--table--sql: TCZX_TY_AOBOIP--dba_objects-- select object_type,count(*)  from dba_objects where owner='TCZX_TY_AOBOIP' group by object_type"),
    ([{
        'OBJECT_TYPE': 'SEQUENCE',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'SYNONYM',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 3
    }],
     "schema--table--sql: TEST--dba_objects-- select object_type,count(*)  from dba_objects where owner='TEST' group by object_type"),
    ([{
        'OBJECT_TYPE': 'LOB',
        'COUNT(*)': 383
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 2136
    }, {
        'OBJECT_TYPE': 'INDEX',
        'COUNT(*)': 24
    }],
     "schema--table--sql: TESTUSER1--dba_objects-- select object_type,count(*)  from dba_objects where owner='TESTUSER1' group by object_type"),
    ([{
        'OBJECT_TYPE': 'PROCEDURE',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'PACKAGE BODY',
        'COUNT(*)': 2
    }, {
        'OBJECT_TYPE': 'PACKAGE',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'VIEW',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'FUNCTION',
        'COUNT(*)': 2
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 32
    }, {
        'OBJECT_TYPE': 'SYNONYM',
        'COUNT(*)': 2
    }, {
        'OBJECT_TYPE': 'INDEX',
        'COUNT(*)': 26
    }, {
        'OBJECT_TYPE': 'TYPE',
        'COUNT(*)': 7
    }],
     "schema--table--sql: TEST_USER--dba_objects-- select object_type,count(*)  from dba_objects where owner='TEST_USER' group by object_type"),
    ([{
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 28
    }, {
        'OBJECT_TYPE': 'INDEX',
        'COUNT(*)': 4
    }],
     "schema--table--sql: TPCDS--dba_objects-- select object_type,count(*)  from dba_objects where owner='TPCDS' group by object_type"),
    ([{
        'OBJECT_TYPE': 'PROCEDURE',
        'COUNT(*)': 323
    }, {
        'OBJECT_TYPE': 'PACKAGE',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 3
    }],
     "schema--table--sql: TXJT--dba_objects-- select object_type,count(*)  from dba_objects where owner='TXJT' group by object_type"),
    ([{
        'OBJECT_TYPE': 'SEQUENCE',
        'COUNT(*)': 20
    }, {
        'OBJECT_TYPE': 'TABLE PARTITION',
        'COUNT(*)': 38
    }, {
        'OBJECT_TYPE': 'PROCEDURE',
        'COUNT(*)': 3
    }, {
        'OBJECT_TYPE': 'LOB',
        'COUNT(*)': 20
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 119
    }, {
        'OBJECT_TYPE': 'INDEX',
        'COUNT(*)': 79
    }, {
        'OBJECT_TYPE': 'FUNCTION',
        'COUNT(*)': 3
    }, {
        'OBJECT_TYPE': 'VIEW',
        'COUNT(*)': 1
    }],
     "schema--table--sql: U1--dba_objects-- select object_type,count(*)  from dba_objects where owner='U1' group by object_type"),
    ([{
        'OBJECT_TYPE': 'TABLE SUBPARTITION',
        'COUNT(*)': 28
    }, {
        'OBJECT_TYPE': 'INDEX PARTITION',
        'COUNT(*)': 12
    }, {
        'OBJECT_TYPE': 'TABLE PARTITION',
        'COUNT(*)': 64
    }, {
        'OBJECT_TYPE': 'SEQUENCE',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'PROCEDURE',
        'COUNT(*)': 16
    }, {
        'OBJECT_TYPE': 'LOB',
        'COUNT(*)': 11
    }, {
        'OBJECT_TYPE': 'TRIGGER',
        'COUNT(*)': 17
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 82
    }, {
        'OBJECT_TYPE': 'INDEX',
        'COUNT(*)': 35
    }, {
        'OBJECT_TYPE': 'FUNCTION',
        'COUNT(*)': 4
    }, {
        'OBJECT_TYPE': 'VIEW',
        'COUNT(*)': 4
    }],
     "schema--table--sql: U2--dba_objects-- select object_type,count(*)  from dba_objects where owner='U2' group by object_type"),
    ([{
        'OBJECT_TYPE': 'TABLE PARTITION',
        'COUNT(*)': 4
    }, {
        'OBJECT_TYPE': 'PROCEDURE',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'LOB',
        'COUNT(*)': 7
    }, {
        'OBJECT_TYPE': 'INDEX',
        'COUNT(*)': 14
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 21
    }],
     "schema--table--sql: U3--dba_objects-- select object_type,count(*)  from dba_objects where owner='U3' group by object_type"),
    ([],
     "schema--table--sql: UBR--dba_objects-- select object_type,count(*)  from dba_objects where owner='UBR' group by object_type"),
    ([{
        'OBJECT_TYPE': 'TABLE SUBPARTITION',
        'COUNT(*)': 9
    }, {
        'OBJECT_TYPE': 'INDEX PARTITION',
        'COUNT(*)': 2
    }, {
        'OBJECT_TYPE': 'TABLE PARTITION',
        'COUNT(*)': 12
    }, {
        'OBJECT_TYPE': 'SEQUENCE',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'INDEX SUBPARTITION',
        'COUNT(*)': 5
    }, {
        'OBJECT_TYPE': 'LOB',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'TRIGGER',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 50
    }, {
        'OBJECT_TYPE': 'INDEX',
        'COUNT(*)': 9
    }, {
        'OBJECT_TYPE': 'FUNCTION',
        'COUNT(*)': 2
    }],
     "schema--table--sql: UM--dba_objects-- select object_type,count(*)  from dba_objects where owner='UM' group by object_type"),
    ([],
     "schema--table--sql: USER1--dba_objects-- select object_type,count(*)  from dba_objects where owner='USER1' group by object_type"),
    ([],
     "schema--table--sql: USER2--dba_objects-- select object_type,count(*)  from dba_objects where owner='USER2' group by object_type"),
    ([{
        'OBJECT_TYPE': 'TABLE PARTITION',
        'COUNT(*)': 4
    }, {
        'OBJECT_TYPE': 'PROCEDURE',
        'COUNT(*)': 9
    }, {
        'OBJECT_TYPE': 'PACKAGE BODY',
        'COUNT(*)': 5
    }, {
        'OBJECT_TYPE': 'PACKAGE',
        'COUNT(*)': 6
    }, {
        'OBJECT_TYPE': 'LOB',
        'COUNT(*)': 4
    }, {
        'OBJECT_TYPE': 'TYPE BODY',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'TRIGGER',
        'COUNT(*)': 8
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 143
    }, {
        'OBJECT_TYPE': 'INDEX',
        'COUNT(*)': 29
    }, {
        'OBJECT_TYPE': 'VIEW',
        'COUNT(*)': 3
    }, {
        'OBJECT_TYPE': 'FUNCTION',
        'COUNT(*)': 14
    }, {
        'OBJECT_TYPE': 'TYPE',
        'COUNT(*)': 13
    }],
     "schema--table--sql: USO--dba_objects-- select object_type,count(*)  from dba_objects where owner='USO' group by object_type"),
    ([{
        'OBJECT_TYPE': 'INDEX',
        'COUNT(*)': 7
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 6
    }, {
        'OBJECT_TYPE': 'VIEW',
        'COUNT(*)': 2
    }],
     "schema--table--sql: USR_SOD--dba_objects-- select object_type,count(*)  from dba_objects where owner='USR_SOD' group by object_type"),
    ([{
        'OBJECT_TYPE': 'PACKAGE',
        'COUNT(*)': 3
    }, {
        'OBJECT_TYPE': 'PACKAGE BODY',
        'COUNT(*)': 3
    }, {
        'OBJECT_TYPE': 'INDEX',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 6
    }],
     "schema--table--sql: UU--dba_objects-- select object_type,count(*)  from dba_objects where owner='UU' group by object_type"),
    ([],
     "schema--table--sql: UU1--dba_objects-- select object_type,count(*)  from dba_objects where owner='UU1' group by object_type"),
    ([{
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 20000
    }],
     "schema--table--sql: U_MANY--dba_objects-- select object_type,count(*)  from dba_objects where owner='U_MANY' group by object_type"),
    ([{
        'OBJECT_TYPE': 'SEQUENCE',
        'COUNT(*)': 4
    }, {
        'OBJECT_TYPE': 'PROCEDURE',
        'COUNT(*)': 5
    }, {
        'OBJECT_TYPE': 'LOB',
        'COUNT(*)': 13
    }, {
        'OBJECT_TYPE': 'TRIGGER',
        'COUNT(*)': 1
    }, {
        'OBJECT_TYPE': 'FUNCTION',
        'COUNT(*)': 2
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 106
    }, {
        'OBJECT_TYPE': 'INDEX',
        'COUNT(*)': 248
    }],
     "schema--table--sql: U_SYNC--dba_objects-- select object_type,count(*)  from dba_objects where owner='U_SYNC' group by object_type"),
    ([],
     "schema--table--sql: VPD--dba_objects-- select object_type,count(*)  from dba_objects where owner='VPD' group by object_type"),
    ([{
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 1
    }],
     "schema--table--sql: WHD--dba_objects-- select object_type,count(*)  from dba_objects where owner='WHD' group by object_type"),
    ([{
        'OBJECT_TYPE': 'INDEX',
        'COUNT(*)': 2
    }, {
        'OBJECT_TYPE': 'TABLE',
        'COUNT(*)': 3
    }],
     "schema--table--sql: Z1--dba_objects-- select object_type,count(*)  from dba_objects where owner='Z1' group by object_type")]

for i in data:
    print(i)