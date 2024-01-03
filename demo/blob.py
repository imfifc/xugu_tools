import argparse
import concurrent.futures
import multiprocessing
import random
import re
import numpy as np
import sys
import os
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime, timedelta
from multiprocessing import freeze_support

import xgcondb

"""
雷达数据 
数值预报
"""


# 连接字符串请视情况而定，输入自己本地的数据库服务器IP地址、端口等
# conn = xgcondb.connect(host="127.0.0.1", port="5138", database="SYSTEM", user="SYSDBA", password="SYSDBA")
# cur = conn.cursor()
# 大对象导入
# cur.execute("select count(*) from all_tables where table_name='TEST_LOB';")
# row = cur.fetchone()
# if row[0] == 1:
#     cur.execute("drop table test_lob;")
# cur.execute("create table test_lob(col1 int,  col2 blob);")

# blob_buf = open("./xg_lob/test_blob.jpg", "rb").read()
# cur.cleartype()
# cur.setinputtype((xgcondb.XG_C_INTEGER, xgcondb.XG_C_BLOB))
# cur.execute("insert into test_lob values(?,?);", (1, blob_buf))
# cur.execute("select len(col1),len(col2) from test_lob where col1=1;")
# row = cur.fetchone()
# # print(row)
# print("CLOB length is", row[0])
# print("BLOB length is", row[1])

# 大对象导出
# cur.execute("select * from test_lob;")
# row = cur.fetchone()
# blob_fd = open("./xg_lob/getBlob.jpg", "wb+")
# blob_fd.write(row[1])
# if len(blob_buf) == len(row[1]):
#     print("")
#     print("The large object was successfully exported with the same length of data")
def parse_args():
    parser = argparse.ArgumentParser(
        description='基于execute_many实现',
        prefix_chars='-'
    )
    # 添加位置参数
    # parser.add_argument('input_file', help='输入文件的路径')
    # 添加可选参数
    # parser.add_argument('-o', '--output', help='输出文件的路径')
    parser.add_argument('-H', '--host', help='输入数据库ip地址')
    parser.add_argument('-P', '--port', help='Port number 数据库端口', type=int, default=5138)
    parser.add_argument('-u', '--user', help='输入数据库 用户')
    parser.add_argument('-p', '--pwd', help='输入数据库密码')
    parser.add_argument('-d', '--database_name', help='输入数据库名')
    parser.add_argument('-f', '--file', help='输入blob文件绝对路径')
    # 添加标志参数
    parser.add_argument('-v', '--verbose', action='store_true', help='是否显示详细信息')
    args = parser.parse_args()
    # 访问解析后的参数
    # input_file = args.input_file
    # output_file = args.output
    host = args.host
    port = args.port
    user = args.user
    password = args.pwd
    db = args.database_name
    verbose = args.verbose
    path = args.file

    # 在这里可以根据解析的参数执行相应的操作
    if len(sys.argv) == 1:
        host = input("请输入ip: ")
        port = input("请输入端口: ")
        user = input("请输入用户: ")
        password = input("请输入密码: ")
        db = input("请输入数据库: ")
        path = input("输入blob文件绝对路径: ")
    if verbose:
        print("显示详细信息")
    if not host:
        parser.print_help()
        raise Exception('没有输入ip !!!\n')
    if not port:
        parser.print_help()
        raise Exception('没有输入port !!!\n')
    if not user:
        parser.print_help()
        raise Exception('没有输入user !!!\n')
    if not password:
        parser.print_help()
        raise Exception('没有输入password !!!\n')
    if not db:
        parser.print_help()
        raise Exception('没有输入数据库 !!!\n')
    if not path:
        parser.print_help()
        raise Exception('没有输入blob 文件路径 !!!\n')
    # if host and port and user and password and db:
    #     print(f'host: {host} port: {port} user: {user} password: {password} db: {db} \n')
    print(host, port, user, password, db, path)
    return host, port, user, password, db, path.strip()


def get_cur(db_config):
    conn = xgcondb.connect(host=db_config['db_host'], port=db_config['db_port'], database=db_config['db_name'],
                           user=db_config['db_user'], password=db_config['db_pwd'], charset="utf8")
    cur = conn.cursor()
    return cur


def rebuild_table(table, db_config):
    """"binary 最大只能装64k"""
    cur = get_cur(db_config)
    sql = f"drop table if exists {table} cascade"
    sql2 = f""" 
    CREATE TABLE {table} (
        X_ZHOU SMALLINT COMMENT 'X轴',
        Y_ZHOU SMALLINT COMMENT 'Y轴',
        XY SMALLINT COMMENT 'XY',
        val_data blob COMMENT '数值',
        high_level SMALLINT COMMENT '高度',
        val_time TIMESTAMP  COMMENT '资料时间',
        validtime TIMESTAMP COMMENT '预报时效',
        strat_time  TIMESTAMP COMMENT '起报时效'
        )PARTITION BY list(high_level) PARTITIONS 
        (PART1 values(1),
        PART2 values(100),
        PART3 values(200),
        PART4 values(300),
        PART5 values(400),
        PART6 values(500),
        PART7 values(600),
        PART8 values(700),
        PART9 values(800),
        PART10 values(900),
        PART11 values(1000),
        PART12 values(1100),
        PART13 values(1200),
        PART14 values(1300),
        PART15 values(1400),
        PART16 values(1500),
        PART17 values(1600),
        PART18 values(1700),
        PART19 values(1800),
        PART20 values(1900),
        PART21 values(2000)) hotspot 20 copy number 1 COMMENT '比湿数值预报表' """
    try:
        cur.execute(sql)
    except Exception as e:
        print(e)
    cur.execute(sql2)


def rebuild_table2(table, db_config):
    """位置服务，7个要素"""
    cur = get_cur(db_config)
    sql = f"drop table if exists {table} cascade"
    sql2 = f"""
    create table {table} (
        val_time TIMESTAMP  COMMENT '资料时间',
        lat_lon INT COMMENT '维度', 	
        ele_type VARCHAR COMMENT '要素类型', 
        lon_00 binary COMMENT '偏移00要素值', 
        lon_01 binary COMMENT '偏移01要素值',
        lon_02 binary COMMENT '偏移02要素值',
        lon_03 binary COMMENT '偏移03要素值',
        lon_04 binary COMMENT '偏移04要素值',
        lon_05 binary COMMENT '偏移05要素值',
        lon_06 binary COMMENT '偏移06要素值',
        lon_07 binary COMMENT '偏移07要素值',
        lon_08 binary COMMENT '偏移08要素值',
        lon_09 binary COMMENT '偏移09要素值',
        lon_10 binary COMMENT '偏移10要素值',
        lon_11 binary COMMENT '偏移11要素值',
        lon_12 binary COMMENT '偏移12要素值',
        lon_13 binary COMMENT '偏移13要素值',
        lon_14 binary COMMENT '偏移14要素值',
        lon_15 binary COMMENT '偏移15要素值',
        lon_16 binary COMMENT '偏移16要素值',
        lon_17 binary COMMENT '偏移17要素值',
        lon_18 binary COMMENT '偏移18要素值',
        lon_19 binary COMMENT '偏移19要素值',
        lon_20 binary COMMENT '偏移20要素值',
        lon_21 binary COMMENT '偏移21要素值',
        lon_22 binary COMMENT '偏移22要素值',
        lon_23 binary COMMENT '偏移23要素值',
        lon_24 binary COMMENT '偏移24要素值',
        lon_25 binary COMMENT '偏移25要素值',
        lon_26 binary COMMENT '偏移26要素值',
        lon_27 binary COMMENT '偏移27要素值',
        lon_28 binary COMMENT '偏移28要素值',
        lon_29 binary COMMENT '偏移29要素值',
        lon_30 binary COMMENT '偏移30要素值',
        lon_31 binary COMMENT '偏移31要素值',
        lon_32 binary COMMENT '偏移32要素值',
        lon_33 binary COMMENT '偏移33要素值',
        lon_34 binary COMMENT '偏移34要素值',
        lon_35 binary COMMENT '偏移35要素值',
        lon_36 binary COMMENT '偏移36要素值',
        lon_37 binary COMMENT '偏移37要素值',
        lon_38 binary COMMENT '偏移38要素值',
        lon_39 binary COMMENT '偏移39要素值',
        lon_40 binary COMMENT '偏移40要素值',
        lon_41 binary COMMENT '偏移41要素值',
        lon_42 binary COMMENT '偏移42要素值',
        lon_43 binary COMMENT '偏移43要素值',
        lon_44 binary COMMENT '偏移44要素值',
        lon_45 binary COMMENT '偏移45要素值',
        lon_46 binary COMMENT '偏移46要素值',
        lon_47 binary COMMENT '偏移47要素值',
        lon_48 binary COMMENT '偏移48要素值',
        lon_49 binary COMMENT '偏移49要素值',
        lon_50 binary COMMENT '偏移50要素值',
        lon_51 binary COMMENT '偏移51要素值',
        lon_52 binary COMMENT '偏移52要素值',
        lon_53 binary COMMENT '偏移53要素值',
        lon_54 binary COMMENT '偏移54要素值',
        lon_55 binary COMMENT '偏移55要素值',
        lon_56 binary COMMENT '偏移56要素值',
        lon_57 binary COMMENT '偏移57要素值',
        lon_58 binary COMMENT '偏移58要素值',
        lon_59 binary COMMENT '偏移59要素值',
        lon_60 binary COMMENT '偏移60要素值',
        lon_61 binary COMMENT '偏移61要素值',
        lon_62 binary COMMENT '偏移62要素值',
        lon_63 binary COMMENT '偏移63要素值',
        lon_64 binary COMMENT '偏移64要素值',
        lon_65 binary COMMENT '偏移65要素值',
        lon_66 binary COMMENT '偏移66要素值',
        lon_67 binary COMMENT '偏移67要素值',
        lon_68 binary COMMENT '偏移68要素值',
        lon_69 binary COMMENT '偏移69要素值',
        lon_70 binary COMMENT '偏移70要素值',
        lon_71 binary COMMENT '偏移71要素值',
        lon_72 binary COMMENT '偏移72要素值',
        lon_73 binary COMMENT '偏移73要素值',
        lon_74 binary COMMENT '偏移74要素值',
        lon_75 binary COMMENT '偏移75要素值',
        lon_76 binary COMMENT '偏移76要素值',
        lon_77 binary COMMENT '偏移77要素值',
        lon_78 binary COMMENT '偏移78要素值',
        lon_79 binary COMMENT '偏移79要素值',
        lon_80 binary COMMENT '偏移80要素值',
        lon_81 binary COMMENT '偏移81要素值',
        lon_82 binary COMMENT '偏移82要素值',
        lon_83 binary COMMENT '偏移83要素值',
        lon_84 binary COMMENT '偏移84要素值',
        lon_85 binary COMMENT '偏移85要素值',
        lon_86 binary COMMENT '偏移86要素值',
        lon_87 binary COMMENT '偏移87要素值',
        lon_88 binary COMMENT '偏移88要素值',
        lon_89 binary COMMENT '偏移89要素值',
        lon_90 binary COMMENT '偏移90要素值',
        lon_91 binary COMMENT '偏移91要素值',
        lon_92 binary COMMENT '偏移92要素值',
        lon_93 binary COMMENT '偏移93要素值',
        lon_94 binary COMMENT '偏移94要素值',
        lon_95 binary COMMENT '偏移95要素值',
        lon_96 binary COMMENT '偏移96要素值',
        lon_97 binary COMMENT '偏移97要素值',
        lon_98 binary COMMENT '偏移98要素值',
        lon_99 binary COMMENT '偏移99要素值')
        PARTITION BY RANGE(lat_lon) PARTITIONS
        (PART1 VALUES LESS THAN (100141),
        PART2 VALUES LESS THAN (200141),
        PART3 VALUES LESS THAN (300141),
        PART4 VALUES LESS THAN (400141),
        PART5 VALUES LESS THAN (500141),
        PART6 VALUES LESS THAN (600141),
        PART7 VALUES LESS THAN (700141),
        PART8 VALUES LESS THAN (800141),
        PART9 VALUES LESS THAN (900141),
        PART10 VALUES LESS THAN (1000141),
        PART11 VALUES LESS THAN (1100141),
        PART12 VALUES LESS THAN (1200141),
        PART13 VALUES LESS THAN (1300141),
        PART14 VALUES LESS THAN (1400141),
        PART15 VALUES LESS THAN (1500141),
        PART16 VALUES LESS THAN (1600141),
        PART17 VALUES LESS THAN (1700141),
        PART18 VALUES LESS THAN (1800141),
        PART19 VALUES LESS THAN (1900141),
        PART20 VALUES LESS THAN (2000141),
        PART21 VALUES LESS THAN (2100141),
        PART22 VALUES LESS THAN (2200141),
        PART23 VALUES LESS THAN (2300141),
        PART24 VALUES LESS THAN (2400141),
        PART25 VALUES LESS THAN (2500141),
        PART26 VALUES LESS THAN (2600141),
        PART27 VALUES LESS THAN (2700141),
        PART28 VALUES LESS THAN (2800141),
        PART29 VALUES LESS THAN (2900141),
        PART30 VALUES LESS THAN (3000141),
        PART31 VALUES LESS THAN (3100141),
        PART32 VALUES LESS THAN (3200141),
        PART33 VALUES LESS THAN (3300141),
        PART34 VALUES LESS THAN (3400141),
        PART35 VALUES LESS THAN (3500141),
        PART36 VALUES LESS THAN (3600141),
        PART37 VALUES LESS THAN (3700141),
        PART38 VALUES LESS THAN (3800141),
        PART39 VALUES LESS THAN (3900141),
        PART40 VALUES LESS THAN (4000141),
        PART41 VALUES LESS THAN (4100141),
        PART42 VALUES LESS THAN (4200141),
        PART43 VALUES LESS THAN (4300141),
        PART44 VALUES LESS THAN (4400141),
        PART45 VALUES LESS THAN (4500141),
        PART46 VALUES LESS THAN (4600141),
        PART47 VALUES LESS THAN (4700141),
        PART48 VALUES LESS THAN (4800141),
        PART49 VALUES LESS THAN (4900141),
        PART50 VALUES LESS THAN (5000141),
        PART51 VALUES LESS THAN (5100141),
        PART52 VALUES LESS THAN (5200141),
        PART53 VALUES LESS THAN (5300141),
        PART54 VALUES LESS THAN (5400141),
        PART55 VALUES LESS THAN (5500141),
        PART56 VALUES LESS THAN (5600141),
        PART57 VALUES LESS THAN (5700141),
        PART58 VALUES LESS THAN (5800141),
        PART59 VALUES LESS THAN (5900141),
        PART60 VALUES LESS THAN (6000141)
        ) hotspot 20 copy number 1;
    """
    try:
        cur.execute(sql)
    except Exception as e:
        print(e)
    cur.execute(sql2)


def rebuild_radr_tab(table, db_config):
    cur = get_cur(db_config)
    sql = f"drop table if exists {table} cascade"
    sql2 = f"""
    CREATE TABLE {table} (
        AREA VARCHAR COMMENT '国家/区域',
        FILE_NAME VARCHAR COMMENT '文件名',
        SITE_ID VARCHAR  COMMENT '站点号',
        REP_TIME TIMESTAMP COMMENT '上报时间',
        FILE_TIME  TIMESTAMP COMMENT '资料时间',
        FILE_FMT VARCHAR COMMENT '文件格式',
        FILE_DATA BLOB COMMENT '流数据文件'
        )PARTITION BY RANGE(FILE_TIME) PARTITIONS
        (PART1 VALUES LESS THAN ('2023-12-01 23:59:59'),
        PART2 VALUES LESS THAN ('2023-12-02 23:59:59'),
        PART3 VALUES LESS THAN ('2023-12-03 23:59:59'),
        PART4 VALUES LESS THAN ('2023-12-04 23:59:59'),
        PART5 VALUES LESS THAN ('2023-12-05 23:59:59'),
        PART6 VALUES LESS THAN ('2023-12-06 23:59:59'),
        PART7 VALUES LESS THAN ('2023-12-07 23:59:59'),
        PART8 VALUES LESS THAN ('2023-12-08 23:59:59'),
        PART9 VALUES LESS THAN ('2023-12-09 23:59:59'),
        PART10 VALUES LESS THAN ('2023-12-10 23:59:59'),
        PART11 VALUES LESS THAN ('2023-12-11 23:59:59'),
        PART12 VALUES LESS THAN ('2023-12-12 23:59:59'),
        PART13 VALUES LESS THAN ('2023-12-13 23:59:59'),
        PART14 VALUES LESS THAN ('2023-12-14 23:59:59'),
        PART15 VALUES LESS THAN ('2023-12-15 23:59:59'),
        PART16 VALUES LESS THAN ('2023-12-16 23:59:59'),
        PART17 VALUES LESS THAN ('2023-12-17 23:59:59'),
        PART18 VALUES LESS THAN ('2023-12-18 23:59:59'),
        PART19 VALUES LESS THAN ('2023-12-19 23:59:59'),
        PART20 VALUES LESS THAN ('2023-12-20 23:59:59'),
        PART21 VALUES LESS THAN ('2023-12-21 23:59:59'),
        PART22 VALUES LESS THAN ('2023-12-22 23:59:59'),
        PART23 VALUES LESS THAN ('2023-12-23 23:59:59'),
        PART24 VALUES LESS THAN ('2023-12-24 23:59:59'),
        PART25 VALUES LESS THAN ('2023-12-25 23:59:59'),
        PART26 VALUES LESS THAN ('2023-12-26 23:59:59'),
        PART27 VALUES LESS THAN ('2023-12-27 23:59:59'),
        PART28 VALUES LESS THAN ('2023-12-28 23:59:59'),
        PART29 VALUES LESS THAN ('2023-12-29 23:59:59'),
        PART30 VALUES LESS THAN ('2023-12-30 23:59:59')) hotspot 20 copy number 2 COMMENT '雷达Z9745站点表'  ;
    """
    try:
        cur.execute(sql)
    except Exception as e:
        print(e)
    cur.execute(sql2)


def rebuild_radr_tab2(table, db_config):
    """
    SITE_ID   --6个值 子分区
    FILE_TIME --6分钟一次  一天240次 30天7200次
    FILE_DATA BLOB COMMENT '流数据文件'  --文件按站点号对应
    :param table:
    :param db_config:
    :return:
    """
    cur = get_cur(db_config)
    sql = f"drop table if exists {table} cascade"
    sql2 = f"""
    CREATE TABLE {table} (
        AREA VARCHAR COMMENT '国家/区域',
        FILE_NAME VARCHAR COMMENT '文件名',
        SITE_ID VARCHAR  COMMENT '站点号',  
        REP_TIME TIMESTAMP COMMENT '上报时间',
        FILE_TIME  TIMESTAMP COMMENT '资料时间',
        FILE_FMT VARCHAR COMMENT '文件格式',
        FILE_DATA BLOB COMMENT '流数据文件'  
        )PARTITION BY RANGE(FILE_TIME) PARTITIONS
        (PART1 VALUES LESS THAN ('2023-12-01 23:59:59'),
        PART2 VALUES LESS THAN ('2023-12-02 23:59:59'),
        PART3 VALUES LESS THAN ('2023-12-03 23:59:59'),
        PART4 VALUES LESS THAN ('2023-12-04 23:59:59'),
        PART5 VALUES LESS THAN ('2023-12-05 23:59:59'),
        PART6 VALUES LESS THAN ('2023-12-06 23:59:59'),
        PART7 VALUES LESS THAN ('2023-12-07 23:59:59'),
        PART8 VALUES LESS THAN ('2023-12-08 23:59:59'),
        PART9 VALUES LESS THAN ('2023-12-09 23:59:59'),
        PART10 VALUES LESS THAN ('2023-12-10 23:59:59'),
        PART11 VALUES LESS THAN ('2023-12-11 23:59:59'),
        PART12 VALUES LESS THAN ('2023-12-12 23:59:59'),
        PART13 VALUES LESS THAN ('2023-12-13 23:59:59'),
        PART14 VALUES LESS THAN ('2023-12-14 23:59:59'),
        PART15 VALUES LESS THAN ('2023-12-15 23:59:59'),
        PART16 VALUES LESS THAN ('2023-12-16 23:59:59'),
        PART17 VALUES LESS THAN ('2023-12-17 23:59:59'),
        PART18 VALUES LESS THAN ('2023-12-18 23:59:59'),
        PART19 VALUES LESS THAN ('2023-12-19 23:59:59'),
        PART20 VALUES LESS THAN ('2023-12-20 23:59:59'),
        PART21 VALUES LESS THAN ('2023-12-21 23:59:59'),
        PART22 VALUES LESS THAN ('2023-12-22 23:59:59'),
        PART23 VALUES LESS THAN ('2023-12-23 23:59:59'),
        PART24 VALUES LESS THAN ('2023-12-24 23:59:59'),
        PART25 VALUES LESS THAN ('2023-12-25 23:59:59'),
        PART26 VALUES LESS THAN ('2023-12-26 23:59:59'),
        PART27 VALUES LESS THAN ('2023-12-27 23:59:59'),
        PART28 VALUES LESS THAN ('2023-12-28 23:59:59'),
        PART29 VALUES LESS THAN ('2023-12-29 23:59:59'),
        PART30 VALUES LESS THAN ('2023-12-30 23:59:59'))
        SUBPARTITION BY LIST(SITE_ID) SUBPARTITIONS(
        subpart1 VALUES ('Z9513'),
        subpart2 VALUES ('Z9745'),
        subpart3 VALUES ('Z9770'),
        subpart4 VALUES ('Z9797'),
        subpart5 VALUES ('Z9873'),
        subpart6 VALUES ('Z9909')) hotspot 20 copy number 2 COMMENT '雷达Z9745站点表'  ;
    """
    try:
        cur.execute(sql)
    except Exception as e:
        print(e)
    cur.execute(sql2)


def update_tab2(ele, num, fields, path, table, db_config):
    """
    更新位置服务表
    num: 更新行数
    :param table:
    :param db_config:
    :return:
    """
    cur = get_cur(db_config)
    blob_buf = open(path, "rb").read()

    if fields == 100:
        sql = f"update {table} set lon_00=?,lon_01=?,lon_02=?,lon_03=?,lon_04=?,lon_05=?,lon_06=?,lon_07=?,lon_08=?,lon_09=?,lon_10=?,lon_11=?,lon_12=?,lon_13=?,lon_14=?,lon_15=?,lon_16=?,lon_17=?,lon_18=?,lon_19=?,lon_20=?,lon_21=?,lon_22=?,lon_23=?,lon_24=?,lon_25=?,lon_26=?,lon_27=?,lon_28=?,lon_29=?,lon_30=?,lon_31=?,lon_32=?,lon_33=?,lon_34=?,lon_35=?,lon_36=?,lon_37=?,lon_38=?,lon_39=?,lon_40=?,lon_41=?,lon_42=?,lon_43=?,lon_44=?,lon_45=?,lon_46=?,lon_47=?,lon_48=?,lon_49=?,lon_50=?,lon_51=?,lon_52=?,lon_53=?,lon_54=?,lon_55=?,lon_56=?,lon_57=?,lon_58=?,lon_59=?,lon_60=?,lon_61=?,lon_62=?,lon_63=?,lon_64=?,lon_65=?,lon_66=?,lon_67=?,lon_68=?,lon_69=?,lon_70=?,lon_71=?,lon_72=?,lon_73=?,lon_74=?,lon_75=?,lon_76=?,lon_77=?,lon_78=?,lon_79=?,lon_80=?,lon_81=?,lon_82=?,lon_83=?,lon_84=?,lon_85=?,lon_86=?,lon_87=?,lon_88=?,lon_89=?,lon_90=?,lon_91=?,lon_92=?,lon_93=?,lon_94=?,lon_95=?,lon_96=?,lon_97=?,lon_98=?,lon_99=? where ele_type='{ele}' and  rownum<={num} "
        cur.cleartype()
        data = (blob_buf,) * 100
        # data = (ele,) + (blob_buf,) * 100
        cur.executemany(sql, tuple(data))
    elif fields == 1:
        sql = f"update {table} set lon_00=?  where ele_type='{ele}' and rownum<={num}"
        cur.cleartype()
        data = (blob_buf,)
        # data = (ele, blob_buf)
        cur.executemany(sql, tuple(data))


def insert_batch(nums, table, db_config):
    cur = get_cur(db_config)
    sql = f"insert into {table} values(?,?)"
    blob_buf = open("./xg_lob/test_blob.jpg", "rb").read()
    cur.cleartype()
    cur.setinputtype((xgcondb.XG_C_INTEGER, xgcondb.XG_C_BLOB))
    rows = []
    for i in range(nums):
        data = (i, blob_buf)
        rows.append(data)
    print(len(rows))
    p0, p1 = zip(*rows)
    # print(p0)
    cur.executebatch(sql, (p0, p1))


def insert_many(time, path, table, db_config):
    cur = get_cur(db_config)
    sql = f"insert into {table} values(?,?,?,?,?,sysdate,?,?)"
    blob_buf = open(path, "rb").read()
    cur.cleartype()
    cur.setinputtype(
        (xgcondb.XG_C_SHORT, xgcondb.XG_C_SHORT, xgcondb.XG_C_SHORT, xgcondb.XG_C_BINARY, xgcondb.XG_C_SHORT,
         xgcondb.XG_C_DATETIME, xgcondb.XG_C_DATETIME, xgcondb.XG_C_DATETIME))
    high_levels = [1, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800,
                   1900, 2000]
    now = datetime.today()
    rounded_hour = now.replace(hour=8, minute=0, second=0, microsecond=0)
    rows = []
    for x in range(70, 140):
        for y in range(1, 61):
            for level in high_levels:
                data = (x, y, (61 - y) * 60 + x, blob_buf, level, time, rounded_hour)
                rows.append(data)
    cur.executemany(sql, tuple(rows))
    # batch_size = 100  # 根据需要调整批量大小
    # chunks = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #     executor.submit(cur.executemany, [sql] * len(chunks), chunks)


def multi_gps(n, path, table, db_config):
    start_value = 0.01
    end_value = 60.01
    # num_intervals = 60
    interval_width = (end_value - start_value) / n
    interval_boundaries = [(start_value + i * interval_width, start_value + (i + 1) * interval_width) for i in
                           range(n)]

    with ProcessPoolExecutor(max_workers=n) as executor:
        futures = [executor.submit(insert_many3, x, y, 'WIND', path, table, db_config) for (x, y) in
                   interval_boundaries]
        for future in futures:
            future.result()


def insert_many3(x, y, ele, path, table, db_config):
    """数值预报2,7个元素"""
    cur = get_cur(db_config)
    sql = f"insert into {table} values(sysdate,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    blob_buf = open(path, "rb").read()
    cur.cleartype()
    rows = []
    for i in np.arange(x, y, 0.01):
        for j in range(71, 141):
            num = 100000 * round(i, 2) + j
            data = (int(num), ele,) + (blob_buf,) * 100
            rows.append(data)
    print(len(rows))
    cur.executemany(sql, tuple(rows))


def insert_many_radr(day, path, table, db_config):
    cur = get_cur(db_config)
    sql = f"insert into {table} values(?,?,?,sysdate,?,?,?)"
    file_name = os.path.basename(path)
    blob_buf = open(path, "rb").read()
    cur.cleartype()
    cur.setinputtype((xgcondb.XG_C_CHAR, xgcondb.XG_C_CHAR, xgcondb.XG_C_CHAR, xgcondb.XG_C_DATETIME,
                      xgcondb.XG_C_DATETIME, xgcondb.XG_C_CHAR, xgcondb.XG_C_BLOB))
    day = datetime.strptime(day, '%Y-%m-%d %H:%M:%S')
    print(day, type(day))
    min_datas = [str(day + timedelta(minutes=j * 6)) for j in range(240)]  # 240个6分钟，即24小时
    # print(time_datas)
    # tab = table.split('_')
    # site = tab[1] if len(tab) == 2 else table
    site = file_name.split("_")[3]
    rows = []
    for i in min_datas:
        min_str = datetime.strptime(i, '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M%S')
        updated_file_name = re.sub(r'\d{5,}', min_str, file_name)
        data = ('Z', updated_file_name, site, i, 'bin.bz2', blob_buf)
        rows.append(data)
    cur.executemany(sql, tuple(rows))


def show(table, db_config):
    cur = get_cur(db_config)
    sql = f'select count(*) from {table}'
    data = cur.execute(sql)
    row = cur.fetchone()
    print(f'{table} : {row}')


def multi_process3(path, table, db_config):
    """7个要素，第3张表"""
    elements = ['TAIR', 'UWIN', 'VWIN', 'WIND', 'GUST', 'RHU', 'SHU']
    processes = []
    for ele in elements:
        process = multiprocessing.Process(target=insert_many3, args=(0.01, 60.01, ele, path, table, db_config))
        processes.append(process)
    for process in processes:
        process.start()
    # 等待所有进程完成
    for process in processes:
        process.join()


def multi_process4(num, fields, path, table, db_config):
    """7个要素，位置服务"""
    elements = ['TAIR', 'UWIN', 'VWIN', 'WIND', 'GUST', 'RHU', 'SHU']
    processes = []
    for ele in elements:
        process = multiprocessing.Process(target=update_tab2, args=(ele, num, fields, path, table, db_config))
        processes.append(process)
    for process in processes:
        process.start()
    # 等待所有进程完成
    for process in processes:
        process.join()


def multi_process(path, table, db_config):
    now = datetime.today()
    rounded_hour = now.replace(hour=8, minute=0, second=0, microsecond=0)
    times = []
    for i in range(72):
        rounded_hour = rounded_hour + timedelta(hours=1)
        times.append(str(rounded_hour))

    processes = []
    for time_n in times:
        process = multiprocessing.Process(target=insert_many, args=(time_n, path, table, db_config))
        processes.append(process)
    for process in processes:
        process.start()
    # 等待所有进程完成
    for process in processes:
        process.join()


def multi_process_radr(path, table, db_config):
    now = datetime.today()
    rounded_hour = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    days_30 = [str(rounded_hour + timedelta(days=i)) for i in range(30)]

    processes = []
    for day in days_30:
        process = multiprocessing.Process(target=insert_many_radr, args=(day, path, table, db_config))
        processes.append(process)
    for process in processes:
        process.start()
    # 等待所有进程完成
    for process in processes:
        process.join()


def multi_process_radr2(num_processes, path, table, db_config):
    """仅用于linux"""
    # now = datetime.today() - timedelta(days=20) #
    now = datetime.strptime("2023-12-30", "%Y-%m-%d")
    rounded_hour = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    days_30 = [str(rounded_hour + timedelta(days=i)) for i in range(30)]

    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = [executor.submit(insert_many_radr, day, path, table, db_config) for day in days_30]
        for future in futures:
            future.result()


def once_proc(table, path, db_config):
    path2 = os.path.isfile(path)
    print(f"文件是否存在: {path2}")
    if path2:
        # nums = int(input("请输入表行数: "))
        # parallel_n = int(input("请输入并发数: "))
        start = time.time()
        multi_process(path, table, db_config)
        end = time.time() - start
        show(table, db_config)
        print(f'耗时{end:.2f}秒')


def once_proc3(table, path, db_config):
    path2 = os.path.isfile(path)
    print(f"文件是否存在: {path2}")
    if path2:
        # nums = int(input("请输入表行数: "))
        # parallel_n = int(input("请输入并发数: "))
        start = time.time()
        multi_process3(path, table, db_config)
        end = time.time() - start
        show(table, db_config)
        print(f'耗时{end:.2f}秒')


def once_proc_radr(table, path, db_config):
    path2 = os.path.isfile(path)
    print(f"文件是否存在: {path2}")
    if path2:
        # nums = int(input("请输入表行数: "))
        parallel_n = int(input("请输入并发数: "))
        start = time.time()
        multi_process_radr2(parallel_n, path, table, db_config)
        end = time.time() - start
        show(table, db_config)
        print(f'耗时{end:.2f}秒')


def once_proc_radars(tables, paths, db_config):
    """用于多张表一起跑完"""
    paths = [i for i in paths.split(' ') if i.strip() and os.path.isfile(i.strip())]
    # print(paths)
    if len(tables) == len(paths):
        parallel_n = int(input("请输入并发数: "))
        run_type = input("请选择for跑完1 还是 并发跑完2,默认1: ") or 2
        start = time.time()
        if int(run_type) == 1:
            for i in range(len(tables)):
                path, table = paths[i], tables[i]
                multi_process_radr2(parallel_n, path, table, db_config)
        else:
            params = []
            for i in range(len(tables)):
                data = (parallel_n, paths[i], tables[i], db_config)
                params.append(data)
            # multi_process_radr2(parallel_n, path, table, db_config)
            with ThreadPoolExecutor(max_workers=len(tables)) as executor:
                results = list(executor.map(lambda args: multi_process_radr2(*args), params))
        end = time.time() - start
        print(f'耗时{end:.2f}秒')


if __name__ == '__main__':
    if sys.platform == 'win32':
        freeze_support()  # linux 不需要
    # db_host = '10.28.20.101'
    # db_port = 5136
    # db_user = 'SYSDBA'
    # db_pwd = 'SYSDBA'
    # db_name = 'SYSTEM'
    db_host, db_port, db_user, db_pwd, db_name, path = parse_args()
    db_config = {
        'db_host': db_host,
        'db_port': db_port,
        'db_user': db_user,
        'db_pwd': db_pwd,
        'db_name': db_name,
    }
    select = input('请选择生成雷达数据1，数值预报2，多雷达数据3，位置服务100 7要素4，\n'
                   '位置服务100 单要素5，更新位置服务6，一张表跑雷达7，默认2: ') or 2

    cur = get_cur(db_config)
    cur.execute('set max_loop_num to 0')
    cur.execute('set max_trans_modify to 0')

    if int(select) == 1:
        table = input('请输入表名(默认 test_blob )：') or 'test_blob'
        rebuild_radr_tab(table, db_config)
        once_proc_radr(table, path, db_config)
    elif int(select) == 2:
        table = input('请输入表名(默认 test_blob )：') or 'test_blob'
        rebuild_table(table, db_config)
        once_proc(table, path, db_config)
    elif int(select) == 4:
        # 位置服务生成
        table = input('请输入表名(默认 test_blob )：') or 'test_blob'
        rebuild_table2(table, db_config)
        once_proc3(table, path, db_config)
    elif int(select) == 5:
        table = input('请输入表名(默认 test_blob )：') or 'test_blob'
        parallel_n = int(input("请输入并发数: "))
        rebuild_table2(table, db_config)
        start = time.time()
        # insert_many3('WIND', path, table, db_config) # 单线程
        multi_gps(parallel_n, path, table, db_config)  # 20 个多线程
        end = time.time() - start
        show(table, db_config)
        print(f'耗时{end:.2f}秒')
    elif int(select) == 6:
        # 位置服务更新
        table = input('请输入表名(默认 test_blob ):') or 'test_blob'
        ele = input("请输入需要更新的要素个数: 默认1个要素: ") or 1
        num = input("请输入更新的行数,默认420 : ") or 420
        fields = input("请输入需要更新的字段数，默认为1: ") or 1
        start = time.time()
        if int(ele) == 1:
            update_tab2('SHU', int(num), int(fields), path, table, db_config)
        else:
            multi_process4(int(num), int(fields), path, table, db_config)  # 7个要素
        end = time.time() - start
        show(table, db_config)
        print(f'耗时{end:.2f}秒')
    elif int(select) == 7:
        #     一张表装6个文件,path="11 22 33"
        parallel_n = int(input("请输入并发数: "))
        table = input('请输入表名(默认 test_blob ):') or 'test_blob'
        paths = [i for i in path.split(' ') if i.strip() and os.path.isfile(i.strip())]
        print(paths)
        rebuild_radr_tab2(table, db_config)
        start = time.time()
        for path in paths:
            multi_process_radr2(parallel_n, path, table, db_config)
        end = time.time() - start
        show(table, db_config)
        print(f'耗时{end:.2f}秒')

    elif int(select) == 3:
        data = input('请输入多个表名，用空格分开: ')
        tables = [i for i in data.split(' ') if i.strip()]
        for table in tables:
            rebuild_radr_tab(table, db_config)
        print(tables)
        once_proc_radars(tables, path, db_config)
        # 显示表行数
        for table in tables:
            show(table, db_config)

# D:\llearn\xugu\demo\xg_lob\test_blob.jpg
# D:\llearn\xugu\demo\xg_lob\test_blob.jpg
# python blob.py -H 10.28.20.101 -P 5136 -uSYSDBA -p SYSDBA -d SYSTEM -f "D:\llearn\xugu\demo\xg_lob\22k.jpg ,D:\llearn\xugu\demo\xg_lob\52k.jpg"
