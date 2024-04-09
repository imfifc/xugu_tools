
#!/bin/bash

--数据文件路径
lj="/local/log1/3000W/";
--tabldr解析线程数
dsbf=64;
--创建索引时并发
sybf=180;
 
 xgconsole nssl 127.0.0.1 5138 SYSTEM SYSDBA SYSDBA <<!
 
create database tpch char set 'binary';
use tpch

CREATE TABLE REGION(
    R_REGIONKEY tinyint,
    R_NAME char(11),
    R_COMMENT varchar(115)) copy number 1 pctfree 1;

CREATE TABLE NATION (
    N_NATIONKEY tinyint,
    N_NAME char(14),
    N_REGIONKEY tinyint,
    N_COMMENT varchar(115)) copy number 1 pctfree 1;

CREATE TABLE PART(
    P_PARTKEY int,
    P_NAME  varchar(52),
    P_MFGR  char(14),
    P_BRAND char(8),
    P_TYPE  varchar(25),
    P_SIZE  tinyint,
    P_CONTAINER char(10),
    P_RETAILPRICE numeric(6,2),
    P_COMMENT varchar(23)) copy number 1 pctfree 1;

CREATE TABLE SUPPLIER(
    S_SUPPKEY int,
    S_NAME     char(18),
    S_ADDRESS varchar(40),
    S_NATIONKEY tinyint,
    S_PHONE   char(15),
    S_ACCTBAL numeric(6,2),
    S_COMMENT varchar(101)) copy number 1 pctfree 1;

CREATE TABLE PARTSUPP(
    PS_PARTKEY int,
    PS_SUPPKEY int,
    PS_AVAILQTY int,
    PS_SUPPLYCOST numeric(6,2),
    PS_COMMENT varchar(200)) copy number 1 pctfree 1 ;

CREATE TABLE CUSTOMER(
    C_CUSTKEY int,
    C_NAME varchar(18),
    C_ADDRESS varchar(40),
    C_NATIONKEY tinyint,
    C_PHONE char(15),
    C_ACCTBAL numeric(6,2),
    C_MKTSEGMENT char(10),
    C_COMMENT varchar(117)) copy number 1 pctfree 1;

CREATE TABLE ORDERS(
    O_ORDERKEY bigint,
    O_CUSTKEY int,
    O_ORDERSTATUS char(1),
    O_TOTALPRICE numeric(8,2),
    O_ORDERDATE date,
    O_ORDERPRIORITY char(15),
    O_CLERK char(15),
    O_SHIPPRIORITY tinyint,
    O_COMMENT varchar(79)) copy number 1 pctfree 1;

CREATE TABLE LINEITEM(
    L_ORDERKEY bigint,
    L_PARTKEY  int,
    L_SUPPKEY  int,
    L_LINENUMBER tinyint,
    L_QUANTITY numeric(4,2),
    L_EXTENDEDPRICE numeric(8,2),
    L_DISCOUNT numeric(2,2),
    L_TAX numeric(2,2),
    L_RETURNFLAG char(1),
    L_LINESTATUS char(1),
    L_SHIPDATE date,
    L_COMMITDATE date,
    L_RECEIPTDATE date,
    L_SHIPINSTRUCT varchar(25),
    L_SHIPMODE varchar(10),
    L_COMMENT varchar(4)) partition by range(L_SHIPDATE) partitions(
    p1 values less than('1993-01-01'),
    p2 values less than('1994-01-01'),
    p3 values less than('1995-01-01'),
    p4 values less than('1996-01-01'),
    p5 values less than('1997-01-01'),
    p6 values less than('1998-01-01'),
    p7 values less than('1999-01-01')
    ) copy number 1 pctfree 1;

EXIT

!
 
 
 
xgconsole nssl 127.0.0.1 5138 tpch sysdba sysdba <<!

-- 导入基础数据(数据文件路径使用真实数据文件所在路径)

tabldr table=region     datafile=${lj}region.tbl   ft='|' rt=X'0a' mode=APPEND imp_type=block parse_para=1 ins_para=1;
tabldr table=nation     datafile=${lj}nation.tbl   ft='|' rt=X'0a' mode=APPEND imp_type=block parse_para=1 ins_para=1;
tabldr table=part       datafile=${lj}part.tbl     ft='|' rt=X'0a' mode=APPEND imp_type=block parse_para=$dsbf ins_para=$dsbf;
tabldr table=supplier   datafile=${lj}supplier.tbl ft='|' rt=X'0a' mode=APPEND imp_type=block parse_para=$dsbf ins_para=$dsbf;
tabldr table=partsupp   datafile=${lj}partsupp.tbl ft='|' rt=X'0a' mode=APPEND imp_type=block parse_para=$dsbf ins_para=$dsbf;
tabldr table=customer   datafile=${lj}customer.tbl ft='|' rt=X'0a' mode=APPEND imp_type=block parse_para=$dsbf ins_para=$dsbf;
tabldr table=orders     datafile=${lj}orders.tbl   ft='|' rt=X'0a' mode=APPEND imp_type=block parse_para=$dsbf ins_para=$dsbf;
tabldr table=lineitem   datafile=${lj}lineitem.tbl ft='|' rt=X'0a' mode=APPEND imp_type=block parse_para=$dsbf ins_para=$dsbf;

-- 创建索引与约束
create index part_uk_idx on part(p_partkey) parallel $sybf;
create index supplier_uk_idx on supplier(s_suppkey) parallel $sybf;
create index partsupp_uk_idx on partsupp(ps_partkey,ps_suppkey) parallel $sybf;
create index customer_uk_idx on customer(c_custkey) parallel $sybf;
create index orders_uk_idx on orders(o_orderkey) parallel $sybf; 

--create index lineitem_uk_idx on lineitem(l_orderkey,l_linenumber) global parallel $sybf;

ALTER TABLE REGION ADD PRIMARY KEY (R_REGIONKEY);
ALTER TABLE NATION ADD PRIMARY KEY (N_NATIONKEY);
ALTER TABLE NATION ADD FOREIGN KEY NATION_FK1 (N_REGIONKEY) references REGION;
ALTER TABLE PART ADD PRIMARY KEY (P_PARTKEY);
ALTER TABLE SUPPLIER ADD PRIMARY KEY (S_SUPPKEY);
ALTER TABLE SUPPLIER ADD FOREIGN KEY SUPPLIER_FK1 (S_NATIONKEY) references NATION;
ALTER TABLE PARTSUPP ADD PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY);
ALTER TABLE PARTSUPP ADD FOREIGN KEY PARTSUPP_FK1 (PS_SUPPKEY) references SUPPLIER;
ALTER TABLE PARTSUPP ADD FOREIGN KEY PARTSUPP_FK2 (PS_PARTKEY) references PART;
ALTER TABLE CUSTOMER ADD PRIMARY KEY (C_CUSTKEY);
ALTER TABLE CUSTOMER ADD FOREIGN KEY CUSTOMER_FK1 (C_NATIONKEY) references NATION;
ALTER TABLE ORDERS ADD PRIMARY KEY (O_ORDERKEY);
ALTER TABLE ORDERS ADD FOREIGN KEY ORDERS_FK1 (O_CUSTKEY) references CUSTOMER;

ALTER TABLE LINEITEM ADD PRIMARY KEY (L_ORDERKEY,L_LINENUMBER);
ALTER TABLE LINEITEM ADD FOREIGN KEY LINEITEM_FK1 (L_ORDERKEY)  references ORDERS;
ALTER TABLE LINEITEM ADD FOREIGN KEY LINEITEM_FK2 (L_PARTKEY,L_SUPPKEY) references PARTSUPP;

--100g分析
--exec dbms_stat.analyze_table('SYSDBA.region','all columns',1,null);
--exec dbms_stat.analyze_table('SYSDBA.nation','all columns',1,null);
--exec dbms_stat.analyze_table('SYSDBA.part','all columns',1,null);
--exec dbms_stat.analyze_table('SYSDBA.SUPPLIER','all columns',1,null);
--exec dbms_stat.analyze_table('SYSDBA.PARTSUPP','all columns',1,null);
--exec dbms_stat.analyze_table('SYSDBA.CUSTOMER','all columns',1,null);
--exec dbms_stat.analyze_table('SYSDBA.ORDERS','all columns',1,null);
--exec dbms_stat.analyze_table('SYSDBA.LINEITEM','all columns',1,null);


--1000g分析
exec dbms_stat.analyze_table('SYSDBA.region','all columns',1,null);
exec dbms_stat.analyze_table('SYSDBA.nation','all columns',1,null);

exec dbms_stat.set_stat_info('SYSDBA.part','P_PARTKEY','200000000','1',null,200000000);
exec dbms_stat.set_stat_info('SYSDBA.part','P_NAME','yellow white wheat violet red','almond antique aquamarine azure blush',null,198339659);
exec dbms_stat.set_stat_info('SYSDBA.part','P_MFGR','Manufacturer#5','Manufacturer#1',null,5);
exec dbms_stat.set_stat_info('SYSDBA.part','P_BRAND','Brand#55','Brand#11',null,25);
exec dbms_stat.set_stat_info('SYSDBA.part','P_TYPE','STANDARD POLISHED TIN','ECONOMY ANODIZED BRASS',null,150);
exec dbms_stat.set_stat_info('SYSDBA.part','P_SIZE','50','1',null,50);
exec dbms_stat.set_stat_info('SYSDBA.part','P_CONTAINER','WRAP PKG','JUMBO BAG',null,40);
exec dbms_stat.set_stat_info('SYSDBA.part','P_RETAILPRICE','2099','900',null,119901);
exec dbms_stat.set_stat_info('SYSDBA.part','P_COMMENT','zzle? speci','Tire',null,13395298);

exec dbms_stat.set_stat_info('SYSDBA.supplier','S_SUPPKEY','10000000','1',null,10000000);
exec dbms_stat.set_stat_info('SYSDBA.supplier','S_ACCTBAL','9999.99','-999.99',null,1099875);
exec dbms_stat.set_stat_info('SYSDBA.supplier','S_NAME','Supplier#010000000','Supplier#000000001',null,10000000);
exec dbms_stat.set_stat_info('SYSDBA.SUPPLIER','S_ADDRESS','zzzzr MaemffsKy','    04SJW3NWgeWBx2YualVtK62DXnr',null,10000000);
exec dbms_stat.set_stat_info('SYSDBA.SUPPLIER','S_NATIONKEY','24','0',null,25);
exec dbms_stat.set_stat_info('SYSDBA.SUPPLIER','S_PHONE','34-999-999-3239','10-100-101-9215',null,9998758);
exec dbms_stat.set_stat_info('SYSDBA.SUPPLIER','S_COMMENT','zzle? special packages haggle carefully regular inst','Customer  accounts are blithely furiousRecommends',null,9796137);

exec dbms_stat.set_stat_info('SYSDBA.PARTSUPP','PS_PARTKEY','200000000','1',null,200000000);
exec dbms_stat.set_stat_info('SYSDBA.PARTSUPP','PS_SUPPKEY','10000000','1',null,10000000);
exec dbms_stat.set_stat_info('SYSDBA.PARTSUPP','PS_AVAILQTY','9999','1',null,9999);
exec dbms_stat.set_stat_info('SYSDBA.PARTSUPP','PS_SUPPLYCOST','1000','1',null,99901);
exec dbms_stat.set_stat_info('SYSDBA.PARTSUPP','PS_COMMENT','zzle? unusual requests wake slyly. slyly regular requests are e','Tiresias about the accounts detect quickly final foxes. instructions about the blithely unusual theodolites use blithely f',null,302211445);

exec dbms_stat.set_stat_info('SYSDBA.CUSTOMER','C_CUSTKEY','150000000','1',null,150000000);
exec dbms_stat.set_stat_info('SYSDBA.CUSTOMER','C_NATIONKEY','24','0',null,25);
exec dbms_stat.set_stat_info('SYSDBA.CUSTOMER','C_ACCTBAL','9999.99','-999.99',null,1099999);
exec dbms_stat.set_stat_info('SYSDBA.CUSTOMER','C_NAME','Customer#150000000','Customer#000000001',null,150000000);
exec dbms_stat.set_stat_info('SYSDBA.CUSTOMER','C_ADDRESS','zzzzyW,aeC8HnFV','    2WGW,hiM7jHg2',null,150000000);
exec dbms_stat.set_stat_info('SYSDBA.CUSTOMER','C_PHONE','34-999-999-9215','10-100-100-3024',null,149720186);
exec dbms_stat.set_stat_info('SYSDBA.CUSTOMER','C_MKTSEGMENT','MACHINERY','AUTOMOBILE',null,5);
exec dbms_stat.set_stat_info('SYSDBA.CUSTOMER','C_COMMENT','zzle? special accounts about the iro','Tiresias about the accounts haggle quiet, busy foxe',null,121231583);

exec dbms_stat.set_stat_info('SYSDBA.ORDERS','O_ORDERKEY','6000000000','1',null,1500000000);
exec dbms_stat.set_stat_info('SYSDBA.ORDERS','O_CUSTKEY','149999999','1',null,99999998);
exec dbms_stat.set_stat_info('SYSDBA.ORDERS','O_TOTALPRICE','602901.81','810.87',null,41473608);
exec dbms_stat.set_stat_info('SYSDBA.ORDERS','O_ORDERDATE','1998-08-02','1992-01-01',null,2406);
exec dbms_stat.set_stat_info('SYSDBA.ORDERS','O_SHIPPRIORITY','0','0',null,1);
exec dbms_stat.set_stat_info('SYSDBA.ORDERS','O_ORDERSTATUS','P','F',null,3);
exec dbms_stat.set_stat_info('SYSDBA.ORDERS','O_CLERK','Clerk#001000000','Clerk#000000001',null,1000000);
exec dbms_stat.set_stat_info('SYSDBA.ORDERS','O_SHIPPRIORITY','0','0',null,1);
exec dbms_stat.set_stat_info('SYSDBA.ORDERS','O_COMMENT','zzle? unusual requests w','Tiresias about the',null,270808180);

exec dbms_stat.set_stat_info('SYSDBA.LINEITEM','L_ORDERKEY','6000000000','1',null,1500000000);
exec dbms_stat.set_stat_info('SYSDBA.LINEITEM','L_PARTKEY','200000000','1',null,200000000);
exec dbms_stat.set_stat_info('SYSDBA.LINEITEM','L_SUPPKEY','10000000','1',null,10000000);
exec dbms_stat.set_stat_info('SYSDBA.LINEITEM','L_LINENUMBER','7','1',null,7);
exec dbms_stat.set_stat_info('SYSDBA.LINEITEM','L_QUANTITY','50','1',null,50);
exec dbms_stat.set_stat_info('SYSDBA.LINEITEM','L_DISCOUNT','0.1','0',null,11);
exec dbms_stat.set_stat_info('SYSDBA.LINEITEM','L_TAX','0.08','0',null,9);
exec dbms_stat.set_stat_info('SYSDBA.LINEITEM','L_SHIPDATE','1998-12-01','1992-01-02',null,2526);
exec dbms_stat.set_stat_info('SYSDBA.LINEITEM','L_COMMITDATE','1998-10-31','1992-01-31',null,2466);
exec dbms_stat.set_stat_info('SYSDBA.LINEITEM','L_RECEIPTDATE','1998-12-31','1992-01-03',null,2555);
exec dbms_stat.set_stat_info('SYSDBA.LINEITEM','L_EXTENDEDPRICE','104950','900',null,3778247);
exec dbms_stat.set_stat_info('SYSDBA.LINEITEM','L_RETURNFLAG','R','A',null,3);
exec dbms_stat.set_stat_info('SYSDBA.LINEITEM','L_LINESTATUS','O','F',null,2);
exec dbms_stat.set_stat_info('SYSDBA.LINEITEM','L_SHIPINSTRUCT','TAKE BACK RETURN','COLLECT COD',null,4);
exec dbms_stat.set_stat_info('SYSDBA.LINEITEM','L_SHIPMODE','TRUCK','AIR',null,7);

EXIT

!
