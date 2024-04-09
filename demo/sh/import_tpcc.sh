#!/bin/bash

--表hash分区数
fq=180
--copynumber数
bbs=2
--数据文件存放路径
lj="/local/log1/3000W/"
--tabldr解析线程数
dsbf=64
--创建索引并发
sybf=180

xgconsole nssl 127.0.0.1 5138 SYSTEM SYSDBA SYSDBA <<!
 
-- 创建测试库及测试表。
create database tpcc char set 'utf8';
use tpcc;
CREATE USER benchmarksql IDENTIFIED BY 'password';
GRANT DBA TO benchmarksql;
set session authorization benchmarksql;

create table bmsql_config (
  cfg_name    varchar(30) primary key,
  cfg_value   varchar(50)
);

create table bmsql_warehouse (
  w_id        integer   not null,
  w_ytd       number(12,2),
  w_tax       number(4,4),
  w_name      varchar(10),
  w_street_1  varchar(20),
  w_street_2  varchar(20),
  w_city      varchar(20),
  w_state     char(2),
  w_zip       char(9)
)partition by hash(w_id) partitions $fq pctfree 99 copy number $bbs zone by local;

create table bmsql_district (
  d_id         integer       not null,
  d_w_id       integer       not null,
  d_ytd        number(12,2),
  d_tax        number(4,4),
  d_next_o_id  integer,
  d_name       varchar(10),
  d_street_1   varchar(20),
  d_street_2   varchar(20),
  d_city       varchar(20),
  d_state      char(2),
  d_zip        char(9)
)partition by hash(d_w_id) partitions $fq pctfree 99 copy number $bbs zone by local;

create table bmsql_customer (
  c_id           integer        not null,
  c_d_id         integer        not null,
  c_w_id         integer        not null,
  c_discount     number(4,4),
  c_credit       char(2),
  c_last         varchar(16),
  c_first        varchar(16),
  c_credit_lim   number(12,2),
  c_balance      number(12,2),
  c_ytd_payment  number(12,2),
  c_payment_cnt  integer,
  c_delivery_cnt integer,
  c_street_1     varchar(20),
  c_street_2     varchar(20),
  c_city         varchar(20),
  c_state        char(2),
  c_zip          char(9),
  c_phone        char(16),
  c_since        timestamp,
  c_middle       char(2),
  c_data         varchar(500)
)partition by hash(c_w_id) partitions $fq pctfree 1 copy number $bbs zone by local;

create sequence bmsql_hist_id_seq;

create table bmsql_history (
  hist_id  integer,
  h_c_id   integer,
  h_c_d_id integer,
  h_c_w_id integer,
  h_d_id   integer,
  h_w_id   integer,
  h_date   timestamp,
  h_amount number(6,2),
  h_data   varchar(24)
)partition by hash(h_c_w_id) partitions $fq pctfree 1 copy number $bbs zone by local;

create table bmsql_new_order (
  no_w_id  integer   not null,
  no_d_id  integer   not null,
  no_o_id  integer   not null
)partition by hash(no_w_id) partitions $fq pctfree 1 copy number $bbs zone by local;

create table bmsql_oorder (
  o_id         integer      not null,
  o_w_id       integer      not null,
  o_d_id       integer      not null,
  o_c_id       integer,
  o_carrier_id integer,
  o_ol_cnt     integer,
  o_all_local  integer,
  o_entry_d    timestamp
)partition by hash(o_w_id) partitions $fq pctfree 1 copy number $bbs zone by local;

create table bmsql_order_line (
  ol_w_id         integer   not null,
  ol_d_id         integer   not null,
  ol_o_id         integer   not null,
  ol_number       integer   not null,
  ol_i_id         integer   not null,
  ol_delivery_d   timestamp,
  ol_amount       number(6,2),
  ol_supply_w_id  integer,
  ol_quantity     integer,
  ol_dist_info    char(24)
)partition by hash(ol_w_id) partitions $fq pctfree 1 copy number $bbs zone by local;

create table bmsql_item (
  i_id     integer      not null,
  i_name   varchar(24),
  i_price  number(5,2),
  i_data   varchar(50),
  i_im_id  integer
) pctfree 1 copy number 1 zone by local;

create table bmsql_stock (
  s_i_id       integer       not null,
  s_w_id       integer       not null,
  s_quantity   integer,
  s_ytd        integer,
  s_order_cnt  integer,
  s_remote_cnt integer,
  s_data       varchar(50),
  s_dist_01    char(24),
  s_dist_02    char(24),
  s_dist_03    char(24),
  s_dist_04    char(24),
  s_dist_05    char(24),
  s_dist_06    char(24),
  s_dist_07    char(24),
  s_dist_08    char(24),
  s_dist_09    char(24),
  s_dist_10    char(24)
)partition by hash(s_w_id) partitions $fq pctfree 1 copy number $bbs zone by local;

EXIT

!

xgconsole nssl 127.0.0.1 5138 tpcc benchmarksql password <<!




tabldr table=benchmarksql.bmsql_config     datafile=${lj}config.csv        ft=',' rt=X'0a'  mode=APPEND  parse_para=1 ins_para=1
tabldr table=benchmarksql.bmsql_warehouse  datafile=${lj}warehouse.csv     ft=',' rt=X'0a'  mode=APPEND  parse_para=1 ins_para=1
tabldr table=benchmarksql.bmsql_item       datafile=${lj}item.csv          ft=',' rt=X'0a'  mode=APPEND  parse_para=8 ins_para=8
tabldr table=benchmarksql.bmsql_district   datafile=${lj}district.csv      ft=',' rt=X'0a'  mode=APPEND  parse_para=1 ins_para=1
tabldr table=benchmarksql.bmsql_customer   datafile=${lj}customer.csv      ft=',' rt=X'0a'  mode=APPEND  parse_para=$dsbf ins_para=$dsbf
tabldr table=benchmarksql.bmsql_history    datafile=${lj}cust-hist.csv     ft=',' rt=X'0a'  mode=APPEND  parse_para=$dsbf ins_para=$dsbf
tabldr table=benchmarksql.bmsql_oorder     datafile=${lj}order.csv         ft=',' rt=X'0a'  mode=APPEND  parse_para=$dsbf ins_para=$dsbf
tabldr table=benchmarksql.bmsql_new_order  datafile=${lj}new-order.csv     ft=',' rt=X'0a'  mode=APPEND  parse_para=$dsbf ins_para=$dsbf
tabldr table=benchmarksql.bmsql_order_line datafile=${lj}order-line.csv    ft=',' rt=X'0a'  mode=append  parse_para=$dsbf ins_para=$dsbf
tabldr table=benchmarksql.bmsql_stock      datafile=${lj}stock.csv         ft=',' rt=X'0a'  mode=append  parse_para=$dsbf ins_para=$dsbf



create index pk_index_customer   on bmsql_customer(c_w_id, c_d_id, c_id) parallel $sybf;
create index pk_index_oorder     on bmsql_oorder(o_w_id, o_d_id, o_id) parallel $sybf;
create index pk_index_new_order  on bmsql_new_order(no_w_id, no_d_id, no_o_id) parallel $sybf;
create index pk_index_order_line on bmsql_order_line(ol_w_id, ol_d_id, ol_o_id, ol_number) parallel $sybf;
create index pk_index_stock      on bmsql_stock(s_w_id, s_i_id) parallel $sybf;



-- 创建约束
alter table bmsql_warehouse add constraint bmsql_warehouse_pkey primary key (w_id);
alter table bmsql_district add constraint bmsql_district_pkey primary key (d_w_id, d_id);
alter table bmsql_customer add constraint bmsql_customer_pkey primary key (c_w_id, c_d_id, c_id);
alter table bmsql_oorder add constraint bmsql_oorder_pkey primary key (o_w_id, o_d_id, o_id);
--create unique index bmsql_oorder_idx1 on bmsql_oorder (o_w_id, o_d_id, o_carrier_id, o_id) parallel $sybf;
alter table bmsql_new_order add constraint bmsql_new_order_pkey primary key (no_w_id, no_d_id, no_o_id);
alter table bmsql_order_line add constraint bmsql_order_line_pkey primary key (ol_w_id, ol_d_id, ol_o_id, ol_number);
alter table bmsql_stock add constraint bmsql_stock_pkey primary key (s_w_id, s_i_id);
alter table bmsql_item add constraint bmsql_item_pkey primary key (i_id);


create index s2_cust_last on benchmarksql.bmsql_customer(c_w_id,c_d_id,c_last) indextype is btree parallel $sybf;
create index  oorder_idx2 on benchmarksql.bmsql_oorder(O_W_ID,O_D_ID,O_C_ID) indextype is btree parallel $sybf;


reindex bmsql_config.*;


alter table bmsql_district add constraint d_warehouse_fkey
    foreign key (d_w_id)
    references bmsql_warehouse (w_id);

alter table bmsql_customer add constraint c_district_fkey
    foreign key (c_w_id, c_d_id)
    references bmsql_district (d_w_id, d_id);

alter table bmsql_history add constraint h_customer_fkey
    foreign key (h_c_w_id, h_c_d_id, h_c_id)
    references bmsql_customer (c_w_id, c_d_id, c_id);
alter table bmsql_history add constraint h_district_fkey
    foreign key (h_w_id, h_d_id)
    references bmsql_district (d_w_id, d_id);

alter table bmsql_new_order add constraint no_order_fkey
    foreign key (no_w_id, no_d_id, no_o_id)
    references bmsql_oorder (o_w_id, o_d_id, o_id);

alter table bmsql_oorder add constraint o_customer_fkey
    foreign key (o_w_id, o_d_id, o_c_id)
    references bmsql_customer (c_w_id, c_d_id, c_id);

alter table bmsql_order_line add constraint ol_order_fkey
    foreign key (ol_w_id, ol_d_id, ol_o_id)
    references bmsql_oorder (o_w_id, o_d_id, o_id);
alter table bmsql_order_line add constraint ol_stock_fkey
    foreign key (ol_supply_w_id, ol_i_id)
    references bmsql_stock (s_w_id, s_i_id);

alter table bmsql_stock add constraint s_warehouse_fkey
    foreign key (s_w_id)
    references bmsql_warehouse (w_id);
alter table bmsql_stock add constraint s_item_fkey
    foreign key (s_i_id)
    references bmsql_item (i_id);





--分析表

--仓数较小时：
--exec dbms_stat.analyze_table('benchmarksql.bmsql_warehouse','all index columns',1,null);
--exec dbms_stat.analyze_table('benchmarksql.bmsql_item','all index columns',1,null);
--exec dbms_stat.analyze_table('benchmarksql.bmsql_history','all index columns',1,null);
--exec dbms_stat.analyze_table('benchmarksql.bmsql_district','all index columns',1,null);
--
--exec dbms_stat.analyze_table('benchmarksql.bmsql_stock','s_i_id',1,null);
--exec dbms_stat.analyze_table('benchmarksql.bmsql_stock','s_w_id',1,null);
--
--exec dbms_stat.analyze_table('benchmarksql.bmsql_order_line','all columns',1,null);
--exec dbms_stat.analyze_table('benchmarksql.bmsql_customer','all columns',1,null);
--exec dbms_stat.analyze_table('benchmarksql.bmsql_oorder','all columns',1,null);
--exec dbms_stat.analyze_table('benchmarksql.bmsql_new_order','all columns',1,null);

--仓数较大时bmsql_order_line，bmsql_customer的全表分析会占用较长时间
--3000仓时
--exec dbms_stat.set_stat_info('benchmarksql.bmsql_order_line','OL_W_ID',3000,1,300004,3000);
--exec dbms_stat.set_stat_info('benchmarksql.bmsql_order_line','OL_D_ID',10,1,90001088,10);
--exec dbms_stat.set_stat_info('benchmarksql.bmsql_order_line','OL_O_ID',3000,1,300004,3000);
--exec dbms_stat.set_stat_info('benchmarksql.bmsql_order_line','OL_NUMBER',15,1,60000725,15);
--
--exec dbms_stat.set_stat_info('benchmarksql.bmsql_customer','c_w_id',3000,1,30000,3000);
--exec dbms_stat.set_stat_info('benchmarksql.bmsql_customer','c_d_id',10,1,9000000,10);
--exec dbms_stat.set_stat_info('benchmarksql.bmsql_customer','c_id',3000,1,30000,3000);
--exec dbms_stat.set_stat_info('benchmarksql.bmsql_customer','c_last','PRIPRIPRI','ABLEABLEABLE',90000,1000);

--1000仓：
exec dbms_stat.analyze_table('benchmarksql.bmsql_warehouse','all index columns',1,null);
exec dbms_stat.analyze_table('benchmarksql.bmsql_item','all index columns',1,null);
exec dbms_stat.analyze_table('benchmarksql.bmsql_history','all index columns',1,null);
exec dbms_stat.analyze_table('benchmarksql.bmsql_district','all index columns',1,null);



exec dbms_stat.set_stat_info('benchmarksql.bmsql_stock','s_i_id',100000 ,1,1000,100000);
exec dbms_stat.set_stat_info('benchmarksql.bmsql_stock','s_w_id',1000,1,100000 ,1000);

exec dbms_stat.set_stat_info('benchmarksql.bmsql_order_line','OL_W_ID',1000,1,300000,1000);
exec dbms_stat.set_stat_info('benchmarksql.bmsql_order_line','OL_D_ID',10,1,30000000,10);
exec dbms_stat.set_stat_info('benchmarksql.bmsql_order_line','OL_O_ID',3000,1,100000,3000);
exec dbms_stat.set_stat_info('benchmarksql.bmsql_order_line','OL_NUMBER',15,1,20000000,15);

exec dbms_stat.set_stat_info('benchmarksql.bmsql_customer','c_w_id',1000,1,null,1000);
exec dbms_stat.set_stat_info('benchmarksql.bmsql_customer','c_d_id',10,1,null,10);
exec dbms_stat.set_stat_info('benchmarksql.bmsql_customer','c_id',3000,1,null,3000);
exec dbms_stat.set_stat_info('benchmarksql.bmsql_customer','c_last','PRIPRIPRI','ABLEABLEABLE',null,1000);


exec dbms_stat.set_stat_info('benchmarksql.bmsql_oorder','o_id',3000,1,10000,3000);
exec dbms_stat.set_stat_info('benchmarksql.bmsql_oorder','o_w_id',1000,1,30000,1000);
exec dbms_stat.set_stat_info('benchmarksql.bmsql_oorder','o_d_id',10,1,3000000,10);
exec dbms_stat.set_stat_info('benchmarksql.bmsql_oorder','o_c_id',3000,1,10000,3000);
exec dbms_stat.set_stat_info('benchmarksql.bmsql_oorder','o_carrier_id',10,1,3000000,10);

exec dbms_stat.set_stat_info('benchmarksql.bmsql_new_order','no_w_id',1000,1,9000,1000);
exec dbms_stat.set_stat_info('benchmarksql.bmsql_new_order','no_d_id',10,1,900000,10);
exec dbms_stat.set_stat_info('benchmarksql.bmsql_new_order','no_o_id',3000,2101,10000,900);



alter table bmsql_item set slow modify on;



-- 数据一致性校验SQL
(Select w_id, w_ytd from bmsql_warehouse) except (select d_w_id, sum(d_ytd) from bmsql_district group by d_w_id) parallel 180;
(Select d_w_id, d_id, D_NEXT_O_ID - 1 from bmsql_district) except (select o_w_id, o_d_id, max(o_id) from bmsql_oorder group by o_w_id, o_d_id) parallel 180;
(Select d_w_id, d_id, D_NEXT_O_ID - 1 from bmsql_district) except (select no_w_id, no_d_id, max(no_o_id) from bmsql_new_order group by no_w_id, no_d_id) parallel 180;
select * from (select (count(no_o_id)-(max(no_o_id)-min(no_o_id)+1)) as diff from bmsql_new_order group by no_w_id, no_d_id) where diff != 0 parallel 180;
(select o_w_id, o_d_id, sum(o_ol_cnt) from bmsql_oorder group by o_w_id, o_d_id) except (select ol_w_id, ol_d_id, count(ol_o_id) from bmsql_order_line group by ol_w_id, ol_d_id) parallel 180;
(select d_w_id, sum(d_ytd) from bmsql_district group by d_w_id) except (Select w_id, w_ytd from bmsql_warehouse) parallel 180;

-- 辅助函数创建
create or replace function pp(i_s_w_id in int,i_s_quantity in int,i_d_w_id in int,i_d_id in int) return bigint as
a int;
b int;
c int;
d int;
e varchar;
f int;
g int;
h varchar;
begin
a:=i_s_w_id;
b:=i_s_quantity;
c:=i_d_w_id;
d:=i_d_id;
h:='select d_next_o_id from bmsql_district where d_w_id ='||c||' AND d_id = '||d;
execute immediate h into g;
e:=('SELECT count(DISTINCT(s_i_id)) AS low_stock FROM bmsql_stock,bmsql_order_line WHERE s_w_id ='||a||' AND s_quantity <'||b||' AND ol_o_id >=('||g||'-20)'||' AND ol_o_id <'||g||' AND ol_w_id ='||c||' AND ol_d_id ='||d||' AND s_i_id = ol_i_id');
execute immediate e into f;
return f;
end;
/



CREATE OR REPLACE PROCEDURE TPCC_DELIVERY(V_W_ID IN INT,V_CARRIER_ID IN INT, V_RET OUT INT)
IS
V_D_ID INT;
V_NO_O_ID INT;
V_C_ID INT;
V_OL_TOTAL DOUBLE PRECISION;
CURSOR C_NO IS SELECT NO_O_ID FROM BMSQL_NEW_ORDER WHERE NO_D_ID=V_D_ID AND NO_W_ID=V_W_ID ORDER BY NO_W_ID,NO_D_ID,NO_O_ID ASC;
BEGIN
FOR I IN 1..10 LOOP
V_D_ID := I;
OPEN C_NO;
FETCH C_NO INTO V_NO_O_ID;
if C_NO%NOTFOUND then close C_NO;
continue;
end if;
CLOSE C_NO;
DELETE FROM BMSQL_NEW_ORDER WHERE NO_O_ID=V_NO_O_ID AND NO_D_ID=V_D_ID AND NO_W_ID=V_W_ID;
SELECT O_C_ID INTO V_C_ID FROM BMSQL_OORDER WHERE O_ID=V_NO_O_ID AND O_W_ID=V_W_ID AND O_D_ID=V_D_ID;
UPDATE BMSQL_OORDER SET O_CARRIER_ID=V_CARRIER_ID WHERE O_ID=V_NO_O_ID AND O_W_ID=V_W_ID AND O_D_ID=V_D_ID;
UPDATE BMSQL_ORDER_LINE SET OL_DELIVERY_D=CURRENT_DATE WHERE OL_O_ID=V_NO_O_ID AND OL_D_ID=V_D_ID AND OL_W_ID=V_W_ID;
SELECT SUM(OL_AMOUNT) INTO V_OL_TOTAL FROM BMSQL_ORDER_LINE WHERE OL_O_ID=V_NO_O_ID AND OL_D_ID=V_D_ID AND OL_W_ID=V_W_ID;
UPDATE BMSQL_CUSTOMER SET C_BALANCE=C_BALANCE+V_OL_TOTAL WHERE C_ID=V_C_ID AND C_D_ID=V_D_ID AND C_W_ID=V_W_ID;
END LOOP;
COMMIT;
V_RET:=1;
EXCEPTION WHEN OTHERS THEN
ROLLBACK;
V_RET:=0;
END;
/

CREATE OR REPLACE PROCEDURE TPCC_NEWORDER
(
V_W_ID IN INT,V_D_ID IN INT,V_C_ID IN INT,V_O_OL_CNT IN INT, 
V_I_ID1 IN INT,
V_I_ID2 IN INT, 
V_I_ID3 IN INT, 
V_I_ID4 IN INT, 
V_I_ID5 IN INT, 
V_I_ID6 IN INT, 
V_I_ID7 IN INT, 
V_I_ID8 IN INT, 
V_I_ID9 IN INT, 
V_I_ID10 IN INT, 
V_I_ID11 IN INT, 
V_I_ID12 IN INT, 
V_I_ID13 IN INT, 
V_I_ID14 IN INT, 
V_I_ID15 IN INT,
V_S_W_ID1 IN INT, 
V_S_W_ID2 IN INT, 
V_S_W_ID3 IN INT, 
V_S_W_ID4 IN INT, 
V_S_W_ID5 IN INT, 
V_S_W_ID6 IN INT, 
V_S_W_ID7 IN INT, 
V_S_W_ID8 IN INT, 
V_S_W_ID9 IN INT, 
V_S_W_ID10 IN INT, 
V_S_W_ID11 IN INT, 
V_S_W_ID12 IN INT, 
V_S_W_ID13 IN INT, 
V_S_W_ID14 IN INT, 
V_S_W_ID15 IN INT,
V_OL_QTY1 IN INT,
V_OL_QTY2 IN INT,
V_OL_QTY3 IN INT,
V_OL_QTY4 IN INT,
V_OL_QTY5 IN INT,
V_OL_QTY6 IN INT,
V_OL_QTY7 IN INT,
V_OL_QTY8 IN INT,
V_OL_QTY9 IN INT,
V_OL_QTY10 IN INT,
V_OL_QTY11 IN INT,
V_OL_QTY12 IN INT,
V_OL_QTY13 IN INT,
V_OL_QTY14 IN INT,
V_OL_QTY15 IN INT,
V_O_ALL_LOCAL IN INT,
V_RET OUT INT
)
IS
TYPE REC IS RECORD(
INAME VARCHAR(24),
IPRICE DOUBLE,
OLAMOUNT DOUBLE,
BGFLAG CHAR(1),
SQUANTITY INT);
TYPE ALL_SEL IS TABLE OF REC INDEX BY BINARY_INTEGER;
TYPE INTTABLE IS TABLE OF INT INDEX BY BINARY_INTEGER;
V_TMP_OID INT;
V_ARR_ALL ALL_SEL;
V_S_DATA  VARCHAR(50);
V_I_DATA  VARCHAR(50);
V_DIST_INFO VARCHAR(25);
V_C_DISCOUNT DOUBLE;
V_C_CREDIT CHAR(2);
V_W_TAX DOUBLE;
V_CUR_OID INT;
V_D_TAX DOUBLE;
V_TOTAL_AMOUNT DOUBLE;
V_COMMIT_FLAG INT;
V_O_ENTRY_D DATE;
V_ARR_I_ID INTTABLE ;
V_ARR_W_ID INTTABLE ;
V_ARR_OL_QTY INTTABLE ;
V_TMP_QUANTITY INT;
V_C_LAST VARCHAR(16);
BEGIN
V_ARR_I_ID(1) := V_I_ID1; 
V_ARR_I_ID(2) := V_I_ID2; 
V_ARR_I_ID(3) := V_I_ID3; 
V_ARR_I_ID(4) := V_I_ID4; 
V_ARR_I_ID(5) := V_I_ID5; 
V_ARR_I_ID(6) := V_I_ID6; 
V_ARR_I_ID(7) := V_I_ID7; 
V_ARR_I_ID(8) := V_I_ID8; 
V_ARR_I_ID(9) := V_I_ID9; 
V_ARR_I_ID(10) := V_I_ID10; 
V_ARR_I_ID(11) := V_I_ID11; 
V_ARR_I_ID(12) := V_I_ID12; 
V_ARR_I_ID(13) := V_I_ID13; 
V_ARR_I_ID(14) := V_I_ID14; 
V_ARR_I_ID(15) := V_I_ID15; 
V_ARR_W_ID(1) := V_S_W_ID1; 
V_ARR_W_ID(2) := V_S_W_ID2; 
V_ARR_W_ID(3) := V_S_W_ID3; 
V_ARR_W_ID(4) := V_S_W_ID4; 
V_ARR_W_ID(5) := V_S_W_ID5; 
V_ARR_W_ID(6) := V_S_W_ID6; 
V_ARR_W_ID(7) := V_S_W_ID7; 
V_ARR_W_ID(8) := V_S_W_ID8; 
V_ARR_W_ID(9) := V_S_W_ID9; 
V_ARR_W_ID(10) := V_S_W_ID10; 
V_ARR_W_ID(11) := V_S_W_ID11; 
V_ARR_W_ID(12) := V_S_W_ID12; 
V_ARR_W_ID(13) := V_S_W_ID13; 
V_ARR_W_ID(14) := V_S_W_ID14; 
V_ARR_W_ID(15) := V_S_W_ID15; 
V_ARR_OL_QTY(1) := V_OL_QTY1; 
V_ARR_OL_QTY(2) := V_OL_QTY2; 
V_ARR_OL_QTY(3) := V_OL_QTY3; 
V_ARR_OL_QTY(4) := V_OL_QTY4; 
V_ARR_OL_QTY(5) := V_OL_QTY5; 
V_ARR_OL_QTY(6) := V_OL_QTY6; 
V_ARR_OL_QTY(7) := V_OL_QTY7; 
V_ARR_OL_QTY(8) := V_OL_QTY8; 
V_ARR_OL_QTY(9) := V_OL_QTY9; 
V_ARR_OL_QTY(10) := V_OL_QTY10; 
V_ARR_OL_QTY(11) := V_OL_QTY11; 
V_ARR_OL_QTY(12) := V_OL_QTY12; 
V_ARR_OL_QTY(13) := V_OL_QTY13; 
V_ARR_OL_QTY(14) := V_OL_QTY14; 
V_ARR_OL_QTY(15) := V_OL_QTY15; 
V_TOTAL_AMOUNT := 0;
V_O_ENTRY_D := CURRENT_DATE;
UPDATE BMSQL_DISTRICT SET D_NEXT_O_ID = D_NEXT_O_ID + 1 WHERE D_ID = V_D_ID AND D_W_ID =V_W_ID RETURNING D_NEXT_O_ID,D_TAX INTO V_TMP_OID, V_D_TAX;
V_CUR_OID := V_TMP_OID - 1;
INSERT INTO BMSQL_OORDER(O_ID, O_C_ID, O_D_ID, O_W_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) 
VALUES (V_CUR_OID, V_C_ID, V_D_ID, V_W_ID, V_O_ENTRY_D, 0, V_O_OL_CNT, V_O_ALL_LOCAL);

FOR I IN 1..V_O_OL_CNT LOOP
SELECT I_NAME, I_PRICE, I_DATA INTO      V_ARR_ALL(I).INAME, V_ARR_ALL(I).IPRICE, V_I_DATA FROM BMSQL_ITEM WHERE I_ID = V_ARR_I_ID(I);
UPDATE BMSQL_STOCK 
SET    S_YTD       = S_YTD + V_ARR_OL_QTY(I), 
      S_QUANTITY  = CASE WHEN (S_QUANTITY-V_ARR_OL_QTY(I) >= 10) THEN S_QUANTITY-V_ARR_OL_QTY(I) ELSE S_QUANTITY-V_ARR_OL_QTY(I)+91 END, 
S_ORDER_CNT = S_ORDER_CNT + 1, 
        S_REMOTE_CNT = S_REMOTE_CNT + CASE WHEN (V_W_ID = V_ARR_W_ID(I)) THEN 0 ELSE 1 END
WHERE S_I_ID = V_ARR_I_ID(I) 
AND S_W_ID=V_ARR_W_ID(I)
RETURNING S_DATA, S_QUANTITY 
INTO V_S_DATA, V_ARR_ALL(I).SQUANTITY;
IF((INSTR(V_I_DATA,'ORIGINAL') > 0) AND (INSTR(V_S_DATA,'ORIGINAL') > 0)) THEN
V_ARR_ALL(I).BGFLAG := 'B';
ELSE 
V_ARR_ALL(I).BGFLAG := 'G';
END IF;
CASE V_D_ID 
        WHEN 1 THEN SELECT S_DIST_01 INTO V_DIST_INFO FROM BMSQL_STOCK WHERE S_I_ID=V_ARR_I_ID(I) AND S_W_ID=V_ARR_W_ID(I);
        WHEN 2 THEN SELECT S_DIST_02 INTO V_DIST_INFO FROM BMSQL_STOCK WHERE S_I_ID=V_ARR_I_ID(I) AND S_W_ID=V_ARR_W_ID(I);
        WHEN 3 THEN SELECT S_DIST_03 INTO V_DIST_INFO FROM BMSQL_STOCK WHERE S_I_ID=V_ARR_I_ID(I) AND S_W_ID=V_ARR_W_ID(I);
        WHEN 4 THEN SELECT S_DIST_04 INTO V_DIST_INFO FROM BMSQL_STOCK WHERE S_I_ID=V_ARR_I_ID(I) AND S_W_ID=V_ARR_W_ID(I);
        WHEN 5 THEN SELECT S_DIST_05 INTO V_DIST_INFO FROM BMSQL_STOCK WHERE S_I_ID=V_ARR_I_ID(I) AND S_W_ID=V_ARR_W_ID(I);
        WHEN 6 THEN SELECT S_DIST_06 INTO V_DIST_INFO FROM BMSQL_STOCK WHERE S_I_ID=V_ARR_I_ID(I) AND S_W_ID=V_ARR_W_ID(I);
        WHEN 7 THEN SELECT S_DIST_07 INTO V_DIST_INFO FROM BMSQL_STOCK WHERE S_I_ID=V_ARR_I_ID(I) AND S_W_ID=V_ARR_W_ID(I);
        WHEN 8 THEN SELECT S_DIST_08 INTO V_DIST_INFO FROM BMSQL_STOCK WHERE S_I_ID=V_ARR_I_ID(I) AND S_W_ID=V_ARR_W_ID(I);
        WHEN 9 THEN SELECT S_DIST_08 INTO V_DIST_INFO FROM BMSQL_STOCK WHERE S_I_ID=V_ARR_I_ID(I) AND S_W_ID=V_ARR_W_ID(I);
        WHEN 10 THEN SELECT S_DIST_10 INTO V_DIST_INFO FROM BMSQL_STOCK WHERE S_I_ID=V_ARR_I_ID(I) AND S_W_ID=V_ARR_W_ID(I);
END case;
V_ARR_ALL(I).OLAMOUNT := V_ARR_OL_QTY(I) * V_ARR_ALL(I).IPRICE;
V_TOTAL_AMOUNT := V_TOTAL_AMOUNT  + V_ARR_ALL(I).OLAMOUNT;
INSERT INTO BMSQL_ORDER_LINE(OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO, OL_DELIVERY_D)  
VALUES(V_CUR_OID, V_D_ID, V_W_ID, I, V_ARR_I_ID(I), V_ARR_W_ID(I), V_ARR_OL_QTY(I), V_ARR_ALL(I).OLAMOUNT, V_DIST_INFO ,'1900-01-01');
END LOOP;
INSERT INTO BMSQL_NEW_ORDER VALUES(V_W_ID,V_D_ID,V_CUR_OID);
select c_last, c_discount, c_credit, w_tax INTO V_C_LAST, V_C_DISCOUNT, V_C_CREDIT, V_W_TAX from BMSQL_CUSTOMER, BMSQL_WAREHOUSE 
where w_id=v_w_id and c_w_id=v_w_id and c_d_id=v_d_id and c_id=v_c_id;
COMMIT;
V_RET:=1;
EXCEPTION WHEN OTHERS THEN
ROLLBACK;
V_RET:=0;
END;
/

CREATE OR REPLACE PROCEDURE TPCC_ORDSTAT
(
    V_C_ID          IN OUT INT,
    V_D_ID          IN INT,
    V_W_ID          IN INT,
    V_C_LAST        IN OUT VARCHAR(16),
    V_BYNAME        IN INT,
    V_RET           OUT INT
)
IS
    V_C_BALANCE     DOUBLE;
    V_C_FIRST       VARCHAR(16); 
    V_C_MIDDLE      VARCHAR(2); 
    V_O_ID int;
    V_O_CARRIER_ID  INT;
    V_O_ENTRY_D     DATE;
    CNT INT;
    CURSOR C3 IS SELECT C_BALANCE, C_FIRST, C_MIDDLE, C_ID 
                 FROM   BMSQL_CUSTOMER 
                 WHERE  C_LAST = V_C_LAST 
                 AND    C_D_ID = V_D_ID 
                 AND    C_W_ID = V_W_ID 
                 ORDER BY C_FIRST;
    CURSOR C_LINE IS SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY,OL_AMOUNT, OL_DELIVERY_D 
                     FROM   BMSQL_ORDER_LINE 
                     WHERE  OL_O_ID = V_O_ID 
                     AND    OL_D_ID = V_D_ID 
                     AND    OL_W_ID = V_W_ID;
                     
    TYPE INTTABLE IS TABLE OF INT INDEX BY BINARY_INTEGER;
    TYPE AMOUNTTABLE IS TABLE OF DOUBLE PRECISION INDEX BY BINARY_INTEGER;
    TYPE DATETABLE IS TABLE OF DATE INDEX BY BINARY_INTEGER;
    OS_C_LINE         C_LINE%ROWTYPE;
        
    OS_OL_I_ID        INTTABLE ;  
    OS_OL_SUPPLY_W_ID INTTABLE ;  
    OS_OL_QUANTITY    INTTABLE ;
    OS_OL_AMOUNT      AMOUNTTABLE;
    OS_OL_DELIVERY_D  DATETABLE;
    I                 INT;
BEGIN
    IF V_BYNAME = 0 THEN
        SELECT  C_BALANCE, C_FIRST, C_MIDDLE, C_LAST 
        INTO    V_C_BALANCE, V_C_FIRST, V_C_MIDDLE, V_C_LAST 
        FROM    BMSQL_CUSTOMER 
        WHERE   C_ID    = V_C_ID 
        AND     C_D_ID  = V_D_ID 
        AND     C_W_ID  = V_W_ID;
    ELSE
        SELECT  COUNT(C_ID) 
        INTO    CNT 
        FROM    BMSQL_CUSTOMER 
        WHERE   C_LAST = V_C_LAST 
        AND     C_D_ID = V_D_ID 
        AND     C_W_ID = V_W_ID;
  
        IF (MOD(CNT, 2) = 1) THEN
            CNT := (CNT + 1);
        END IF;
    
        CNT := CNT / 2;
        
        OPEN C3;
        FOR I IN 1 .. CNT
        LOOP
            FETCH C3 INTO V_C_BALANCE, V_C_FIRST, V_C_MIDDLE, V_C_ID;
        END LOOP;
    
        CLOSE C3;
    END IF;
    
    SELECT O_ID, O_CARRIER_ID, O_ENTRY_D 
    INTO  V_O_ID, V_O_CARRIER_ID, V_O_ENTRY_D 
    FROM (
            SELECT TOP 1 O_ID, O_CARRIER_ID, O_ENTRY_D 
            FROM   BMSQL_OORDER  
            WHERE  O_C_ID = V_C_ID 
            AND    O_D_ID = V_D_ID 
            AND    O_W_ID = V_W_ID 
            ORDER BY O_W_ID,O_D_ID,O_ID DESC) ;
--    WHERE ROWNUM = 1;
    
    I := 1;
    
    FOR OS_C_LINE IN C_LINE
    LOOP
        OS_OL_I_ID(I)         := OS_C_LINE.OL_I_ID;
        OS_OL_SUPPLY_W_ID(I)  := OS_C_LINE.OL_SUPPLY_W_ID;
        OS_OL_QUANTITY(I)     := OS_C_LINE.OL_QUANTITY;
        OS_OL_AMOUNT(I)       := OS_C_LINE.OL_AMOUNT;
        OS_OL_DELIVERY_D(I)   := OS_C_LINE.OL_DELIVERY_D;
        
        I := I + 1;
    END LOOP;
    COMMIT;
    V_RET:=1;
    EXCEPTION WHEN OTHERS THEN
    ROLLBACK;
    V_RET:=0;
END;
/

CREATE OR REPLACE PROCEDURE TPCC_PAYMENT
(
V_W_ID IN INT,
V_D_ID IN INT,
V_C_ID IN OUT INT,
V_C_W_ID IN INT,
V_C_D_ID IN INT,
V_C_LAST IN OUT VARCHAR2,
V_A_AMOUNT IN DOUBLE,
V_BYNAME IN INT,
V_RET OUT INT
)
IS
V_W_STATE VARCHAR(2);V_D_STATE VARCHAR(2);V_C_STATE VARCHAR(2);V_C_MIDDLE VARCHAR(2);V_C_CREDIT VARCHAR(2);
V_C_CREDIT_LIM DOUBLE;V_C_DISCOUNT DOUBLE;V_C_BALANCE DOUBLE;
V_C_SINCE DATE;V_H_DATE DATE;
V_W_STREET_1 VARCHAR(20);
V_W_STREET_2 VARCHAR(20);
V_W_CITY VARCHAR(20);
V_D_STREET_1 VARCHAR(20);V_D_STREET_2 VARCHAR(20);V_D_CITY VARCHAR(20);
V_C_STREET_1 VARCHAR(20);V_C_STREET_2 VARCHAR(20);V_C_CITY VARCHAR(20);
V_C_FIRST VARCHAR(16);V_C_PHONE VARCHAR(16);
V_W_NAME VARCHAR(10);V_D_NAME VARCHAR(10);
V_W_ZIP VARCHAR(9);V_D_ZIP VARCHAR(9);V_C_ZIP VARCHAR(9);
V_C_DATA VARCHAR(500);
C_DATA_TMP VARCHAR(500);
P_C_NEW_DATA VARCHAR(500);
CNT INT;
CURSOR C1 IS SELECT C_ID 
FROM BMSQL_CUSTOMER 
WHERE C_LAST = V_C_LAST 
AND C_D_ID = V_D_ID 
AND C_W_ID = V_C_W_ID 
ORDER BY C_FIRST;
BEGIN
UPDATE BMSQL_WAREHOUSE 
SET W_YTD = W_YTD + V_A_AMOUNT 
WHERE W_ID = V_W_ID
RETURNING W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP 
INTO V_W_NAME, V_W_STREET_1, V_W_STREET_2, V_W_CITY, V_W_STATE, V_W_ZIP;
UPDATE BMSQL_DISTRICT 
SET    D_YTD = D_YTD + V_A_AMOUNT 
WHERE  D_W_ID = V_W_ID 
AND    D_ID = V_D_ID
RETURNING D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP
INTO V_D_NAME, V_D_STREET_1, V_D_STREET_2, V_D_CITY, V_D_STATE, V_D_ZIP;
IF(V_BYNAME = 1) THEN
SELECT COUNT(*) 
INTO   CNT 
FROM   BMSQL_CUSTOMER 
WHERE  C_LAST = V_C_LAST 
AND    C_D_ID = V_C_D_ID 
AND    C_W_ID = V_C_W_ID;
IF (MOD(CNT, 2) = 1) THEN
CNT := (CNT + 1);
END IF;
CNT := CNT / 2;
OPEN C1;
FOR I IN 1..CNT LOOP
FETCH C1 INTO V_C_ID;
END LOOP;
CLOSE C1;
END IF;
UPDATE BMSQL_CUSTOMER 
SET C_BALANCE = C_BALANCE - V_A_AMOUNT,
        C_YTD_PAYMENT = C_YTD_PAYMENT + V_A_AMOUNT,
        C_PAYMENT_CNT = C_PAYMENT_CNT + 1 
WHERE C_W_ID = V_C_W_ID 
AND C_D_ID = V_C_D_ID 
AND C_ID  = V_C_ID
RETURNING C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, 
C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE
INTO V_C_FIRST, V_C_MIDDLE, V_C_LAST, V_C_STREET_1, V_C_STREET_2, V_C_CITY, V_C_STATE, V_C_ZIP, 
V_C_PHONE, V_C_SINCE, V_C_CREDIT, V_C_CREDIT_LIM, V_C_DISCOUNT, V_C_BALANCE;
IF(V_C_CREDIT = 'BC') THEN
P_C_NEW_DATA := '' || V_C_ID || ' ' || V_C_D_ID || ' ' || V_C_W_ID || 
' ' || V_D_ID || ' ' || V_W_ID || ' ' || V_A_AMOUNT;
UPDATE BMSQL_CUSTOMER 
SET C_DATA = SUBSTR(P_C_NEW_DATA || C_DATA,
                          1,
                          500 - LENGTH(P_C_NEW_DATA)) 
WHERE C_W_ID = V_C_W_ID 
AND C_D_ID = V_C_D_ID 
AND   C_ID   = V_C_ID 
RETURNING C_DATA INTO C_DATA_TMP;
V_C_DATA := SUBSTR(C_DATA_TMP, 1, 200);
END IF;
V_H_DATE := CURRENT_DATE;
INSERT INTO BMSQL_HISTORY(H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID,H_W_ID, H_DATE, H_AMOUNT, H_DATA) 
VALUES (V_C_D_ID, V_C_W_ID, V_C_ID, V_D_ID, V_W_ID, V_H_DATE, V_A_AMOUNT, V_W_NAME || '    ' || V_D_NAME);
COMMIT;
V_RET:=1;
EXCEPTION WHEN OTHERS THEN
ROLLBACK;
V_RET:=0;
END;
/



CREATE OR REPLACE PROCEDURE TPCC_STOCKLEVEL(V_W_ID IN INT,V_D_ID IN INT,V_A IN INT, O_RET OUT INT) 
IS V_D_NEXT_O_ID INT;
V_DISTINCT_I_ID INT;
BEGIN
SELECT D_NEXT_O_ID INTO V_D_NEXT_O_ID FROM BMSQL_DISTRICT WHERE D_ID = V_D_ID AND D_W_ID = V_W_ID;
SELECT COUNT (DISTINCT S_I_ID) INTO V_DISTINCT_I_ID  FROM BMSQL_ORDER_LINE, BMSQL_STOCK
WHERE OL_D_ID=V_D_ID AND OL_W_ID =V_W_ID
AND OL_I_ID = S_I_ID AND S_W_ID=V_W_ID AND S_QUANTITY < V_A 
AND OL_O_ID BETWEEN (V_D_NEXT_O_ID - 20) AND (V_D_NEXT_O_ID - 1);
COMMIT;
O_RET:=1;
EXCEPTION WHEN OTHERS THEN
ROLLBACK;
O_RET:=0;
END;
/







-- 数据量查看
declare
  t_count   number(10);
  t_str VARCHAR2(500);
  cursor t_tables is select table_name from user_tables order by table_name;
begin
  for t_row in t_tables loop
    t_str := 'select count(*) from '|| t_row.table_name;
    execute immediate t_str into t_count;
    dbms_output.put_line( t_row.table_name || '=' || to_char(t_count));
  end loop;
end;
/ 
.

EXIT

!
