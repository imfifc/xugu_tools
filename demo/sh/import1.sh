#!/bin/bash

xgconsole nssl 127.0.0.1 5190 NACOS SYSDBA SYSDBA <<! 
 
 

-- SYSDBA."config_info" definition

-- Create Table --
create table SYSDBA.config_info(
id bigint not null comment 'id',
data_id varchar(255) not null comment 'data_id',
group_id varchar(255),
content clob not null comment 'content',
md5 varchar(32) comment 'md5',
gmt_create datetime not null default CURRENT_TIMESTAMP comment '创建时间',
gmt_modified datetime not null default CURRENT_TIMESTAMP comment '修改时间',
src_user clob comment 'source user',
src_ip varchar(50) comment 'source ip',
app_name varchar(128),
tenant_id varchar(128) default '' comment '租户字段',
c_desc varchar(256),
c_use varchar(64),
effect varchar(64),
type varchar(64),
c_schema clob
);
comment on table SYSDBA.config_info is 'config_info';
-- Alter Table Add Identity --
alter table SYSDBA.config_info alter column id BIGINT identity(4,1) primary key;


-- Create Table Index --
create unique index UK_IDX_S13423164925885134 on SYSDBA.config_info(data_id,group_id,tenant_id) indextype is btree global ;


-- SYSDBA."config_info_aggr" definition

-- Create Table --
create table SYSDBA.config_info_aggr(
id bigint not null comment 'id',
data_id varchar(255) not null comment 'data_id',
group_id varchar(255) not null comment 'group_id',
datum_id varchar(255) not null comment 'datum_id',
content clob not null comment '内容',
gmt_modified datetime not null comment '修改时间',
app_name varchar(128),
tenant_id varchar(128) default '' comment '租户字段'
);
comment on table SYSDBA.config_info_aggr is '增加租户字段';
-- Alter Table Add Identity --
alter table SYSDBA.config_info_aggr alter column id BIGINT identity(1,1) primary key;

-- Create Table Index --
create unique index UK_IDX_S31441164925885131 on SYSDBA.config_info_aggr(data_id,group_id,tenant_id,datum_id) indextype is btree global ;


-- SYSDBA."config_info_beta" definition

-- Create Table --
create table SYSDBA.config_info_beta(
id bigint not null comment 'id',
data_id varchar(255) not null comment 'data_id',
group_id varchar(128) not null comment 'group_id',
app_name varchar(128) comment 'app_name',
content clob not null comment 'content',
beta_ips varchar(1024) comment 'betaIps',
md5 varchar(32) comment 'md5',
gmt_create datetime not null default CURRENT_TIMESTAMP comment '创建时间',
gmt_modified datetime not null default CURRENT_TIMESTAMP comment '修改时间',
src_user clob comment 'source user',
src_ip varchar(50) comment 'source ip',
tenant_id varchar(128) default '' comment '租户字段'
);
comment on table SYSDBA.config_info_beta is 'config_info_beta';
-- Alter Table Add Identity --
alter table SYSDBA.config_info_beta alter column id BIGINT identity(1,1) primary key;


-- Create Table Index --
create unique index UK_IDX_S5039164925885132 on SYSDBA.config_info_beta(data_id,group_id,tenant_id) indextype is btree global ;


-- SYSDBA."config_info_tag" definition

-- Create Table --
create table SYSDBA.config_info_tag(
id bigint not null comment 'id',
data_id varchar(255) not null comment 'data_id',
group_id varchar(128) not null comment 'group_id',
tenant_id varchar(128) default '' comment 'tenant_id',
tag_id varchar(128) not null comment 'tag_id',
app_name varchar(128) comment 'app_name',
content clob not null comment 'content',
md5 varchar(32) comment 'md5',
gmt_create datetime not null default CURRENT_TIMESTAMP comment '创建时间',
gmt_modified datetime not null default CURRENT_TIMESTAMP comment '修改时间',
src_user clob comment 'source user',
src_ip varchar(50) comment 'source ip'
);
comment on table SYSDBA.config_info_tag is 'config_info_tag';
-- Alter Table Add Identity --
alter table SYSDBA.config_info_tag alter column id BIGINT identity(1,1) primary key;


-- Create Table Index --
create unique index UK_IDX_S29564164925885133 on SYSDBA.config_info_tag(data_id,group_id,tenant_id,tag_id) indextype is btree global ;


-- SYSDBA."config_tags_relation" definition

-- Create Table --
create table SYSDBA.config_tags_relation(
id bigint not null comment 'id',
tag_name varchar(128) not null comment 'tag_name',
tag_type varchar(64) comment 'tag_type',
data_id varchar(255) not null comment 'data_id',
group_id varchar(128) not null comment 'group_id',
tenant_id varchar(128) default '' comment 'tenant_id',
nid bigint not null
);
comment on table SYSDBA.config_tags_relation is 'config_tag_relation';
-- Alter Table Add Identity --
alter table SYSDBA.config_tags_relation alter column nid BIGINT identity(1,1) primary key;

-- Create Table Index --
create index IDX_TENANT_ID on SYSDBA.config_tags_relation(tenant_id) indextype is btree global ;
-- Create Table Index --
create unique index UK_IDX_S22394164925885135 on SYSDBA.config_tags_relation(id,tag_name,tag_type) indextype is btree global ;


-- SYSDBA."group_capacity" definition

-- Create Table --
create table SYSDBA.group_capacity(
id bigint not null comment '主键ID',
group_id varchar(128) not null default '' comment 'Group ID，空字符表示整个集群',
quota bigint not null default 0 comment '配额，0表示使用默认值',
usage bigint not null default 0 comment '使用量',
max_size bigint not null default 0 comment '单个配置大小上限，单位为字节，0表示使用默认值',
max_aggr_count bigint not null default 0 comment '聚合子配置最大个数，，0表示使用默认值',
max_aggr_size bigint not null default 0 comment '单个聚合数据的子配置大小上限，单位为字节，0表示使用默认值',
max_history_count bigint not null default 0 comment '最大变更历史数量',
gmt_create datetime not null default CURRENT_TIMESTAMP comment '创建时间',
gmt_modified datetime not null default CURRENT_TIMESTAMP comment '修改时间'
);
comment on table SYSDBA.group_capacity is '集群、各Group容量信息表';
-- Alter Table Add Identity --
alter table SYSDBA.group_capacity alter column id BIGINT identity(1,1) primary key;

-- Create Table Index --
create unique index UK_IDX_S22111164925885136 on SYSDBA.group_capacity(group_id) indextype is btree global ;


-- SYSDBA."his_config_info" definition

-- Create Table --
create table SYSDBA.his_config_info(
id bigint not null,
nid bigint not null,
data_id varchar(255) not null,
group_id varchar(128) not null,
app_name varchar(128) comment 'app_name',
content clob not null,
md5 varchar(32),
gmt_create datetime not null default CURRENT_TIMESTAMP,
gmt_modified datetime not null default CURRENT_TIMESTAMP,
src_user clob,
src_ip varchar(50),
op_type varchar(10),
tenant_id varchar(128) default '' comment '租户字段'
);
comment on table SYSDBA.his_config_info is '多租户改造';
-- Alter Table Add Identity --
alter table SYSDBA.his_config_info alter column nid BIGINT identity(3,1) primary key;

create index IDX_DID on SYSDBA.his_config_info(data_id) indextype is btree global ;
-- Create Table Index --
create index IDX_GMT_CREATE on SYSDBA.his_config_info(gmt_create) indextype is btree global ;
-- Create Table Index --
create index IDX_GMT_MODIFIED on SYSDBA.his_config_info(gmt_modified) indextype is btree global ;


-- SYSDBA."permissions" definition

-- Create Table --
create table SYSDBA.permissions(
role varchar(50) not null,
resource varchar(255) not null,
action varchar(8) not null
);

-- Create Table Index --
create unique index UK_IDX_S24855164925885137 on SYSDBA.permissions(role,resource,action) indextype is btree global ;


-- SYSDBA."roles" definition

-- Create Table --
create table SYSDBA.roles(
username varchar(50) not null,
role varchar(50) not null
);

-- Create Table Index --
create unique index UK_IDX_S10142164925885138 on SYSDBA.roles(username,role) indextype is btree global ;


-- SYSDBA."tenant_capacity" definition

-- Create Table --
create table SYSDBA.tenant_capacity(
id bigint not null comment '主键ID',
tenant_id varchar(128) not null default '' comment 'Tenant ID',
quota bigint not null default 0 comment '配额，0表示使用默认值',
usage bigint not null default 0 comment '使用量',
max_size bigint not null default 0 comment '单个配置大小上限，单位为字节，0表示使用默认值',
max_aggr_count bigint not null default 0 comment '聚合子配置最大个数',
max_aggr_size bigint not null default 0 comment '单个聚合数据的子配置大小上限，单位为字节，0表示使用默认值',
max_history_count bigint not null default 0 comment '最大变更历史数量',
gmt_create datetime not null default CURRENT_TIMESTAMP comment '创建时间',
gmt_modified datetime not null default CURRENT_TIMESTAMP comment '修改时间'
);
comment on table SYSDBA.tenant_capacity is '租户容量信息表';
-- Alter Table Add Identity --
alter table SYSDBA.tenant_capacity alter column id BIGINT identity(1,1) primary key;

-- Create Table Index --
create unique index UK_IDX_S16456164925885139 on SYSDBA.tenant_capacity(tenant_id) indextype is btree global ;


-- SYSDBA."tenant_info" definition

-- Create Table --
create table SYSDBA.tenant_info(
id bigint not null comment 'id',
kp varchar(128) not null comment 'kp',
tenant_id varchar(128) default '' comment 'tenant_id',
tenant_name varchar(128) default '' comment 'tenant_name',
tenant_desc varchar(256) comment 'tenant_desc',
create_source varchar(32) comment 'create_source',
gmt_create bigint not null comment '创建时间',
gmt_modified bigint not null comment '修改时间'
);
comment on table SYSDBA.tenant_info is 'tenant_info';
-- Alter Table Add Identity --
alter table SYSDBA.tenant_info alter column id BIGINT identity(1,1) primary key;

-- Create Table Index --
create index IDX_TENANT_ID on SYSDBA.tenant_info(tenant_id) indextype is btree global ;
-- Create Table Index --
create unique index UK_IDX_S26416164925885140 on SYSDBA.tenant_info(kp,tenant_id) indextype is btree global ;


-- SYSDBA."users" definition

-- Create Table --
create table SYSDBA.users(
username varchar(50) not null,
password varchar(500) not null,
enabled boolean not null
);
-- Alter Table Add PrimaryKey Constraint --
alter table SYSDBA.users add constraint "PRIMARY" primary key(username);




INSERT INTO SYSDBA.users (username, password, enabled) VALUES ('nacos', '$2a$10$EuWPZHzz32dJN7jexM34MOeYirDdFAZm2kuWj7VEOJhhZkDrxfvUu', TRUE);

INSERT INTO SYSDBA.roles (username, role) VALUES ('nacos', 'ROLE_ADMIN');
 
 
 
 
 EXIT

!
