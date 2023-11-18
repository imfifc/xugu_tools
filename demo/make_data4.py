# -*- coding: utf-8 -*-
import argparse
import multiprocessing
import queue
import sys
import time
from datetime import datetime, timedelta
from multiprocessing import freeze_support

import xgcondb


def get_cur(db_host, db_port, db_user, db_pwd, db_name):
    conn = xgcondb.connect(host=db_host, port=db_port, database=db_name, user=db_user, password=db_pwd, charset="utf8")
    cur = conn.cursor()
    return cur


# 参数解析前置，多进程才能不报错
def parse_args():
    parser = argparse.ArgumentParser(
        # description='这是一个数据库环境采集工具',
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

    # 在这里可以根据解析的参数执行相应的操作
    if len(sys.argv) == 1:
        host = input("请输入ip: ")
        port = input("请输入端口: ")
        user = input("请输入用户: ")
        password = input("请输入密码: ")
        db = input("请输入数据库: ")
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
    # if host and port and user and password and db:
    #     print(f'host: {host} port: {port} user: {user} password: {password} db: {db} \n')

    return host, port, user, password, db


# 存储过程中不能有注释
def create_random_package():
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    pkg_header = """
    CREATE OR REPLACE PACKAGE random is
      function value(min_value bigint ,max_value bigint) return bigint ;
      FUNCTION string(length IN NUMBER) RETURN varchar2;
      FUNCTION chinese_string(length IN NUMBER) RETURN varchar2;
    END;
    """
    pkg_body = """
    CREATE OR REPLACE PACKAGE BODY random as
        function value(min_value bigint, max_value bigint) return bigint as
            tmp_value  double;
            ret_value  bigint;
        begin
            return mod(rand(),max_value-min_value)+min_value;
        end;

        FUNCTION string(length IN NUMBER) RETURN VARCHAR2 IS
           characters VARCHAR2(62) := 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
           random_string VARCHAR2(32767) := '';
        BEGIN
           FOR i IN 1..length LOOP
              random_string := random_string || SUBSTR(characters,mod(rand(),61)+1, 1);
           END LOOP;
           RETURN random_string;
        end;

        FUNCTION chinese_string(length IN NUMBER) RETURN VARCHAR2 is
          random_string VARCHAR2 := '';
          characters VARCHAR2(58) := '啊阿埃挨哎唉哀皑癌蔼矮艾碍爱隘鞍氨安俺按暗岸胺案肮昂盎凹敖熬翱袄傲奥懊澳芭捌扒叭吧笆八疤巴拔跋靶把耙坝霸罢爸白柏百摆';
        BEGIN
           FOR i IN 1..length LOOP
              random_string := random_string || SUBSTR(characters, CEIL(value(1, 58)), 1);
           END LOOP;
           RETURN random_string;
        end;

    end random;
    """
    cur.execute(pkg_header)
    cur.execute(pkg_body)


def drop_tb(table):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = f"drop table if exists {table} cascade"
    cur.execute(sql)


def create_product_tb():
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = f"""
    create table product4(
    product_no varchar(50) not null,
    product_name varchar(200),
    product_introduce varchar(4000),
    manufacture_date date,
    sell_date datetime,
    address varchar(200),
    product_type varchar(50)
    )PARTITION BY RANGE(sell_date)INTERVAL 1 DAY PARTITIONS(('1970-01-01 00:00:00')) ;
    """
    cur.execute(sql)


def create_proc():
    """
     num: 往临时表单次的插入数据
    :return:
    """
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = f"""
    create or replace procedure pr_test_insert4(insert_num in int, insert_date in date) is
    declare
    TYPE t_var IS varray(30) OF VARCHAR;
    TYPE t_varr IS VARRAY(2000) OF varchar2(1);
    arr t_varr;
    city_var t_var;
    add_num int;
    case_num int;
    rand_len int;
    arr_len int;
    ss varchar2 :='';
    str varchar2 ;
    begin
    city_var.EXTEND(30);
    city_var(1):='北京';
    city_var(2):='上海';
    city_var(3):='成都';
    city_var(4):='青岛';
    city_var(5):='广州';
    city_var(6):='香港';
    city_var(7):='西安';
    city_var(8):='拉萨';
    city_var(9):='杭州';
    city_var(10):='深圳';
    city_var(11):='汽车';
    city_var(12):='手机';
    city_var(13):='日用品';
    city_var(14):='电脑';
    city_var(15):='海鲜';
    city_var(16):='植物';
    city_var(17):='家具';
    city_var(18):='服装';
    city_var(19):='书籍';
    city_var(20):='饮料';

     arr:=t_varr('啊', '阿', '埃', '挨', '哎', '唉', '哀', '皑', '癌', '蔼', '矮', '艾', '碍', '爱', '隘', '鞍', '氨', '安', '俺', '按', '暗', '岸', '胺', '案', '肮', '昂', '盎', '凹', '敖', '熬', '翱', '袄', '傲', '奥', '懊', '澳', '芭', '捌', '扒', '叭', '吧', '笆', '八', '疤', '巴', '拔', '跋', '靶', '把', '耙', '坝', '霸', '罢', '爸', '白', '柏', '百', '摆', '佰', '败', '拜', '稗', '斑', '班', '搬', '扳', '般', '颁', '板', '版', '扮', '拌', '伴', '瓣', '半', '办', '绊', '邦', '帮', '梆', '榜', '膀', '绑', '棒', '磅', '蚌', '镑', '傍', '谤', '苞', '胞', '包', '褒', '剥', '薄', '雹', '保', '堡', '饱', '宝', '抱', '报', '暴', '豹', '鲍', '爆', '杯', '碑', '悲', '卑', '北', '辈', '背', '贝', '钡', '倍', '狈', '备', '惫', '焙', '被', '奔', '苯', '本', '笨', '崩', '绷', '甭', '泵', '蹦', '迸', '逼', '鼻', '比', '鄙', '笔', '彼', '碧', '蓖', '蔽', '毕', '毙', '毖', '币', '庇', '痹', '闭', '敝', '弊', '必', '辟', '壁', '臂', '避', '陛', '鞭', '边', '编', '贬', '扁', '便', '变', '卞', '辨', '辩', '辫', '遍', '标', '彪', '膘', '表', '鳖', '憋', '别', '瘪', '彬', '斌', '濒', '滨', '宾', '摈', '兵', '冰', '柄', '丙', '秉', '饼', '炳', '病', '并', '玻', '菠', '播', '拨', '钵', '波', '博', '勃', '搏', '铂', '箔', '伯', '帛', '舶', '脖', '膊', '渤', '泊', '驳', '捕', '卜', '哺', '补', '埠', '不', '布', '步', '簿', '部', '怖', '擦', '猜', '裁', '材', '才', '财', '睬', '踩', '采', '彩', '菜', '蔡', '餐', '参', '蚕', '残', '惭', '惨', '灿', '苍', '舱', '仓', '沧', '藏', '操', '糙', '槽', '曹', '草', '厕', '策', '侧', '册', '测', '层', '蹭', '插', '叉', '茬', '茶', '查', '碴', '搽', '察', '岔', '差', '诧', '拆', '柴', '豺', '搀', '掺', '蝉', '馋', '谗', '缠', '铲', '产', '阐', '颤', '昌', '猖', '场', '尝', '常', '长', '偿', '肠', '厂', '敞', '畅', '唱', '倡', '超', '抄', '钞', '朝', '嘲', '潮', '巢', '吵', '炒', '车', '扯', '撤', '掣', '彻', '澈', '郴', '臣', '辰', '尘', '晨', '忱', '沉', '陈', '趁', '衬', '撑', '称', '城', '橙', '成', '呈', '乘', '程', '惩', '澄', '诚', '承', '逞', '骋', '秤', '吃', '痴', '持', '匙', '池', '迟', '弛', '驰', '耻', '齿', '侈', '尺', '赤', '翅', '斥', '炽', '充', '冲', '虫', '崇', '宠', '抽', '酬', '畴', '踌', '稠', '愁', '筹', '仇', '绸', '瞅', '丑', '臭', '初', '出', '橱', '厨', '躇', '锄', '雏', '滁', '除', '楚', '础', '储', '矗', '搐', '触', '处', '揣', '川', '穿', '椽', '传', '船', '喘', '串', '疮', '窗', '幢', '床', '闯', '创', '吹', '炊', '捶', '锤', '垂', '春', '椿', '醇', '唇', '淳', '纯', '蠢', '戳', '绰', '疵', '茨', '磁', '雌', '辞', '慈', '瓷', '词', '此', '刺', '赐', '次', '聪', '葱', '囱', '匆', '从', '丛', '凑', '粗', '醋', '簇', '促', '蹿', '篡', '窜', '摧', '崔', '催', '脆', '瘁', '粹', '淬', '翠', '村', '存', '寸', '磋', '撮', '搓', '措', '挫', '错', '搭', '达', '答', '瘩', '打', '大', '呆', '歹', '傣', '戴', '带', '殆', '代', '贷', '袋', '待', '逮', '怠', '耽', '担', '丹', '单', '郸', '掸', '胆', '旦', '氮', '但', '惮', '淡', '诞', '弹', '蛋', '当', '挡', '党', '荡', '档', '刀', '捣', '蹈', '倒', '岛', '祷', '导', '到', '稻', '悼', '道', '盗', '德', '得', '的', '蹬', '灯', '登', '等', '瞪', '凳', '邓', '堤', '低', '滴', '迪', '敌', '笛', '狄', '涤', '翟', '嫡', '抵', '底', '地', '蒂', '第', '帝', '弟', '递', '缔', '颠', '掂', '滇', '碘', '点', '典', '靛', '垫', '电', '佃', '甸', '店', '惦', '奠', '淀', '殿', '碉', '叼', '雕', '凋', '刁', '掉', '吊', '钓', '调', '跌', '爹', '碟', '蝶', '迭', '谍', '叠', '丁', '盯', '叮', '钉', '顶', '鼎', '锭', '定', '订', '丢', '东', '冬', '董', '懂', '动', '栋', '侗', '恫', '冻', '洞', '兜', '抖', '斗', '陡', '豆', '逗', '痘', '都', '督', '毒', '犊', '独', '读', '堵', '睹', '赌', '杜', '镀', '肚', '度', '渡', '妒', '端', '短', '锻', '段', '断', '缎', '堆', '兑', '队', '对', '墩', '吨', '蹲', '敦', '顿', '囤', '钝', '盾', '遁', '掇', '哆', '多', '夺', '垛', '躲', '朵', '跺', '舵', '剁', '惰', '堕', '蛾', '峨', '鹅', '俄', '额', '讹', '娥', '恶', '厄', '扼', '遏', '鄂', '饿', '恩', '而', '儿', '耳', '尔', '饵', '洱', '二', '贰', '发', '罚', '筏', '伐', '乏', '阀', '法', '珐', '藩', '帆', '番', '翻', '樊', '矾', '钒', '繁', '凡', '烦', '反', '返', '范', '贩', '犯', '饭', '泛', '坊', '芳', '方', '肪', '房', '防', '妨', '仿', '访', '纺', '放', '菲', '非', '啡', '飞', '肥', '匪', '诽', '吠', '肺', '废', '沸', '费', '芬', '酚', '吩', '氛', '分', '纷', '坟', '焚', '汾', '粉', '奋', '份', '忿', '愤', '粪', '丰', '封', '枫', '蜂', '峰', '锋', '风', '疯', '烽', '逢', '冯', '缝', '讽', '奉', '凤', '佛', '否', '夫', '敷', '肤', '孵', '扶', '拂', '辐', '幅', '氟', '符', '伏', '俘', '服', '浮', '涪', '福', '袱', '弗', '甫', '抚', '辅', '俯', '釜', '斧', '脯', '腑', '府', '腐', '赴', '副', '覆', '赋', '复', '傅', '付', '阜', '父', '腹', '负', '富', '讣', '附', '妇', '缚', '咐', '噶', '嘎', '该', '改', '概', '钙', '盖', '溉', '干', '甘', '杆', '柑', '竿', '肝', '赶', '感', '秆', '敢', '赣', '冈', '刚', '钢', '缸', '肛', '纲', '岗', '港', '杠', '篙', '皋', '高', '膏', '羔', '糕', '搞', '镐', '稿', '告', '哥', '歌', '搁', '戈', '鸽', '胳', '疙', '割', '革', '葛', '格', '蛤', '阁', '隔', '铬', '个', '各', '给', '根', '跟', '耕', '更', '庚', '羹', '埂', '耿', '梗', '工', '攻', '功', '恭', '龚', '供', '躬', '公', '宫', '弓', '巩', '汞', '拱', '贡', '共', '钩', '勾', '沟', '苟', '狗', '垢', '构', '购', '够', '辜', '菇', '咕', '箍', '估', '沽', '孤', '姑', '鼓', '古', '蛊', '骨', '谷', '股', '故', '顾', '固', '雇', '刮', '瓜', '剐', '寡', '挂', '褂', '乖', '拐', '怪', '棺', '关', '官', '冠', '观', '管', '馆', '罐', '惯', '灌', '贯', '光', '广', '逛', '瑰', '规', '圭', '硅', '归', '龟', '闺', '轨', '鬼', '诡', '癸', '桂', '柜', '跪', '贵', '刽', '辊', '滚', '棍', '锅', '郭', '国', '果', '裹', '过', '哈', '骸', '孩', '海', '氦', '亥', '害', '骇', '酣', '憨', '邯', '韩', '含', '涵', '寒', '函', '喊', '罕', '翰', '撼', '捍', '旱', '憾', '悍', '焊', '汗', '汉', '夯', '杭', '航', '壕', '嚎', '豪', '毫', '郝', '好', '耗', '号', '浩', '呵', '喝', '荷', '菏', '核', '禾', '和', '何', '合', '盒', '貉', '阂', '河', '涸', '赫', '褐', '鹤', '贺', '嘿', '黑', '痕', '很');
     arr_len:=arr.count();
    for i in 1..insert_num loop
     add_num := mod(rand,10)+1;
     case_num  :=mod(rand,10)+11;
      rand_len :=mod(rand,99)+1;
            for i in 1..rand_len loop
                 ss := ss || arr(mod(rand(),arr_len)+1);   
            END LOOP;
     insert into product4 values (sys_guid(),'零食大礼包'||MOD(rand(),10),
                        ss,
                        to_date('2017-01-01 00:00:00','yyyy-mm-dd hh24:mi:ss')+i,
                        insert_date,
                        city_var(add_num),
                        city_var(case_num)
     );

      ss :='';
     if mod(i,10000)=0 then
        commit;
     end if;
     end loop;
    commit;
    end pr_test_insert4;
    """
    cur.execute("truncate table SYSDBA.product4 ")
    cur.execute(sql)


def show(table):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    sql = f'select count(*) from {table}'
    data = cur.execute(sql)
    row = cur.fetchone()
    print(f'{table} : {row}')


def rebuild_tables():
    """
    重建表： 先删除后重建
    :return:
    """
    tables = ['product4']
    tables = [f'{db_user}.{i}' for i in tables]
    for table in tables:
        drop_tb(table)

    create_product_tb()


def generate_dates(n):
    current_date = datetime.now()
    date_list = [current_date + timedelta(days=i) for i in range(n)]
    date_strings = [date.strftime("%Y-%m-%d") for date in date_list]
    return date_strings


def execute_proc_args(name):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    # print(cur.callproc("test_in", (20,), (1,)))
    cur.callproc(name, (200000,), (1,))


def execute_proc(name, rows, date, db_host, db_port, db_user, db_pwd, db_name):
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    cur.callproc(name, (rows, date), (1, 1))
    # if len(args):
    #     print(name, tuple(args), tuple(1 for _ in range(len(args))))
    #     cur.callproc(name, tuple(args), tuple(1 for _ in range(len(args))))
    # else:
    #     cur.callproc(name)


# 多进程调用
def multi_process(proc_name, rows, dates, db_host, db_port, db_user, db_pwd, db_name):
    processes = []
    for date in dates:
        process = multiprocessing.Process(target=execute_proc,
                                          args=(proc_name, rows, date, db_host, db_port, db_user, db_pwd, db_name))
        processes.append(process)
    for process in processes:
        process.start()
    # 等待所有进程完成
    for process in processes:
        process.join()


def once_proc():
    rows = int(input("请输入表行数: "))
    days = int(input("请输入天数: "))

    # parallel_n = int(input("请输入并发数: "))
    dates = generate_dates(days)
    print(dates)
    create_proc()
    start = time.time()
    multi_process('pr_test_insert4', rows, dates, db_host, db_port, db_user, db_pwd, db_name)
    end = time.time() - start
    show('product4')
    print(f'耗时{end:.2f}秒', f'tps:{(rows * days / end):.2f} 行/s')


if __name__ == '__main__':
    freeze_support()
    # db_host = '10.28.20.101'
    # db_port = 6325
    # db_user = 'SYSDBA'
    # db_pwd = 'SYSDBA'
    # db_name = 'SYSTEM'
    db_host, db_port, db_user, db_pwd, db_name = parse_args()
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    create_random_package()
    cur.execute('set max_loop_num to 0')
    cur.execute('set max_trans_modify to 0')
    # 目的： 从临时表中取出1w数据到正式表
    rebuild_tables()
    while True:
        once_proc()
        flag = input("是否需要清除表重建，(默认不重建) 请输入Y/N: ")
        if flag == 'Y' or flag == 'y':
            rebuild_tables()
            print('已重建表')
        q = input('\nPress q to exit…or continue')
        if q == 'q' or q == 'Q':
            break

#   每天500w, 加一个中文字段1-100随机字符，一个月数据
