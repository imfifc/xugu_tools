# -*- coding: utf-8 -*-
import time

import faker
from faker import Faker

# 创建 Faker 实例
fake = Faker(locale='zh_CN')

# 生成姓名、地址、邮箱等数据
name = fake.name()
address = fake.address()
email = fake.email()

print(f"姓名: {name}\n地址: {address}\n邮箱: {email}")

# max_length = 30  # 你想要的文本最大长度
# for _ in range(30):
#     data = fake.text(20)  # 固定长度
#     print(data)
#     chinese_text = fake.text(max_nb_chars=max_length)
#     print(chinese_text)

start = time.time()
data = fake.text(1000)  # 固定长度
# print(data)
print(time.time() - start)  # 0.044634103775024414

import random


def generate_random_chinese(length):
    code_points = [random.randint(0x4e00, 0x9fff) for _ in range(length)]
    return ''.join(chr(code_point) for code_point in code_points)


st = time.time()
generate_random_chinese(1000)
print(time.time() - st)


# 存储过程中不能有注释
def create_random_package():
    cur = get_cur(db_host, db_port, db_user, db_pwd, db_name)
    pkg_header = """
    CREATE OR REPLACE PACKAGE random IS
      FUNCTION value(min_value bigint, max_value bigint) return bigint;
      FUNCTION string(length IN NUMBER) RETURN varchar2;
      FUNCTION chinese_string(length IN NUMBER) RETURN varchar2;
    END;
    """
    pkg_body = """
       CREATE OR REPLACE PACKAGE BODY random as
        function value(min_value bigint, max_value bigint) return bigint as
            div  bigint := power(2, 31)-1;
            tmp_value  double;
            ret_value  bigint;
        begin
            tmp_value := to_number(abs(rand())) / div * (max_value - min_value);
            ret_value := round(tmp_value, 0) + min_value;
            return ret_value;
        end;

        FUNCTION string(length IN NUMBER) RETURN VARCHAR2 IS
           characters VARCHAR2(62) := 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
           random_string VARCHAR2(32767) := '';
        BEGIN
           FOR i IN 1..length LOOP
              random_string := random_string || SUBSTR(characters, CEIL(value(1, 62)), 1);
           END LOOP;
           RETURN random_string;
        end;

        FUNCTION chinese_string(length IN NUMBER) RETURN VARCHAR2 IS
           characters VARCHAR2(6200) := '啊阿埃挨哎唉哀皑癌蔼矮艾碍爱隘鞍氨安俺按暗';
           random_string VARCHAR2 := '';
        BEGIN
           FOR i IN 1..length LOOP
              random_string := random_string || SUBSTR(characters, CEIL(value(1, 6200)), 1);
           END LOOP;
           RETURN random_string;
        end;
    end random;
    """
    cur.execute(pkg_header)
    cur.execute(pkg_body)


from datetime import datetime, timedelta

def generate_dates(n):
    # 获取当前日期
    current_date = datetime.now()

    # 生成 n 个日期
    date_list = [current_date + timedelta(days=i) for i in range(n)]

    # 格式化日期为字符串
    date_strings = [date.strftime("%Y-%m-%d") for date in date_list]

    return date_strings

data = generate_dates(5)
print(data)
