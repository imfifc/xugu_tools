# -*- coding: utf-8 -*-
import time

import faker
from faker import Faker

# 创建 Faker 实例
fake = Faker(locale='zh_CN')

# 生成姓名、地址、邮箱等数据
# name = fake.name()
# address = fake.address()
# email = fake.email()

# print(f"姓名: {name}\n地址: {address}\n邮箱: {email}")
start = time.time()
# max_length = 30  # 你想要的文本最大长度
for _ in range(100000):
    # data = fake.text(20)  # 固定长度
    # print(data)
    chinese_text = fake.text(max_nb_chars=100)
    # print(chinese_text)
    # print(fake.city())
    # print(fake.random_number())
print(time.time() - start)

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
