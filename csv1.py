import csv

data = [{'DATA_TYPE': 'varchar', 'cnt': 391}, {'DATA_TYPE': 'bigint', 'cnt': 345}, {'DATA_TYPE': 'decimal', 'cnt': 182},
        {'DATA_TYPE': 'longtext', 'cnt': 25}, {'DATA_TYPE': 'timestamp', 'cnt': 25}, {'DATA_TYPE': 'enum', 'cnt': 12},
        {'DATA_TYPE': 'text', 'cnt': 11}, {'DATA_TYPE': 'datetime', 'cnt': 6}, {'DATA_TYPE': 'time', 'cnt': 6},
        {'DATA_TYPE': 'mediumtext', 'cnt': 6}, {'DATA_TYPE': 'int', 'cnt': 6}]

csv_file_path = 'db.csv'

# 获取所有字段名，这里假设所有字典中的键相同
fieldnames = data[0].keys()
print(fieldnames)

# 使用'w'模式打开文件，将数据写入CSV文件
with open(csv_file_path, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(data)
    write = csv.writer(f)
    write.writerow(['hhhh'])
print("写入成功！")
