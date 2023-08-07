import csv
from pathlib import Path
import os
import time

# data = [{'DATA_TYPE': 'varchar', 'cnt': 391}, {'DATA_TYPE': 'bigint', 'cnt': 345}, {'DATA_TYPE': 'decimal', 'cnt': 182},
#         {'DATA_TYPE': 'longtext', 'cnt': 25}, {'DATA_TYPE': 'timestamp', 'cnt': 25}, {'DATA_TYPE': 'enum', 'cnt': 12},
#         {'DATA_TYPE': 'text', 'cnt': 11}, {'DATA_TYPE': 'datetime', 'cnt': 6}, {'DATA_TYPE': 'time', 'cnt': 6},
#         {'DATA_TYPE': 'mediumtext', 'cnt': 6}, {'DATA_TYPE': 'int', 'cnt': 6}]
#
# timestands = time.strftime('%Y年%m月%d日%H%M%S', time.localtime())
# csv_file_path = 'db.csv'
#
# # 获取所有字段名，这里假设所有字典中的键相同
# fieldnames = data[0].keys()
# print(fieldnames)
# BASE_PATH = str(Path(__file__).parent)
# dir = f'result_{timestands}'
# os.path.exists(dir) or os.makedirs(dir)
# # 使用'w'模式打开文件，将数据写入CSV文件
# with open(os.path.join(dir, csv_file_path), 'w', newline='') as f:
#     writer = csv.DictWriter(f, fieldnames=fieldnames)
#     writer.writeheader()
#     writer.writerows(data)
#     write = csv.writer(f)
#     write.writerow(['hhhh'])
# print("写入成功！")


data1 = [{
    'View': 'ADMINISTRABLE_ROLE_AUTHORIZATIONS',
    'Create View': "CREATE ALGORITHM=UNDEFINED DEFINER=`mysql.infoschema`@`localhost` SQL SECURITY DEFINER VIEW `information_schema`.`ADMINISTRABLE_ROLE_AUTHORIZATIONS` AS select `information_schema`.`applicable_roles`.`USER` AS `USER`,`information_schema`.`applicable_roles`.`HOST` AS `HOST`,`information_schema`.`applicable_roles`.`GRANTEE` AS `GRANTEE`,`information_schema`.`applicable_roles`.`GRANTEE_HOST` AS `GRANTEE_HOST`,`information_schema`.`applicable_roles`.`ROLE_NAME` AS `ROLE_NAME`,`information_schema`.`applicable_roles`.`ROLE_HOST` AS `ROLE_HOST`,`information_schema`.`applicable_roles`.`IS_GRANTABLE` AS `IS_GRANTABLE`,`information_schema`.`applicable_roles`.`IS_DEFAULT` AS `IS_DEFAULT`,`information_schema`.`applicable_roles`.`IS_MANDATORY` AS `IS_MANDATORY` from `information_schema`.`APPLICABLE_ROLES` where (`information_schema`.`applicable_roles`.`IS_GRANTABLE` = 'YES')",
    'character_set_client': 'utf8mb3',
    'collation_connection': 'utf8_general_ci'
}]

data = [(data1, 'sql')]
with open('kk.txt', 'a+', newline='', encoding='utf-8') as f:
    if data[0][0]:
        fieldnames = data[0][0][0].keys()
        writer1 = csv.DictWriter(f, fieldnames=fieldnames)
        write = csv.writer(f)

        for i in data:
            write.writerow([i[1]])
            # writer1.writeheader()
            fieldnames = i[0][0].keys()
            writer1.fieldnames = fieldnames
            writer1.writeheader()
            writer1.writerows(i[0])
