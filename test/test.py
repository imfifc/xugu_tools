import csv

data = [([{}], "db--table--sql: pro_hky_devi"), ([{'table_name': 'match', 'table_rows': 732}], "temp-22222")]

with open('kk.csv', 'a+', newline='', encoding='utf-8') as f:
    for i in data:
        if i[0] is not None and i[0][0]:
            fieldnames = i[0][0].keys()
            writer1 = csv.DictWriter(f, fieldnames=fieldnames)
            write = csv.writer(f)
            break

    for item in data:
        if item and item[0] is not None:
            datas, temp_sql = item
            write.writerow([temp_sql])
            fieldnames = datas[0].keys()
            writer1.fieldnames = fieldnames
            writer1.writeheader()
            writer1.writerows(datas)
