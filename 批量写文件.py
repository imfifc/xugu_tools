import datetime
import csv

data = [([{
    'Name': 'accountmoremenuinfo',
    'Engine': 'InnoDB',
    'Version': 10,
    'Row_format': 'Dynamic',
    'Rows': 8,
    'Avg_row_length': 2048,
    'Data_length': 16384,
    'Max_data_length': 0,
    'Index_length': 0,
    'Data_free': 0,
    'Auto_increment': None,
    'Create_time': datetime.datetime(2023, 6, 1, 15, 10, 16),
    'Update_time': None,
    'Check_time': None,
    'Collation': 'utf8mb3_general_ci',
    'Checksum': None,
    'Create_options': '',
    'Comment': ''
}], "db--table--sql: ecology--accountmoremenuinfo-- show table status from `ecology` like 'accountmoremenuinfo';"),
    ([{
        'Name': 'actionexecutelog',
        'Engine': 'InnoDB',
        'Version': 10,
        'Row_format': 'Dynamic',
        'Rows': 26,
        'Avg_row_length': 630,
        'Data_length': 16384,
        'Max_data_length': 0,
        'Index_length': 32768,
        'Data_free': 0,
        'Auto_increment': 28,
        'Create_time': datetime.datetime(2023, 6, 1, 15, 10, 36),
        'Update_time': None,
        'Check_time': None,
        'Collation': 'utf8mb3_general_ci',
        'Checksum': None,
        'Create_options': '',
        'Comment': ''
    }], "db--table--sql: ecology--actionexecutelog-- show table status from `ecology` like 'actionexecutelog';")]


def write_csv(data):
    with open('kk.txt', 'a+', newline='', encoding='utf-8') as f:
        # write = csv.writer(f)
        # if sql is not None:
        #     write.writerow([sql])
        fieldnames1 = data[0][0][0].keys()
        writer1 = csv.DictWriter(f, fieldnames=fieldnames1)
        write = csv.writer(f)

        for i in data:
            write.writerow([i[1]])
            writer1.writeheader()
            writer1.writerow(i[0][0])



write_csv(data)
