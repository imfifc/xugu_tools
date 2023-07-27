import concurrent.futures


def write_to_file(filename, data):
    with open(filename, 'a') as file:
        file.write(data + '\n')


def main():
    filename = 'data.txt'
    data_to_write = ['Data from Thread 1', 'Data from Thread 2', 'Data from Thread 3']

    # 使用ThreadPoolExecutor创建一个线程池
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(write_to_file, filename, data) for data in data_to_write]

        # 等待所有任务完成
        concurrent.futures.wait(futures)


if __name__ == "__main__":
    main()
