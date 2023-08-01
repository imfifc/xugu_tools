import concurrent.futures
import threading
import time

lock = threading.Lock()


# 假设这是一个耗时的函数，接受一个参数并返回一个结果
def some_function(input_value):
    # 这里可以是任何需要耗时计算的操作
    with lock:
        with open('kk.txt', 'a+') as f:
            f.write(f'{input_value}\n')


def main():
    input_data = list(range(10000))  # 假设这是输入数据

    # 创建线程池
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        # 使用 map 方法来调用函数并获取结果，它会按照原始输入的顺序返回结果
        results = list(executor.map(some_function, input_data))

    # 打印结果
    print("原始输入数据:", input_data)
    print("对应结果:", results)


#     同步
def main_tb():
    data = list(range(10000))
    for i in data:
        some_function(i)


if __name__ == "__main__":
    start = time.time()
    main()  # 10000
    # main_tb() #  10000 45.40183138847351
    print(time.time() - start)
    # 3.9458184242248535
