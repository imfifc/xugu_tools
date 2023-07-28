import concurrent.futures


# 假设这是一个耗时的函数，接受一个参数并返回一个结果
def some_function(input_value):
    # 这里可以是任何需要耗时计算的操作
    result = input_value * 2
    return result


def main():
    input_data = [1, 2, 3, 4, 5]  # 假设这是输入数据

    # 创建线程池
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # 使用 map 方法来调用函数并获取结果，它会按照原始输入的顺序返回结果
        results = list(executor.map(some_function, input_data))

    # 打印结果
    print("原始输入数据:", input_data)
    print("对应结果:", results)


if __name__ == "__main__":
    main()
