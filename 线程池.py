import concurrent.futures
import time
from functools import partial

# 定义要执行的无参数函数任务
def function_task1():
    print("Function task 1 executing")
    time.sleep(2)  # 模拟任务执行的耗时操作
    return "Task 1 completed"

def function_task2():
    print("Function task 2 executing")
    time.sleep(1)  # 模拟任务执行的耗时操作
    return "Task 2 completed"

def function_task3():
    print("Function task 3 executing")
    time.sleep(3)  # 模拟任务执行的耗时操作
    return "Task 3 completed"

def main():
    # 创建一个线程池，其中包含3个线程
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # 使用functools.partial创建多个无参数的任务函数
        task_functions = [partial(function_task1), partial(function_task2), partial(function_task3)]

        # 提交多个函数任务到线程池中
        futures = [executor.submit(task_function) for task_function in task_functions]

        # 获取并处理任务的返回结果
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                print(f"Function task returned: {result}")
            except Exception as e:
                print(f"Function task encountered an error: {e}")

if __name__ == "__main__":
    main()
