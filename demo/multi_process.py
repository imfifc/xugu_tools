# import multiprocessing
#
#
# # 定义一个接受多个参数的函数
# def worker_function(process_id, arg1, arg2):
#     print(f"进程 {process_id} 正在执行，参数1: {arg1}, 参数2: {arg2}")
#
#
# if __name__ == "__main__":
#     num_processes = 4
#     pool = multiprocessing.Pool(processes=num_processes)
#
#     process_args = [(i, f"参数1_{i}", f"参数2_{i}") for i in range(num_processes)]
#     print(process_args)
#
#     pool.starmap(worker_function, process_args)
#
#     pool.close()
#     pool.join()
import concurrent.futures
from functools import partial


# 定义一个接受多个参数的函数
def worker_function(process_id, arg1, arg2):
    print(f"进程 {process_id} 正在执行，参数1: {arg1}, 参数2: {arg2}")


if __name__ == "__main__":
    num_processes = 4
    process_args = [(i, f"参数1_{i}", f"参数2_{i}") for i in range(num_processes)]
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_processes) as executor:
        # 使用列表解析来传递多个参数
        results = [executor.submit(worker_function, *args) for args in process_args]
        for future in concurrent.futures.as_completed(results):
            future.result()


# def multi_process(n, proc_name, *args):
    # with concurrent.futures.ThreadPoolExecutor(max_workers=n) as executor:
    #     # 使用列表解析来传递多个参数
    #     process_args = [(proc_name, *args) for i in range(n)]
    #     results = [executor.submit(execute_proc, *args) for args in process_args]
    #
    #     for future in concurrent.futures.as_completed(results):
    #         future.result()
    # pool = multiprocessing.Pool(processes=n)
    # args = [(proc_name, *args) for i in range(n)]
    # print(args)
    # pool.starmap(execute_proc, args)
    # pool.close()
    # pool.join()

    # processes = []
    # for i in range(n):
    #     process = multiprocessing.Pool(target=execute_proc, args=(proc_name, *args))
    #     processes.append(process)
    # for process in processes:
    #     process.start()
    # # 等待所有进程完成
    # for process in processes:
    #     process.join()