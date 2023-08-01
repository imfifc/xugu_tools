# import multiprocessing
# import time
#
#
# # 中间函数
# def multiplication(num):
#     return num + 1
#
#
# # 设置回调函数
# def setcallback(x):
#     with open('hh.txt', 'a+') as f:
#         line = str(x) + "\n"
#         f.write(line)
#
#
# if __name__ == '__main__':
#     start = time.time()
#     pool = multiprocessing.Pool(8)
#
#     for i in range(10000):
#         pool.apply_async(func=multiplication, args=(i,), callback=setcallback)
#     pool.close()
#     pool.join()
#     print(time.time() - start)


import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor

import time


def do(file_name, i, lock):
    with lock:
        with open(file_name, 'a+') as f:
            f.writelines(["i=" + str(i) + ":" + str(x) + "\n" for x in range(3)])


def main():
    pool = ProcessPoolExecutor(max_workers=8)
    m = multiprocessing.Manager()
    lock = m.Lock()
    futures = [pool.submit(do, "hh2.txt", num, lock) for num in range(10000)]
    for future in futures:
        future.result()


if __name__ == '__main__':
    start = time.time()
    main()  # 62.3888418674469
    print(time.time() - start)
