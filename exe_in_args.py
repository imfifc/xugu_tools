import sys
import time


def main():
    if len(sys.argv) > 1:
        # 获取命令行参数
        arg1 = sys.argv[1]
        arg2 = sys.argv[2] if len(sys.argv) > 2 else None

        get_args(sys.argv)
        # print("参数1:", arg1)
        # print("参数2:", arg2)
    else:
        print("没有传递参数。")
    time.sleep(5)


def get_args(*args):
    for i in args:
        print(i)


if __name__ == "__main__":
    main()
