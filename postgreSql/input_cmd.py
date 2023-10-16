def func():
    ip = input("请输入ip:")
    a = input("请输入第1个参数")
    b = input("请输入第2个参数")
    if ip == '1.1.1.1':
        print('guess ok')
    else:
        print('other ip ')
        print(a, b)


func()
input('Press Enter to exit…')
