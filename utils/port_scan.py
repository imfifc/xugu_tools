import time
import socket
import threading


def scan_port(ip, port, port_list, timeout=2):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    result = sock.connect_ex((ip, port))
    sock.close()
    if result == 0:
        print(f"端口 {port} 开放")
        port_list.append(port)


def scan_ports(ip, num_threads=10):
    port_list = []
    threads = []
    for port in range(1, 65536):
        thread = threading.Thread(target=scan_port, args=(ip, port, port_list))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
    print(len(port_list), port_list)


if __name__ == "__main__":
    target_ip = input("请输入目标IP地址：")
    start = time.time()
    scan_ports(target_ip)
    print(time.time() - start)
    # sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # sock.settimeout(2)
    # result = sock.connect_ex(('107.173.35.133', 8001))
    # print(result)
