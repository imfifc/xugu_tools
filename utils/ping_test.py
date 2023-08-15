import threading
import time

from ping3 import ping, verbose_ping


def ping_host(ip, temp_list):
    response_time = ping(ip)
    if response_time:
        print(f"{ip} is reachable (Response Time: {response_time} ms)")
        temp_list.append(ip)


def one_bit_ip(base_ip):
    """
    :param base_ip: '10.28.15.'
    :return:
    """
    threads = []
    temp_list = []
    for i in range(1, 255):
        ip = base_ip + str(i)
        thread = threading.Thread(target=ping_host, args=(ip, temp_list))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()

    print(len(temp_list), temp_list)


def two_bit_ip(base_ip):
    """
    :param base_ip: "10.28."
    :return:
    """
    threads = []
    temp_list = []
    for i in range(1, 255):
        ip = base_ip + str(i)
        for j in range(1, 255):
            host = f'{ip}.{j}'
            thread = threading.Thread(target=ping_host, args=(host, temp_list))
            threads.append(thread)
            thread.start()
    for thread in threads:
        thread.join()

    print(len(temp_list), temp_list)


if __name__ == "__main__":
    ip1 = '10.28.15.'
    ip2 = "10.28."
    start = time.time()
    # one_bit_ip(ip1)
    two_bit_ip(ip2)
    print(time.time() - start)
