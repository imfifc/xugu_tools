# import subprocess
# import time
#
#
# def monitor_top(interval, duration):
#     command = "top -b -n {} -d {} -c ".format(duration // interval, interval)
#     process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
#
#     for line in iter(process.stdout.readline, b''):
#         print(line.decode())
#
#     process.stdout.close()
#     process.wait()
#
#
# def exec_command(cmd):
#     result = subprocess.run(cmd, check=True, shell=True, capture_output=True, text=True)
#     print(result.stdout)
#
#
# if __name__ == "__main__":
#     interval = 1  # 采样间隔时间（秒）
#     duration = 1  # 监控持续时间（秒）
#
#     # monitor_top(interval, duration)
#
#     # exec_command('top -o %CPU -b -n 1 -d 1 -c')
#     exec_command('top -c -b -n 1 -d 1')
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import psutil


def get_top_cpu_process(duration=2):
    start_time = time.time()
    process_info = {}

    while time.time() - start_time < duration:
        for process in psutil.process_iter(['pid', 'name', 'cpu_percent']):
            try:
                pid = process.info['pid']
                cpu_percent = process.info['cpu_percent']
                cmdline = process.cmdline()
                memory_info = process.memory_info()
                if cmdline:
                    if pid in process_info:
                        process_info[pid]['cpu'] += cpu_percent
                        process_info[pid]['memory'] += memory_info.rss / 1024 / 1024
                    else:
                        # process_info[pid] = {'cpu_percent': cpu_percent,
                        process_info[pid] = {
                            'name': psutil.Process(pid).name(),
                            'cpu': cpu_percent,
                            'memory': memory_info.rss / 1024 / 1024,
                            'cmdline': ' '.join(cmdline),
                        }

            except psutil.ZombieProcess:
                continue
            except psutil.AccessDenied:
                continue

        time.sleep(1)

    # print(process_info)
    top_processs = sorted(process_info.items(), key=lambda x: x[1]['cpu'], reverse=True)[:4]
    mem_processs = sorted(process_info.items(), key=lambda x: x[1]['memory'], reverse=True)[:4]

    print('cpu排序:')
    for process in top_processs:
        pid = process[0]
        cpu = process[1]['cpu'] / duration
        memory = process[1]['memory'] / duration
        cmdline = process[1]['cmdline']
        print(f"pid:{pid:10}, cpu:{cpu:7.2f}, memory:{memory:10.2f}MB, cmd:{cmdline}")

    print('内存排序:')
    for process in mem_processs:
        pid = process[0]
        cpu = process[1]['cpu'] / duration
        memory = process[1]['memory'] / duration
        cmdline = process[1]['cmdline']
        print(f"pid:{pid:10}, cpu:{cpu:7.2f}, memory:{memory:10.2f}MB, cmd:{cmdline}")


def exec_command(cmd):
    ret_code, ret_msg = subprocess.getstatusoutput(cmd)
    if isinstance(ret_msg, bytes):
        ret_msg = ret_msg.decode("utf-8")
    return ret_code, ret_msg


def parse_io_data(strings, dev='all'):
    rs, ws, rmb, wmb, r_await, w_await, all_await, util = None, None, None, None, None, None, None, None
    for line in strings.splitlines():
        if line and 'Device' in line:
            data = line.split()
            rs = data.index('r/s')
            ws = data.index('w/s')
            rmb = data.index('rMB/s')
            wmb = data.index('wMB/s')
            r_await = data.index('r_await')
            w_await = data.index('w_await')
            if 'await' in data:
                all_await = data.index('await')
            elif 'd_await' in data:
                all_await = data.index('d_await')
            util = data.index('%util')
            break

    p_data = []
    for line in strings.splitlines()[2:]:
        if line and 'Device' not in line:
            data = line.split()
            each = [data[0], float(data[rs]), float(data[ws]), float(data[rmb]), float(data[wmb]), float(data[r_await]),
                    float(data[w_await]), float(data[all_await]), float(data[util])]
            p_data.append(each)

    df = pd.DataFrame(p_data, columns=['Device', 'r/s', 'w/s', 'rMB/s', 'wMB/s', 'r_await', 'w_await', 'd_await',
                                       '%util'])
    averages = df.groupby('Device').mean().round(2)
    print(averages)


def iostat(n=2, dev='all'):
    io_cmd = f"iostat -xmd {n} {n} "
    ret_code, ret_msg = exec_command(io_cmd)
    if ret_code == 0:
        print('磁盘io平均值:')
        # print(ret_msg)
        parse_io_data(ret_msg, dev=dev)


def parse_net_data(ss):
    iface, rp, tp, rb, tb, ifutil = None, None, None, None, None, None
    for line in ss.splitlines():
        if line and '平均时间' in line or 'Average' in line:
            data = line.split()
            iface = data.index('IFACE')
            rp = data.index('rxpck/s')
            tp = data.index('txpck/s')
            rb = data.index('rxkB/s')
            tb = data.index('txkB/s')
            util = data.index('%ifutil')
            break
    for line in ss.splitlines()[2:]:
        if 'IFACE' in line and ('平均时间' in line or 'Average' in line):
            print(f"{data[1]:13} {data[2]:>10} {data[3]:>10} {'rxMB/s':>10} {'rxMB/s':>10} {data[-1]:>10}")
        if line and 'IFACE' not in line and ('平均时间' in line or 'Average' in line):
            data = line.split()
            print(
                f"{data[1]:13} {data[2]:>10} {data[3]:>10} {float(data[4]) / 1024:>10.2f} {float(data[5]) / 1024:>10.2f} {float(data[-1]):>10.2f}")


def get_net_data(n=1):
    net_cmd = f" sar -n DEV {n} 2"
    ret_code, ret_msg = exec_command(net_cmd)
    if ret_code == 0:
        print('网络:')
        # print(ret_msg)
        parse_net_data(ret_msg)


def main(task_names):
    # print(len(task_names))
    with ThreadPoolExecutor(max_workers=len(task_names)) as executor:
        futures = executor.map(lambda func: func(), task_names)
        # 获取并处理任务的返回结果
        for future in futures:
            try:
                # for future in concurrent.futures.as_completed(futures):
                if future is not None:
                    future.result()
            # print(f"Function task returned: {result}")
            except Exception as e:
                print(f"Function task encountered an error: {e}")


if __name__ == "__main__":
    start = time.time()
    # get_top_cpu_process()
    # iostat(dev='all', n=2)
    # get_net_data(1)
    tasknames = [get_top_cpu_process, iostat, get_net_data]
    main(tasknames)
    print(time.time() - start)
