import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import psutil


# pyinstaller -c -F  --clean  --hidden-import=pandas   --hidden-import=psutil top.py

def clear_screen():
    if sys.platform.startswith('win'):
        os.system('cls')
    else:
        os.system('clear')


def get_top_cpu_process(duration=1):
    cpu_times = psutil.cpu_times_percent(interval=1, percpu=False)
    if cpu_times.iowait > 10:
        io_wait = print_red(str(cpu_times.iowait))
    else:
        io_wait = cpu_times.iowait
    # print(
    #     f"%Cpu(s): {cpu_times.user} user, {cpu_times.system} system, {cpu_times.idle} idle,{cpu_times.nice} nice, {io_wait}  I/O wait, {cpu_times.irq} irq, {cpu_times.softirq} SoftIrq, {cpu_times.steal} steal")

    mem = psutil.virtual_memory()
    total_mib = mem.total / (1024 ** 3)
    free_mib = mem.free / (1024 ** 3)
    used_mib = mem.used / (1024 ** 3)
    buff_cache_mib = (mem.buffers + mem.cached) / (1024 ** 3)

    ss = ''
    ss += f"%Cpu(s): {cpu_times.user} us, {cpu_times.system} sy, {cpu_times.nice} ni, {cpu_times.idle} id, {io_wait} io_wait, {cpu_times.irq} hi, {cpu_times.softirq} si, {cpu_times.steal} st \n"
    ss += f"Memory(GB): {total_mib:.1f} total,  {free_mib:.1f} free, {used_mib:.1f} used,  {buff_cache_mib:.1f} buff/cache \n\n"

    start_time = time.time()
    process_info = {}

    # while time.time() - start_time < duration:
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

        # time.sleep(0.1)

    # print(process_info)
    top_processs = sorted(process_info.items(), key=lambda x: x[1]['cpu'], reverse=True)[:4]
    mem_processs = sorted(process_info.items(), key=lambda x: x[1]['memory'], reverse=True)[:4]

    ss += 'cpu排序:\n'
    ss += f"{'pid':>10} {'cpu':>7} {'memory':>10} {'cmdline':>10}\n"
    for process in top_processs:
        pid = process[0]
        cpu = process[1]['cpu'] / duration
        memory = process[1]['memory'] / duration
        cmdline = process[1]['cmdline']
        ss += f"{pid:>10} {cpu:>7.2f}  {memory:>10.2f}MB  {cmdline}\n"

    ss += '内存排序:\n'
    for process in mem_processs:
        pid = process[0]
        cpu = process[1]['cpu'] / duration
        memory = process[1]['memory'] / duration
        cmdline = process[1]['cmdline']
        ss += f"{pid:>10} {cpu:>7.2f}  {memory:>10.2f}MB  {cmdline}\n"

    return ss


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
    # print('磁盘io平均值:')
    # print(str(averages))
    ss = ''
    ss += '磁盘io平均值: \n'
    ss += str(averages)
    return ss


def iostat(n=1, dev='all'):
    io_cmd = f"iostat -xmd {n} {n} "
    ret_code, ret_msg = exec_command(io_cmd)
    if ret_code == 0:
        # print(ret_msg)
        return parse_io_data(ret_msg, dev=dev)


def parse_net_data(data_lines):
    data_lines = data_lines.splitlines()
    iface_index, rp_index, tp_index, rb_index, tb_index, util_index = None, None, None, None, None, None
    for line in data_lines:
        if '平均时间' in line or 'Average' in line:
            data = line.split()
            iface_index = data.index('IFACE')
            rp_index = data.index('rxpck/s')
            tp_index = data.index('txpck/s')
            rb_index = data.index('rxkB/s')
            tb_index = data.index('txkB/s')
            util_index = data.index('%ifutil') if '%ifutil' in line else ''
            break

    max_len = 0
    for line in data_lines:
        if '平均时间' in line or 'Average' in line:
            data = line.split()
            iface = data[iface_index]
            max_len = len(iface) if len(iface) > max_len else max_len

    ss = ''
    for line in data_lines:
        if 'IFACE' in line and ('平均时间' in line or 'Average' in line):
            if util_index:
                ss += f"网络平均值: \n{'IFACE':{max_len}} {'rxpck/s':>10} {'txpck/s':>10} {'rxMB/s':>10} {'txMB/s':>10} {'%ifutil':>10}\n"
            else:
                ss += f"网络平均值: \n{'IFACE':{max_len}} {'rxpck/s':>10} {'txpck/s':>10} {'rxMB/s':>10} {'txMB/s':>10}\n"

        elif '平均时间' in line or 'Average' in line:
            data = line.split()
            iface = data[iface_index]
            rxpck = data[rp_index]
            txpck = data[tp_index]
            rxkb = float(data[rb_index]) / 1024
            txkb = float(data[tb_index]) / 1024
            if util_index:
                ss += f"{iface:{max_len}} {rxpck:>10} {txpck:>10} {rxkb:>10.2f} {txkb:>10.2f} {float(data[util_index]):>10.2f}\n"
            else:
                ss += f"{iface:{max_len}} {rxpck:>10} {txpck:>10} {rxkb:>10.2f} {txkb:>10.2f}\n"
    return ss


def get_net_data(n=1):
    net_cmd = f" sar -n DEV {n} 1"
    ret_code, ret_msg = exec_command(net_cmd)
    if ret_code == 0:
        # print(ret_msg)
        return parse_net_data(ret_msg)


def print_red(text):
    red_text = "\033[31m" + text + "\033[0m"
    return red_text


def main(task_names):
    with ThreadPoolExecutor(max_workers=len(task_names)) as executor:
        futures = executor.map(lambda func: func(), task_names)
        # 获取并处理任务的返回结果
        for future in futures:
            try:
                # for future in concurrent.futures.as_completed(futures):
                if future is not None:
                    print(future)
            # print(f"Function task returned: {result}")
            except Exception as e:
                print(f"Function task encountered an error: {e}")


if __name__ == "__main__":
    start = time.time()
    # print(get_top_cpu_process())
    tasknames = [get_top_cpu_process, iostat, get_net_data]
    while 1:
        clear_screen()
        main(tasknames)
        time.sleep(3)

    # print(time.time() - start)
