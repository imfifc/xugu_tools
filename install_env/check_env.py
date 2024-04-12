# -*- coding:utf-8 -*-
import os
import platform
import socket
import subprocess

# import multiprocessing
import psutil


def check_hyperthreading():
    """
    awk 有的系统没有

    logicalNumber=$(grep "processor" /proc/cpuinfo|sort -u|wc -l)
    physicalNumber=$(grep "physical id" /proc/cpuinfo|sort -u|wc -l)
    coreNumber=$(grep "cpu cores" /proc/cpuinfo|uniq|awk -F':' '{print $2}'|xargs)
    HTNumber=$((logicalNumber / (physicalNumber * coreNumber)))
    """
    logical_processors = os.cpu_count()
    try:
        with open('/proc/cpuinfo', 'r') as cpuinfo_file:
            cpuinfo = cpuinfo_file.read()
            if 'physical id' in cpuinfo:
                physical_processors = cpuinfo.count('physical id')
                hyperthreading_enabled = logical_processors > physical_processors
                if hyperthreading_enabled:
                    print(f"1.系统逻辑cpu核数:{logical_processors}，已开启超线程")
            else:
                check_hyperthreading2()
    except FileNotFoundError:
        check_hyperthreading2()


def check_hyperthreading2():
    # 运行lscpu命令并捕获输出, 查找Thread(s) per core行
    # try:
    logical_processors = os.cpu_count()
    code, ret_msg = exec_command('lscpu')
    thread_line = [line for line in ret_msg.split('\n') if 'Thread(s) per core' in line or '每个核的线程数：' in line]
    # print(thread_line)
    code, ret_msg = exec_command('grep  processor /proc/cpuinfo |wc -l')
    if thread_line:
        if '每个核的线程数' in thread_line[0]:
            threads_per_core = int(thread_line[0].split('：')[1].strip())
            check_cpu_threads(logical_processors, threads_per_core)
        elif 'Thread(s) per core' in thread_line[0]:
            threads_per_core = int(thread_line[0].split(':')[1].strip())
            check_cpu_threads(logical_processors, threads_per_core)
    else:
        print("无法找到Thread(s) per core信息。")
    # except Exception as e:
    #     print(f"发生错误: {e}")


def check_cpu_threads(logical_processors, threads_per_core):
    if threads_per_core == 1:
        print(f"1.系统逻辑cpu核数:{logical_processors}, 每核的线程数:{threads_per_core}, 系统未开启超线程")
    else:
        print(f"1.系统逻辑cpu核数:{logical_processors}, 每核的线程数:{threads_per_core}, 已开启超线程。")
        help_text = """
              1、关闭 CPU 超线程（在 BIOS 查找 CPUConfiguration–>Hyper-threading 设置为 Disable）。
              2、关闭节能模式（在 BIOS 查找 SystemProfileSettings–>systemprofile 设置为CPUPerformance）。
              """
        print(help_text)


def update_sysctl_conf():
    """
    更新网络参数
    """
    sysctl_conf_path = '/etc/sysctl.conf'
    exists = ['net.core.rmem_default', 'net.core.wmem_default', 'net.core.rmem_max', 'net.core.wmem_max']
    parameters = {
        'net.core.rmem_default': '2097152',
        'net.core.wmem_default': '2097152',
        'net.core.rmem_max': '8388608',
        'net.core.wmem_max': '8388608'
    }

    if not os.path.exists(sysctl_conf_path):
        print(f"Error: {sysctl_conf_path} not found.")
        return
    with open(sysctl_conf_path, 'r') as file:
        lines = file.readlines()

    # 存在就更新，不存在就追加
    for k, v in parameters.items():
        found = False
        for i, line in enumerate(lines):
            if k in line:
                # print(line)
                lines[i] = f'{k} = {v} \n'
                found = True
                break
        if not found:
            lines.append(f'{k} = {v} \n')

    with open(sysctl_conf_path, 'w') as f:
        f.writelines(lines)

    code, msg = exec_command(f'sysctl -p {sysctl_conf_path}')
    # result = subprocess.run(["sysctl", "-p", sysctl_conf_path])
    if code == 0:
        print(f"2.{sysctl_conf_path} 网络参数更新成功")
    else:
        print(f'2.{sysctl_conf_path} 网络参数更新失败, {msg}')
    help_text = "   集群节点互ping: 万兆网<= 0.06ms，千兆网<= 0.1ms"
    print(help_text)


def exec_command(cmd):
    ret_code, ret_msg = subprocess.getstatusoutput(cmd)
    if isinstance(ret_msg, bytes):
        ret_msg = ret_msg.decode("utf-8")
    return ret_code, ret_msg

    # result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    # return result.returncode, result.stdout


def check_service(name, cmd):
    ret_code, ret_msg = exec_command(cmd)
    if ret_code != 0:
        return ret_code, ret_msg
    if name in ret_msg:
        print(f'{name} 已安装')
        return True
    else:
        print(f'{name} 未安装')
        code, ret_msg = exec_command(f'yum install -y {name}')
        if code == 0:
            print(f'{name} 开始安装')
        return False


def install_package_if_needed(name, cmd):
    if not check_service(name, f'rpm -qa | grep {name}'):
        code, ret_msg = exec_command(f'yum install -y {cmd}')
        if code == 0:
            print(f'{name} 开始安装')


def check_gcc():
    code, msg = exec_command('gcc --version')
    if 'GCC' in msg:
        print('3.3 gcc 已安装')
    else:
        print('gcc 未安装')
        code, msg = exec_command('yum install -y gcc')
        if code == 0:
            print('gcc 开始安装')
        else:
            print(msg)


def check_gdb():
    code, msg = exec_command('gdb --version')
    if 'GDB' in msg:
        print('3.3 gdb 已安装')
    else:
        print('gdb 未安装')
        code, msg = exec_command('yum remove -y gdb && yum install -y gdb')
        if code == 0:
            print('gdb 开始安装')
        else:
            print(msg)


def check_libaio():
    """异步io包"""
    code, msg = exec_command('rpm -qa | grep libaio')
    if 'libaio' in msg:
        print('3.3 libaio 已安装')
    else:
        print('libaio 未安装')
        code, msg = exec_command('yum install -y libaio libaio-devel')
        if code == 0:
            print('libaio 开始安装')
        else:
            print(msg)


def check_others():
    """
    磁盘监控：iotop（yum install -y iotop）
    性能分析工具：perf（yum install -y perf）
    系统活动情况监控：sar（yum install -y sysstat）
    """
    code, msg = exec_command('yum install -y iotop perf sysstat  iperf3')
    if code == 0:
        print('3.5 iotop perf sysstat iperf3 已安装')
    else:
        print(msg)


def check_port(host='localhost', port=5138):
    try:
        socket.create_connection((host, port), timeout=2)
        print('4. 端口5138 被占用')
    except Exception as e:
        print('4. 端口5138 未被占用')


def check_sys_kernel():
    kernel_conf_path = '/etc/profile'
    exists = ['net.core.rmem_default', 'net.core.wmem_default', 'net.core.rmem_max', 'net.core.wmem_max']
    parameters = {
        'ulimit -s': '20480',
        'ulimit -n': '10240',
    }

    if not os.path.exists(kernel_conf_path):
        print(f"Error: {kernel_conf_path} not found.")
        return
    with open(kernel_conf_path, 'r') as file:
        lines = file.readlines()

    # 存在就更新，不存在就追加
    for k, v in parameters.items():
        found = False
        for i, line in enumerate(lines):
            if k in line:
                # print(line)
                lines[i] = f'{k}  {v} \n'
                found = True
                break
        if not found:
            lines.append(f'{k}  {v} \n')

    with open(kernel_conf_path, 'w') as f:
        f.writelines(lines)

    # result = subprocess.run(["source", kernel_conf_path])
    code, ret_msg = exec_command('source /etc/profile')
    if code == 0:
        print(f'3.4 {kernel_conf_path} 内核参数更新成功')
    else:
        print(f'内核参数更新失败 {ret_msg}')


def check_firewalld():
    code, ret_msg = exec_command('systemctl stop firewalld && systemctl disable firewalld')
    if code == 0:
        print('3.6 防火墙已关闭')
    else:
        print(f'防火墙关闭失败 {ret_msg}')


def check_depend_rpm_and_service():
    """
    snmp
    ntp
    gcc、libaio、gdb
    """
    code, ret_msg = exec_command('yum install -y net-snmp && systemctl enable snmpd && systemctl start snmpd')
    if code == 0:
        print('3.1 snmp 已创建自启动，snmp 服务已启动')
    else:
        print(f'snmp 安装失败 {ret_msg}')

    code, ret_msg = exec_command('yum install -y ntpdate ntp && systemctl enable ntpd && systemctl start ntpd')
    if code == 0:
        print('3.2 ntp 已创建自启动，ntp 服务已启动')
    else:
        print(f'ntp 安装失败 {ret_msg}')


def show_sys_version():
    code, msg = exec_command('cat /etc/system-release')
    return msg


if __name__ == "__main__":
    sys_version = show_sys_version()
    arch = platform.machine()
    memory_info = psutil.virtual_memory()
    total = memory_info.total / 1024 / 1024 / 1024
    available = memory_info.available / 1024 / 1024 / 1024
    percent = memory_info.percent
    used = memory_info.used / 1024 / 1024 / 1024
    # free = memory_info.free / 1024 / 1024 / 1024
    print(f'\n当前系统为: {sys_version} 架构: {arch} \n')
    print(f'内存总量 {total :.2f}g 可用内存{available :.2f}g 使用{used:.2f}g 使用百分比{percent} \n')
    check_hyperthreading2()
    update_sysctl_conf()
    check_depend_rpm_and_service()
    check_gcc()
    check_gdb()
    check_libaio()
    check_sys_kernel()
    check_others()
    check_firewalld()
    check_port()
