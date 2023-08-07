from scapy.all import sniff
from scapy.layers.inet import TCP


def packet_handler(packet):
    # 进行MySQL流量分析
    if packet.haslayer('TCP') and packet.haslayer('Raw'):
        print(packet.summary())
        # 检查TCP端口号是否是MySQL默认端口3306
        if packet[TCP].dport == 3306 or packet[TCP].sport == 3306:
            # 解析MySQL通信内容，MySQL协议的数据包格式可能较复杂，需要根据实际情况进行解析
            print(packet.summary())
            print(packet.)
            print(packet[TCP].payload)


# 设置BPF过滤规则，这里示例过滤所有流量
bpf_filter = "tcp"

# 开始数据包捕获，指定接口和BPF过滤规则
sniff(iface="ens33", filter=bpf_filter, prn=packet_handler)
