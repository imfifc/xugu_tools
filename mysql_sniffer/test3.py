from scapy.layers.l2 import Ether
from scapy.layers.inet import IP, TCP


def packet_handler(header, data):
    eth_frame = Ether(data)

    # 过滤出TCP流量
    if TCP in eth_frame:
        ip_packet = eth_frame[IP]
        tcp_packet = eth_frame[TCP]

        # 过滤出MySQL流量，默认MySQL端口为3306
        if tcp_packet.dport == 3306 or tcp_packet.sport == 3306:
            print(f"Source: {ip_packet.src}, Destination: {ip_packet.dst}, Data: {tcp_packet.payload}")


# 设置BPF过滤规则，这里示例过滤所有TCP流量
bpf_filter = "tcp"

# 选择网络接口（网卡）和捕获包的最大大小
interface = "eth0"
max_packet_size = 65536

# 开始数据包捕获，设置为无限循环
pcap = pcapy.open_live(interface, max_packet_size, 1, 0)
pcap.setfilter(bpf_filter)
pcap.loop(-1, packet_handler)
