from scapy.all import *
from scapy.layers.inet import TCP, IP
import re
from datetime import datetime
import argparse


def parse_bytes(string):
    pattern1 = re.compile(rb'\x01\x00\x00\x00+(.*?)\x01\x00\x00\x00', re.S)
    data = re.search(pattern1, string)
    if data:
        pattern2 = re.compile(
            rb'\x00\xfe\xff\xff\xff\xff\xff\xff\xff\xfe\xff\xff\xff\xff\xff\xff\xff\x00(.*?)\x01\x00\x00\x00', re.S)
        pattern3 = re.compile(rb'\x01\x00\x00\x00\x00\x00(.*?)\x01\x00\x00\x00', re.S)
        patterns = [pattern2, pattern3]
        for pattern in patterns:
            new_datas = pattern.findall(data.group())
            for i in new_datas:
                # print(i)
                res = i.decode('utf-8', errors='ignore')
                # print(res)
                extracted_chars = ''.join([char for char in res if char.isprintable()])
                # print(extracted_chars)
                # 去掉开头的特殊字符，re.MULTILINE 表示匹配每一行的开头
                cleaned_text = re.sub(r"^(?:'|%|\*|;)+", "", extracted_chars, flags=re.MULTILINE)
                # print(cleaned_text)
                return cleaned_text
    return None


def analyze_packet(packet, filename, port):
    if packet.haslayer(TCP) and packet.haslayer(Raw):
        if packet[TCP].dport == port or packet[TCP].sport == port:
            payload = bytes(packet[TCP].payload)
            source_port = packet[TCP].sport
            dest_port = packet[TCP].dport
            ip = packet[IP].src
            # print(f"s_port:{source_port},d_port:{dest_port}")
            # payload = packet[scapy.Raw].load
            # payload = packet[scapy].load

            if dest_port == port and len(payload):
                print(payload)
                res = parse_bytes(payload)
                if res:
                    time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    output = f"{time}  {ip}  {res}"
                    print(output)

                # if payload != b'\x00\x00\x00\x00\x00\x00':
                #     pattern = re.compile(rb'[\x00-\x1F]+(.*)')
                #     data = re.findall(pattern, payload)
                #     res = ''.join([i.decode('utf-8', errors='ignore') for i in data])


def main(interface, filename, port):
    try:
        print(f"Starting packet capture on interface: {interface}")
        packets = sniff(iface=interface, prn=lambda pkt: analyze_packet(pkt, filename, port), filter=f"tcp port {port}")
        # for i in packets:
        #     i.show()
    except KeyboardInterrupt:
        print("Packet capture stopped.")
        sys.exit(0)


if __name__ == "__main__":
    program = rf"""
                          _                      _  __  __           
      ___  _ __ __ _  ___| | ___       ___ _ __ (_)/ _|/ _| ___ _ __ 
     / _ \| '__/ _` |/ __| |/ _ \_____/ __| '_ \| | |_| |_ / _ \ '__|
    | (_) | | | (_| | (__| |  __/_____\__ \ | | | |  _|  _|  __/ |   
     \___/|_|  \__,_|\___|_|\___|     |___/_| |_|_|_| |_|  \___|_|   
                                                                       v1.0
    """
    print(program)
    parser = argparse.ArgumentParser(
        # description='这是一个数据库环境采集工具',
        prefix_chars='-'
    )
    parser.add_argument('interface',
                        help='interface 请输入网卡。如果抓包是乱码，请关闭ssl')
    # if len(sys.argv) != 2:
    #     print("Usage: python mysql_traffic_analyzer.py <interface>")
    #     sys.exit(1)
    # interface = sys.argv[1]
    parser.add_argument('-P', '--port', help='Port number, oracle数据库端口', default=1521, type=int)
    parser.add_argument('-f', '--filename', help='输出文件名', default='output.txt')
    args = parser.parse_args()
    interface = args.interface
    port = args.port
    filename = args.filename
    main(interface, filename, port)

# yum -y install libpcap-devel
