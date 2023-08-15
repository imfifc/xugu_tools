from scapy.all import *
from scapy.layers.inet import TCP, IP
import re
from datetime import datetime
import argparse


def analyze_packet(packet, filename, port):
    if packet.haslayer(TCP):
        if packet[TCP].dport == port or packet[TCP].sport == port:
            payload = bytes(packet[TCP].payload)
            source_port = packet[TCP].sport
            dest_port = packet[TCP].dport
            ip = packet[IP].src
            # print(f"s_port:{source_port},d_port:{dest_port}")
            # payload = packet[scapy.Raw].load
            # payload = packet[scapy].load

            if dest_port == port and len(payload):  # MySQL port
                # print(payload)
                if payload != b'\x00\x00\x00\x00\x00\x00':
                    pattern = re.compile(rb'[\x00-\x1F]+(.*)')
                    data = re.findall(pattern, payload)
                    res = ''.join([i.decode('utf-8', errors='ignore') for i in data])
                    time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    if b'\x00\x00\x00\x02' in payload:
                        res = f'USE {res}'
                    output = f"{time}  {ip}  {res}"
                    print(output)
                    with open(filename, 'a+', newline='', encoding='utf-8') as f:
                        f.write(f'{output} \n')


def main(interface, filename, port):
    try:
        print(f"Starting packet capture on interface: {interface}")
        sniff(iface=interface, prn=lambda pkt: analyze_packet(pkt, filename, port), filter=f"tcp port {port}")
    except KeyboardInterrupt:
        print("Packet capture stopped.")
        sys.exit(0)


if __name__ == "__main__":
    program = rf"""
                               _                 _  __  __           
     _ __ ___  _   _ ___  __ _| |      ___ _ __ (_)/ _|/ _| ___ _ __ 
    | '_ ` _ \| | | / __|/ _` | |_____/ __| '_ \| | |_| |_ / _ \ '__|
    | | | | | | |_| \__ \ (_| | |_____\__ \ | | | |  _|  _|  __/ |   
    |_| |_| |_|\__, |___/\__, |_|     |___/_| |_|_|_| |_|  \___|_|   
               |___/        |_|                                          v1.0
    """
    print(program)
    parser = argparse.ArgumentParser(
        # description='这是一个数据库环境采集工具',
        prefix_chars='-'
    )
    parser.add_argument('interface',
                        help='interface 请输入网卡。如果抓包是乱码，请使用 mysql -h hostname -u username -p --ssl-mode=DISABLED')
    # if len(sys.argv) != 2:
    #     print("Usage: python mysql_traffic_analyzer.py <interface>")
    #     sys.exit(1)
    # interface = sys.argv[1]
    parser.add_argument('-P', '--port', help='Port number, mysql数据库端口', default=3306, type=int)
    parser.add_argument('-f', '--filename', help='输出文件名', default='output.txt')
    args = parser.parse_args()
    interface = args.interface
    port = args.port
    filename = args.filename
    main(interface, filename, port)
