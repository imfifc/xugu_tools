from scapy.all import *
from scapy.layers.inet import TCP, IP
import re
from datetime import datetime


# MYSQL_PACKET_HEADER_LEN = 4
# MYSQL_OK_PACKET = 0x00
# MYSQL_ERR_PACKET = 0xFF

# def get_now_str(is_client):
#     msg = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
#     if is_client:
#         msg += "| cli -> ser |"
#     else:
#         msg += "| ser -> cli |"
#     return msg
#
#
# def length_binary(b):
#     first = b[0]
#     if 0 < first <= 250:
#         return first, 1
#     if first == 251:
#         return 0, 1
#     if first == 252:
#         return struct.unpack('<H', b[1:3])[0], 3
#     if first == 253:
#         return struct.unpack('<I', b[1:4] + b'\x00')[0], 4
#     if first == 254:
#         return struct.unpack('<Q', b[1:9])[0], 8
#     return 0, 0
#
#
# def read_string_from_byte(b):
#     l = b.find(b'\x00')
#     if l == -1:
#         l = len(b)
#     return b[:l].decode('utf-8'), l
#
#
# def length_encoded_int(input):
#     if input[0] == 0xfb:
#         return 0, True, 1
#     num, n = length_binary(input)
#     return num, False, n
#
#
# def length_encoded_string(b):
#     num, is_null, n = length_encoded_int(b)
#     if num < 1:
#         return None, is_null, n
#
#     n += num
#     if len(b) >= n:
#         return b[n - num:n].decode('utf-8'), False, n
#     return None, False, n, io.EOF


def analyze_packet(packet):
    if packet.haslayer(TCP):
        if packet[TCP].dport == 3306 or packet[TCP].sport == 3306:
            payload = bytes(packet[TCP].payload)
            source_port = packet[TCP].sport
            dest_port = packet[TCP].dport
            ip = packet[IP].src
            # print(f"s_port:{source_port},d_port:{dest_port}")
            # payload = packet[scapy.Raw].load
            # payload = packet[scapy].load

            if dest_port == 3306 and len(payload):  # MySQL port
                print(payload)
                if payload != b'\x00\x00\x00\x00\x00\x00':
                    pattern = re.compile(rb'[\x00-\x1F]+(.*)')
                    data = re.findall(pattern, payload)
                    res = ''.join([i.decode('utf-8', errors='ignore') for i in data])
                    time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    if b'\x00\x00\x00\x02' in payload:
                        res = f'USE {res}'
                    print(f"{time}  {ip}  {res}")

            # msg = ""
            # if len(payload) == 0:
            #     return
            # packet_type = payload[0]
            # if packet_type == 0xff:
            #     error_code = struct.unpack('<H', payload[1:3])[0]
            #     error_msg, _ = read_string_from_byte(payload[4:])
            #     msg = f"{get_now_str(False)} Error code: {error_code}, Error msg: {error_msg}"
            # elif packet_type == 0x00:
            #     pos = 1
            #     affected_rows, _, _ = length_encoded_int(payload[pos:])
            #     msg = f"{get_now_str(False)} Effect Row: {affected_rows}"
            # else:
            #     return
            # print(msg)

            # if dest_port == 3306 and len(payload) > 5 and payload[4] == 0x00:  # MySQL OK packet
            #     affected_rows = int.from_bytes(payload[5:8], byteorder='little')
            #     time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            #     print(f"{time}  Affected Rows: {affected_rows}")
            # print(f"ip {ip} client: {payload.decode('utf-8',errors='ignore')}")
            # if source_port == 3306:
            #     print(f"server: {payload.decode('utf-8',errors='ignore')}")


def main(interface):
    try:
        print(f"Starting packet capture on interface: {interface}")
        sniff(iface=interface, prn=analyze_packet, filter="tcp port 3306")
    except KeyboardInterrupt:
        print("Packet capture stopped.")
        sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python mysql_traffic_analyzer.py <interface>")
        sys.exit(1)
    interface = sys.argv[1]
    main(interface)
