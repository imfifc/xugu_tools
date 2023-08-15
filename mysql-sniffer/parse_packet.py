import struct
import time
import io


def get_now_str(is_client):
    msg = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    if is_client:
        msg += "| cli -> ser |"
    else:
        msg += "| ser -> cli |"
    return msg


def read_string_from_byte(b):
    l = b.find(b'\x00')
    if l == -1:
        l = len(b)
    return b[:l].decode('utf-8'), l


def length_binary(b):
    first = b[0]
    if 0 < first <= 250:
        return first, 1
    if first == 251:
        return 0, 1
    if first == 252:
        return struct.unpack('<H', b[1:3])[0], 3
    if first == 253:
        return struct.unpack('<I', b[1:4] + b'\x00')[0], 4
    if first == 254:
        return struct.unpack('<Q', b[1:9])[0], 8
    return 0, 0


def length_encoded_int(input):
    if input[0] == 0xfb:
        return 0, True, 1
    num, n = length_binary(input)
    return num, False, n


def length_encoded_string(b):
    num, is_null, n = length_encoded_int(b)
    if num < 1:
        return None, is_null, n

    n += num
    if len(b) >= n:
        return b[n - num:n].decode('utf-8'), False, n
    return None, False, n, io.EOF


def resolve_server_packet(payload, seq):
    msg = ""
    if len(payload) == 0:
        return
    packet_type = payload[0]
    if packet_type == 0xff:
        error_code = struct.unpack('<H', payload[1:3])[0]
        error_msg, _ = read_string_from_byte(payload[4:])
        msg = f"{get_now_str(False)} Error code: {error_code}, Error msg: {error_msg}"
    elif packet_type == 0x00:
        pos = 1
        affected_rows, _, _ = length_encoded_int(payload[pos:])
        msg = f"{get_now_str(False)} Effect Row: {affected_rows}"
    else:
        return
    print(msg)


# Example usage
payload = b'\xff\x00\x00\x00\x00\x00\x00\x00'
seq = 1
resolve_server_packet(payload, seq)
