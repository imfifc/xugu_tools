import gopacket
import io
import bytes
import errors
import log
import time
import os
from typing import Dict, List

Port = 3306
Version = "0.1"
CmdPort = "-p"


class Mysql:
    def __init__(self):
        self.port = Port
        self.version = Version
        self.source = {}

    def resolve_stream(self, net, transport, buf):
        uuid = f"{net.FastHash()}:{transport.FastHash()}"

        if uuid not in self.source:
            new_stream = {
                "packets": [],
                "stmtMap": {}
            }
            self.source[uuid] = new_stream
            self.resolve(new_stream)

        while True:
            new_packet = self.new_packet(net, transport, buf)
            if new_packet is None:
                return
            self.source[uuid]["packets"].append(new_packet)

    def bpf_filter(self):
        return f"tcp and port {self.port}"

    def version(self):
        return Version

    def set_flag(self, flg: List[str]):
        c = len(flg)
        if c == 0:
            return
        if c >> 1 == 0:
            print("ERR : Mysql Number of parameters")
            os.exit(1)
        i = 0
        while i < c:
            key = flg[i]
            val = flg[i + 1]
            if key == CmdPort:
                port = int(val)
                self.port = port
                if port < 0 or port > 65535:
                    print("ERR : port(0-65535)")
                    os.exit(1)
            else:
                print("ERR : mysql's params")
                os.exit(1)
            i += 2

    def new_packet(self, net, transport, r):
        payload = bytes.Buffer()
        seq, err = self.resolve_packet_to(r, payload)

        if err == io.EOF:
            print(f"{net} {transport} close")
            return None
        elif err is not None:
            print(f"ERR : Unknown stream {net} {transport}: {err}")
            return None

        is_client_flow = transport.Src().String() == str(self.port)

        pk = {
            "isClientFlow": is_client_flow,
            "seq": seq,
            "length": payload.Len(),
            "payload": payload.Bytes()
        }
        return pk

    def resolve_packet_to(self, r, w):
        header = [0] * 4
        n, err = io.ReadFull(r, header)
        if err is not None:
            if n == 0 and err == io.EOF:
                return 0, io.EOF
            return 0, errors.New("ERR : Unknown stream")

        length = int(header[0]) | int(header[1]) << 8 | int(header[2]) << 16
        seq = header[3]

        n, err = io.CopyN(w, r, length)
        if err is not None:
            return 0, errors.New("ERR : Unknown stream")
        elif n != length:
            return 0, errors.New("ERR : Unknown stream")
        else:
            return seq, None

    def resolve(self, stm):
        while True:
            packet = stm["packets"].pop(0)
            if packet["length"] != 0:
                if packet["isClientFlow"]:
                    self.resolve_client_packet(stm, packet["payload"], packet["seq"])
                else:
                    self.resolve_server_packet(stm, packet["payload"], packet["seq"])

    def find_stmt_packet(self, stm, seq):
        while True:
            if not stm["packets"]:
                return None
            packet = stm["packets"].pop(0)
            if packet["seq"] == seq:
                return packet
            elif time.time() - packet["timestamp"] > 5:
                return None

    def resolve_server_packet(self, stm, payload, seq):
        msg = ""
        if len(payload) == 0:
            return
        payload_type = payload[0]
        if payload_type == 0xff:
            error_code = int.from_bytes(payload[1:3], byteorder='little')
            error_msg, _ = self.read_string_from_byte(payload[4:])
            msg = f"{self.get_now_str(False)}{ErrorPacket} Err code:{error_code},Err msg:{error_msg}"
        elif payload_type == 0x00:
            pos = 1
            l, _, _ = self.length_encoded_int(payload[pos:])
            affected_rows = int(l)
            msg += f"{self.get_now_str(False)}{OkPacket} Effect Row:{affected_rows}"
        else:
            return

        print(msg)

    def resolve_client_packet(self, stm, payload, seq):
        msg = ""
        payload_type = payload[0]
        if payload_type == COM_INIT_DB:
            msg = f"USE {payload[1:]};\n"
        elif payload_type == COM_DROP_DB:
            msg = f"Drop DB {payload[1:]};\n"
        elif payload_type == COM_CREATE_DB or payload_type == COM_QUERY:
            statement = payload[1:].decode('utf-8')
            msg = f"{ComQueryRequestPacket} {statement}"
        elif payload_type == COM_STMT_PREPARE:
            server_packet = self.find_stmt_packet(stm, seq + 1)
            if server_packet is None:
                log.print("ERR : Not found stm packet")
                return

            # fetch stm id
            stmt_id = int.from_bytes(server_packet["payload"][1:5], byteorder='little')
            stmt = {
                "ID": stmt_id,
                "Query": payload[1:].decode('utf-8'),
                "FieldCount": int.from_bytes(server_packet["payload"][5:7], byteorder='little'),
                "ParamCount": int.from_bytes(server_packet["payload"][7:9], byteorder='little'),
                "Args": [None] * int.from_bytes(server_packet["payload"][7:9], byteorder='little')
            }
            stm["stmtMap"][stmt_id] = stmt
            msg = f"{PreparePacket}{stmt['Query']}"
        elif payload_type == COM_STMT_SEND_LONG_DATA:
            stmt_id = int.from_bytes(payload[1:5], byteorder='little')
            param_id = int.from_bytes(payload[5:7], byteorder='little')
            stmt = stm["stmtMap"].get(stmt_id)
            if stmt is None:
                log.print(f"ERR : Not found stm id {stmt_id}")
                return

            if stmt["Args"][param_id] is None:
                stmt["Args"][param_id] = payload[7:]
            else:
                b = stmt["Args"][param_id]
                if isinstance(b, bytes):
                    b += payload[7:]
                stmt["Args"][param_id] = b
            return
        elif payload_type == COM_STMT_RESET:
            stmt_id = int.from_bytes(payload[1:5], byteorder='little')
            stmt = stm["stmtMap"].get(stmt_id)
            if stmt is None:
                return
