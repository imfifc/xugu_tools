import signal
import sys
import time
import socket
import nids
from mysql_dissector import mysql_dissector
from config import config_init, config_is_daemon, config_get_pcapfile, config_get_device, config_get_filter, \
    config_is_log_split, config_set_log_split, config_is_server_port, config_get_tcp_stream_cnt, config_fini
from util import log_runtime_error


def nids_syslog_hook(type, errnum, iph, data):
    return


def tcp_callback(tcp, no_need_param):
    ret = 0
    no_need_param[0] = None

    if tcp.nids_state == nids.NIDS_JUST_EST:
        tcp.client.collect = 1
        tcp.server.collect = 1
        add_mysql_session(tcp.addr)
    elif tcp.nids_state in (nids.NIDS_CLOSE, nids.NIDS_RESET, nids.NIDS_TIMED_OUT):
        del_mysql_session(tcp.addr)
    elif tcp.nids_state == nids.NIDS_DATA:
        ret = mysql_dissector(tcp, no_need_param)
        if ret == SESSION_DEL:
            del_mysql_session(tcp.addr)
    # TODO: Handle NIDS_RESUME state if ENABLE_TCPREASM is defined


def tcp_resume_is_client(packet_tcphdr, packet_iphdr, is_client):
    # TODO: Implement this function to determine if the packet is from client or server
    pass


def sig_exit_handler(signum, frame):
    if is_shutdown[0] == 0:
        print("program is going to shutdown!")
    is_shutdown[0] = 1


def sniffer_is_shutdown():
    return is_shutdown[0] == 1


def main():
    config_init(sys.argv)

    if config_is_daemon():
        # Daemonize the process
        # TODO: Implement daemonization
        pass

    signal.signal(signal.SIGINT, sig_exit_handler)
    signal.signal(signal.SIGTERM, sig_exit_handler)

    nids_params = nids.get_param_handle()
    filename = config_get_pcapfile()
    if len(filename) != 0:
        nids_params.filename = filename
        nids_params.device = None
    else:
        nids_params.device = config_get_device()
        nids_params.pcap_filter = config_get_filter()

    nids_params.pcap_timeout = 5
    nids_params.n_tcp_streams = config_get_tcp_stream_cnt()
    nids_params.scan_num_hosts = 0
    nids_params.syslog = nids_syslog_hook

    if not nids.nids_init():
        print(nids.nids_errbuf)
        sys.exit(1)

    nids_chksum_ctl = nids.nids_chksum_ctl()
    nids_chksum_ctl.netaddr = 0
    nids_chksum_ctl.mask = 0
    nids_chksum_ctl.action = nids.NIDS_DONT_CHKSUM
    nids_chksum_ctl.reserved = 0
    nids.nids_register_chksum_ctl(nids_chksum_ctl, 1)

    # TODO: Initialize session, log, and other components

    nids.nids_register_tcp(tcp_callback)
    # TODO: Register TCP resume callback if ENABLE_TCPREASM is defined

    is_shutdown[0] = 0
    while not sniffer_is_shutdown():
        ret = nids.nids_dispatch(-1)
        if nids_params.device is None and ret == 0:
            # We have read all content of the file
            break

    config_fini()
    # TODO: Finalize session, log, and other components
    nids.nids_exit()


if __name__ == "__main__":
    main()
