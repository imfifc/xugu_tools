import socket
from scapy.all import sniff, UDP
from pybpfcc import BPF

# MySQL default port
MYSQL_PORT = 3306

# BPF filter to capture MySQL packets (TCP and UDP)
BPF_FILTER = "port {} or port {} and udp".format(MYSQL_PORT, MYSQL_PORT)

# BPF code to capture MySQL payload
BPF_CODE = """
#include <linux/skbuff.h>
#include <linux/ip.h>
#include <linux/tcp.h>

BPF_TABLE("percpu_hash", u64, long, pkt_counts, 256);

int capture_mysql(struct __sk_buff *skb) {
    u64 pkt_size = skb->len;
    pkt_counts.increment(skb->len);

    struct iphdr *ip = bpf_hdr_pointer(skb, 0);
    struct tcphdr *tcp = (struct tcphdr *)(ip + 1);

    int offset = sizeof(struct ethhdr) + ip->ihl * 4 + sizeof(struct tcphdr);
    u_char *payload = (u_char *)(skb->data) + offset;

    u_int16_t dport = ntohs(tcp->dest);
    if (dport == %d) { // MySQL port
        // Parse MySQL payload here
        // Example: print MySQL payload hex dump
        bpf_trace_printk(payload, pkt_size - offset);
    }

    return 0;
}
""" % MYSQL_PORT


def print_mysql_packets(pkt):
    pkt_count = pkt_counts[pkt[UDP].len].value
    print(f"Received MySQL packet (size={pkt[UDP].len}, count={pkt_count}):")
    print(pkt.sprintf("Payload: %Raw.load%"))
    print("=" * 50)


# Load BPF program
bpf = BPF(text=BPF_CODE)

# Attach the BPF filter to the network interface
bpf.attach_raw_socket("lo")  # Replace "lo" with the desired network interface name

# Retrieve the packet count table
pkt_counts = bpf.get_table("pkt_counts")

# Start packet capture and processing
print("MySQL traffic capture started.")
sniff(filter=BPF_FILTER, prn=print_mysql_packets)
