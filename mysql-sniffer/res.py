import re
import psutil

from datetime import datetime

data = b'cvv\x15\x00\x00\x00\x03select * from person'

pattern = re.compile(rb'[\x00-\x1F]+(.*)')
# pattern = r'^.*?([^\x00-\x1F]+)$'

res = re.findall(pattern, data)
print(res)

tt = datetime.now()
print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

with open('output.txt', 'a+', newline='\n', encoding='utf-8') as f:
    f.write('111')
    f.write('222')

if __name__ == "__main__":
    network_interfaces = psutil.net_if_addrs()
    for interface_name, interface_addresses in network_interfaces.items():
        print(f"Interface: {interface_name}")
        for addr in interface_addresses:
            print(f"  {addr.family.name}: {addr.address}")
