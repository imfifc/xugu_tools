source=" 192.168.17"
#online_ip=()
for i in {1..254}; do
  ip="${source}.$i"
  if ping -c 1 -W 1 ${ip}  >/dev/null 2>&1; then
    echo "${ip}"
#    online_ip+=("$ip")

  fi
done

