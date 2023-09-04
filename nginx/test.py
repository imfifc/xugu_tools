import requests

url = 'http://192.168.17.128/hh/'
r = requests.get(url)
print(r.headers)
print(r.status_code)
print(r.cookies)
