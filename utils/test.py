import requests

proxy = {'http': 'http://localhost:10809'}

r = requests.post('http://httpbin.org/post', proxies=proxy)
print(r.status_code)
print(r.headers)
print(r.content)
