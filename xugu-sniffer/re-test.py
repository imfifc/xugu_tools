import re

ss = b'?\x00\x00\x00*update  test.tb t set value=55 where id=2;\x00\x00\x00\x00\x00'

pattern = re.compile(rb'[a-zA-Z].*(.*)', re.S)
data = re.search(pattern, ss).group()
print(data)
