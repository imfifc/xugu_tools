import codecs

res = []
for high in range(0xA1, 0xF8):
    for low in range(0xA1, 0xFF):
        gbk_bytes = bytes([high, low])
        try:
            # 尝试解码为汉字
            chinese_character = gbk_bytes.decode('gbk')
            print(chinese_character, end=' ')
            res.append(chinese_character)
        except UnicodeDecodeError:
            pass
with codecs.open('gbk_characters.txt', 'a+', 'gbk') as output_file:
    output_file.write(''.join(res))
