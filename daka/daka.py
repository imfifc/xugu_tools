import os
import time
import random
# import requests
import ddddocr
from DrissionPage import ChromiumPage
from DrissionPage.configs.chromium_options import ChromiumOptions
from DrissionPage.easy_set import set_headless
from chinese_calendar import is_workday
from datetime import datetime, date

user = 'zhujm'
# password = 'Wyc.2428714195'
password = '4175a0b7b165c54973d246d8adf37928_random_'

# Pillow==9.5.0
def ocr_validate(link):
    r = requests.get(link)
    ocr = ddddocr.DdddOcr()
    code = ocr.classification(r.content)
    return code



def get_screenshot(page, element):
    if os.path.exists('screenshot.jpg'):
        os.remove('screenshot.jpg')
    left = int(element.location[0])
    top = int(element.location[1])
    right = int(element.location[0] + element.size[0])
    bottom = int(element.location[1] + element.size[1])
    print(left, top, right, bottom)
    # page.get_screenshot('screenshot.jpg')

    page.get_screenshot('screenshot.jpg', left_top=(left, top), right_bottom=(right, bottom))


def get_validate_code():
    with open('screenshot.jpg', 'rb') as f:
        ocr = ddddocr.DdddOcr(show_ad=False)
        code = ocr.classification(f.read())
    return code


def login(user, password):
    set_headless(True)
    co = ChromiumOptions()
    co.set_argument('--incognito')
    co.set_argument('--no-sandbox')
    # co.set_argument('--window-size', '1500,1200')
    page = ChromiumPage(co)

    # page.set_headless(False)
    url = 'http://oa.xugu.com/wui/index.html#/?_key=po35wp'
    page.get(url)
    page.get_screenshot('xugu.jpg')
    ele = page.ele('#loginid')
    ele.input(user)
    page.ele('#userpassword').input(password)
    # link = page.ele('.e9login-form-vc-img').ele('tag:img').link
    # print(link)
    img_ele = page.ele('.e9login-form-vc-img').ele('tag:img')
    get_screenshot(page, img_ele)
    code = get_validate_code()
    page.ele('#validatecode').input(code)
    page.ele('#submit').click()  # 登录
    return page


def morning_daka(page):
    page.ele('#signPlugin').click()
    morning_daka = page.ele('.signBtnPanel').ele('@name=signBtn')
    if morning_daka:
        print(morning_daka.text)
        morning_daka.click()
    update_time = page.ele('.info info-last').ele('.signTime text-elli')
    print(f'更新时间: {update_time.text}')


def evening_daka(page):
    page.ele('#signPlugin').click()
    try:
        after_daka = page.ele('.info info-last').ele('@name=signBtn')
        if after_daka:
            print(after_daka.text)
            after_daka.click()
            update_time = page.ele('.info info-last').ele('.signTime text-elli')
            print(f'更新时间: {update_time.text}')
    except Exception as e:
        print(e)
    page.ele('.info info-last').ele('.resign').click()
    update_time = page.ele('.info info-last').ele('.signTime text-elli')
    print(f'更新时间: {update_time.text}')




def punch_clock(page):
    today = datetime.strptime("2023-10-06", '%Y-%m-%d').date()
    today = date.today()

    morning_start = 8 * 60 + 10
    morning_end = 8 * 60 + 60
    evening_start = 16 * 60
    evening_end = 16 * 60 + 60
    while is_workday(today):
        current_time = time.localtime(time.time())
        current_minutes = current_time.tm_hour * 60 + current_time.tm_min
        random_minutes = random.randint(0, 20)  # 在20分钟内随机选择打卡时间
        if morning_start <= current_minutes <= morning_end:
            punch_time = current_minutes + random_minutes
            punch_hour, punch_minute = divmod(punch_time, 60)
            print(f"打卡时间：{punch_hour:02d}:{punch_minute:02d}")
            morning_daka(page)
        elif evening_start <= current_minutes <= evening_end:
            punch_time = current_minutes + random_minutes
            punch_hour, punch_minute = divmod(punch_time, 60)
            print(f"打卡时间：{punch_hour:02d}:{punch_minute:02d}")
            evening_daka(page)

        time.sleep(60)



# print(is_workday(today))
# print(is_workday(data2))
# print(is_workday(data3))


if __name__ == '__main__':
    while True:
        page = login(user, password)
        time.sleep(1)
        if page.ele('验证码错误'):
            print(111, '验证码错误')
            page.quit()
            continue

        page.ele('#signPlugin').click()
        element = page.ele('.timeLine').ele('.time text-elli')
        print(element.text)
        evening_daka(page)
        page.quit()
        break

    page.quit()

time.sleep(1)