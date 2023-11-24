import sys
import os
import time
import random
import ddddocr
from pathlib import Path

from PIL import Image
from io import BytesIO
from selenium import webdriver
from chinese_calendar import is_workday
from datetime import datetime, date
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--disable-infobars")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--disable-popup-blocking")
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument("--no-sandbox ")
chrome_options.add_argument("--ignore-certificate-errors")
# chrome_options.add_argument("--headless")
# disable the banner "Chrome is being controlled by automated test software"
chrome_options.add_experimental_option("useAutomationExtension", False)
chrome_options.add_experimental_option("excludeSwitches", ['enable-automation'])

BASE_PATH = Path(__file__).parent
if sys.platform == 'win32':
    driver_path = os.path.join(BASE_PATH, r'chromedriver.exe')
else:
    driver_path = os.path.join(BASE_PATH, r'chromedriver')

user = 'zhujm'
# password = 'Wyc.2428714195'
password = '4175a0b7b165c54973d246d8adf37928_random_'


def get_screenshot(driver, element):
    if os.path.exists('screenshot.jpg'):
        os.remove('screenshot.jpg')
    # print(element.location, element.size)
    # data = driver.get_window_rect()
    # print('窗口', data)
    k = 1.75
    left = int(element.location['x']) * k
    top = int(element.location['y']) * k
    right = int(element.location['x'] + element.size['width']) * k
    bottom = int(element.location['y'] + element.size['height']) * k
    # print(left, top, right, bottom)
    driver.save_screenshot('screenshot.jpg')
    im = Image.open('screenshot.jpg')
    im = im.crop((left, top, right, bottom))
    im.save('code.png')


def get_validate_code():
    if not os.path.exists('screenshot.jpg'):
        print('code.png 不存在')
        pass
    with open('code.png', 'rb') as f:
        ocr = ddddocr.DdddOcr()
        code = ocr.classification(f.read())
    return code


def login(username, password):
    driver = webdriver.Chrome(driver_path, options=chrome_options)
    driver.implicitly_wait(10)
    driver.set_window_size(1200, 800)
    # service = Service(executable_path=driver_path)
    # driver = webdriver.Chrome(service=service, options=chrome_options)
    url = 'http://oa.xugu.com/wui/index.html#/?_key=po35wp'
    driver.get(url)
    # time.sleep(1)
    driver.find_element_by_css_selector('#loginid').send_keys(username)
    driver.find_element_by_css_selector('#userpassword').send_keys(password)
    img_ele = driver.find_element_by_css_selector('.e9login-form-vc-img img')
    # img_ele = driver.find_element(By.CSS_SELECTOR, 'div.e9login-form-vc-img img')
    # print(img_ele.get_attribute('src'))
    get_screenshot(driver, img_ele)
    code = get_validate_code()
    driver.find_element_by_name('validatecode').send_keys(code)
    driver.find_element_by_name('submit').click()
    time.sleep(1)
    try:
        driver.find_element_by_css_selector('#signPlugin')
    except Exception as e:
        print('验证码错误，重试')
        quit(driver)
        return None
    return driver


def morning_daka(driver):
    driver.find_element_by_css_selector('#signPlugin').click()
    try:
        morning_daka = driver.find_element_by_css_selector('.signBtnPanel button[name="signBtn"]')
        if morning_daka:
            print(morning_daka.text)
            morning_daka.click()
    except Exception as e:
        print('morning daka 异常 ')
    time.sleep(1)
    update_time = driver.find_element_by_css_selector('.timeLine .info:nth-child(3) .signData .signTime ')
    print(f'早上打卡时间: {update_time.text}')
    # return driver


def evening_daka(driver):
    sign_ele = driver.find_element_by_css_selector('#signPlugin')
    sign_ele.click()
    try:
        after_daka = driver.find_element_by_css_selector('.info.info-last button[name="signBtn"]')
        if after_daka:
            print(after_daka.text)
            after_daka.click()
    except Exception as e:
        print('evening daka 异常')
    sign_ele.click()
    # ele = driver.find_element_by_css_selector('.info-last .resign')
    # driver.find_element_by_css_selector('.info-last .content div:nth-child(2)').click()
    driver.find_element_by_css_selector('.info-last .content div:nth-child(2) .resign').click()
    time.sleep(1)
    update_time = driver.find_element_by_css_selector('.info.info-last .signTime.text-elli')
    print(f'晚上打卡更新时间: {update_time.text}')
    # return driver


def punch_clock(driver):
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
            # print(f"打卡时间：{punch_hour:02d}:{punch_minute:02d}")
            morning_daka(driver)
        elif evening_start <= current_minutes <= evening_end:
            punch_time = current_minutes + random_minutes
            punch_hour, punch_minute = divmod(punch_time, 60)
            # print(f"打卡时间：{punch_hour:02d}:{punch_minute:02d}")
            evening_daka(driver)
        break
        # time.sleep(60)


def quit(driver):
    driver.close()
    driver.quit()


if __name__ == '__main__':
    while True:
        start = time.time()
        driver = login(user, password)
        if driver:
            punch_clock(driver)
            # morning_daka(driver)
            # evening_daka(driver)
            quit(driver)
            print(time.time() - start)
            break
