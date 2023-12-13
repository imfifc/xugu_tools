import sys
import os
import time
import random
import ddddocr
from pathlib import Path
import schedule
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
chrome_options.add_argument("--headless")
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
password = '84EqgwegxJ#@'


def get_screenshot(driver, element):
    if os.path.exists('screenshot.jpg'):
        os.remove('screenshot.jpg')
    # print(element.location, element.size)
    # data = driver.get_window_rect()
    # print('窗口', data)
    k = 1
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
        ocr = ddddocr.DdddOcr(show_ad=False)
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
    print(f'{datetime.now()} login success')
    return driver


def morning_daka(driver):
    try:
        driver.find_element_by_css_selector('.ant-modal-content .ant-modal-footer button:nth-child(1)').click()
    except Exception as e:
        print('今天已打卡')  # 获取打卡时间
    time.sleep(1)
    driver.find_element_by_css_selector('#signPlugin').click()
    update_times = driver.find_elements_by_css_selector('.timeLine .signData .signTime ')
    for update_time in update_times:
        print(f'早上打卡时间: {update_time.text}')
    # try:
    #     morning_daka = driver.find_element_by_css_selector('.signBtnPanel button[name="signBtn"]')
    #     if morning_daka:
    #         print(morning_daka.text)
    #         morning_daka.click()
    # except Exception as e:
    #     print('morning daka 异常 ')
    # time.sleep(1)
    # driver.save_screenshot('moroning.jpg')
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
    # driver.save_screenshot('evening.jpg')
    driver.find_element_by_css_selector('.info-last .content div:last-child').click()
    driver.find_element_by_css_selector('.info-last .content  .resign').click()
    time.sleep(1)
    update_time = driver.find_element_by_css_selector('.info.info-last .signTime.text-elli')
    print(f'晚上打卡更新时间: {update_time.text}')
    # return driver


def punch_clock():
    today = datetime.strptime("2023-10-06", '%Y-%m-%d').date()
    today = date.today()

    morning_start = 8 * 60
    morning_end = 8 * 60 + 58
    evening_start = 15 * 60
    evening_end = 18 * 60 + 60
    while is_workday(today):
        current_time = time.localtime(time.time())
        current_minutes = current_time.tm_hour * 60 + current_time.tm_min
        # punch_time = current_minutes + random_minutes
        if morning_start <= current_minutes <= morning_end:
            random_minutes = random.randint(0, 5)
            print(f'随机时间{random_minutes * 60}秒')
            time.sleep(random_minutes * 60)
            # punch_hour, punch_minute = divmod(punch_time, 60)
            # print(f"打卡时间：{punch_hour:02d}:{punch_minute:02d}")
            driver = login(user, password)
            if driver:
                morning_daka(driver)
                quit(driver)
                break
            if not driver:
                continue

        elif evening_start <= current_minutes <= evening_end:
            random_minutes = random.randint(0, 5)
            print(f'随机时间{random_minutes * 60}秒')
            time.sleep(random_minutes * 60)
            # punch_hour, punch_minute = divmod(punch_time, 60)
            # print(f"打卡时间：{punch_hour:02d}:{punch_minute:02d}")
            driver = login(user, password)
            if driver:
                evening_daka(driver)
                quit(driver)
                break
            if not driver:
                continue

        break
        # time.sleep(60)


def quit(driver):
    driver.close()
    driver.quit()


if __name__ == '__main__':
    def my_job():
        while True:
            start = time.time()
            punch_clock()
            print(time.time() - start)
            break


    # my_job()
    # schedule.every().day.at("08:10").do(my_job)
    schedule.every().day.at("08:16").do(my_job)
    # schedule.every().day.at("15:10").do(my_job)
    schedule.every().day.at("18:00").do(my_job)
    schedule.every().day.at("18:20").do(my_job)
    schedule.every().day.at("18:30").do(my_job)

    while True:
        # 检查是否有要运行的任务
        schedule.run_pending()
        # 休眠1秒，避免过多占用系统资源
        time.sleep(1)

"""
ddddocr==1.4.8
Pillow==9.5.0
selenium==3.9.0
"""

"""
docker run --rm  -it  -w /test --name chrome   -v /data:/test chrome3.8 python selnium2.py
# 下载chromedriver 119版本
https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/119.0.6045.105/linux64/chromedriver-linux64.zip
"""

"""
# 总数重启
docker run   -itd  -w /test --restart always  --name chrome    chrome3.81 
 # 无缓存  
docker build --no-cache  -t chrome3.81 . 
docker stop chrome && docker rm chrome


# Dockerfile
FROM chrome3.8
WORKDIR /test
RUN pip install  schedule -i https://pypi.tuna.tsinghua.edu.cn/simple
COPY . .

CMD ["python","selnium2.py"]
"""
