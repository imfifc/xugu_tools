# from DrissionPage import ChromiumPage
# from DrissionPage import ChromiumOptions
# from DrissionPage.easy_set import set_headless, set_paths
#
# set_headless(False)

# co = ChromiumOptions()
# # co.set_paths(browser_path=r'/usr/bin/google-chrome')
# co.set_argument('--incognito')
# co.set_argument('--no-sandbox')
# co.set_argument('--window-size', '1500,1200')
# page = ChromiumPage(co)
# data = page.get('http://www.baidu.com')
# print(data)
# page.get_screenshot('screenshot.jpg', full_page=True)


# import base64
# import time
# import os
# import sys
# import os
# import time
# import random
# import requests
# import ddddocr
# from pathlib import Path
from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC

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
# BASE_PATH = Path(__file__).parent
# if sys.platform == 'win32':
#     driver_path = os.path.join(BASE_PATH, r'chromedriver.exe')
# else:
#     driver_path = os.path.join(BASE_PATH, r'chromedriver')

# service = Service(executable_path=driver_path)
driver = webdriver.Chrome(options=chrome_options)
url = 'http://www.baidu.com'
driver.get(url)
driver.save_screenshot('baidu.jpg')
# time.sleep(2)
# driver.find_element(By.CSS_SELECTOR, '#loginid').send_keys('hhh')
# time.sleep(10)
