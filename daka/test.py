# from DrissionPage import ChromiumPage
# from DrissionPage import ChromiumOptions
# from DrissionPage.easy_set import set_headless, set_paths
#
# set_headless(True)
#
# co = ChromiumOptions()
# # co.set_paths(browser_path=r'/usr/bin/chromium')
# co.set_argument('--incognito')
# co.set_argument('--no-sandbox')
#
# page = ChromiumPage(co)
# page.get('http://www.baidu.com')
# page.get_screenshot('baidu.jpg')
import random
import time

morning_start = 14 * 60
morning_end = 14 * 60 + 58
evening_start = 15 * 60
evening_end = 15 * 60 + 60
while True:
    current_time = time.localtime(time.time())
    current_minutes = current_time.tm_hour * 60 + current_time.tm_min
    if morning_start <= current_minutes <= morning_end or evening_start <= current_minutes <= evening_end:
        time.sleep(3)
        # print(f"打卡时间：{punch_hour:02d}:{punch_minute:02d}")
        print(111)
        break

    # elif evening_start <= current_minutes <= evening_end:
    #     time.sleep(5)
    #     print(222)
    #     break

    # break
    time.sleep(60)
