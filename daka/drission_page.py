from DrissionPage import ChromiumPage

page = ChromiumPage()
url = "https://login.taobao.com/member/login.jhtml?spm=a21bo.jianhua.0.0.5af92a89wOIfYH&f=top&redirectURL=http%3A%2F%2Fwww.taobao.com%2F"
page.get(url)

# 定位到账号文本框，获取文本框元素
ele = page.ele('#fm-login-id')
# 输入对文本框输入账号
ele.input('18818080228')
# 定位到密码文本框并输入密码
# page.ele('#fm-login-password').input('')
# 点击登录按钮
page.ele('@class=fm-button fm-submit password-login').click()
