from time import sleep
from appium import webdriver

# 连接移动设备必须的参数
desired_caps = {}
k = 1   
# 当前要测试的设备名称
desired_caps["devicename"] = "127.0.0.1:62001"
# 要启动的APP的包名
desired_caps["appPackage"] = "com.tencent.mm"   #设置的包名
# 要启动APP哪个页面
desired_caps["appActivity"] = "com.tencent.mm.plugin.account.ui.WelcomeActivity"    #App初始化类
# 操作系统
desired_caps["platformName"] = "Android"
# android系统的版本
desired_caps["platformVersion"] = "7.1.2"

driver = webdriver.Remote(command_executor="http://127.0.0.1:62001/wd/hub", desired_capabilities=desired_caps)

sleep(3)

driver.close_app()
driver.quit()
