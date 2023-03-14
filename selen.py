from selenium import webdriver
from selenium.webdriver.common.by import By
from urllib.parse import urlparse

driver = webdriver.Chrome()

driver.get("https://cn.bing.com/")
search_box = driver.find_element(By.NAME, "q")
search_box.send_keys("您的姓名")
search_box.submit()

# 等待搜索结果加载完毕
driver.implicitly_wait(10)

# 获取第二页搜索结果的链接
next_page = driver.find_element(By.XPATH, '//a[@title="下一页"]')
next_page_link = next_page.get_attribute("href")
driver.get(next_page_link)

# 统计每个顶级域名出现次数
domain_count = {}

# 获取搜索结果
results = driver.find_elements(By.XPATH, '//li[@class="b_algo"]')

for result in results:
    # 获取标题和链接
    title = result.find_element(By.TAG_NAME, "h2").text
    link = result.find_element(By.TAG_NAME, "a").get_attribute("href")
    print(title)
    print(link)

    # 统计顶级域名出现次数
    domain = urlparse(link).hostname.split('.')[-2]
    domain_count[domain] = domain_count.get(domain, 0) + 1

# 将结果保存至文本文档中
with open("search_results.txt", "w") as f:
    for result in results:
        title = result.find_element(By.TAG_NAME, "h2").text
        link = result.find_element(By.TAG_NAME, "a").get_attribute("href")
        f.write(f"{title}\n{link}\n\n")

    f.write("\n\nTop Level Domain Count:\n")
    for domain, count in domain_count.items():
        f.write(f"{domain}: {count}\n")

driver.quit()
