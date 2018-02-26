# coding=utf-8
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
import urllib
import random
import requests
import time
import logging
import config

order = "6fe364e27f29ff8e83f0cae046bd18c8";
apiUrl = "http://dynamic.goubanjia.com/dynamic/get/" + order + ".html";

cf = config.get_conf()


# 动态代理IP
def AutoProxy(targetUrl):
    while True:
        try:
            # 获取IP列表
            res = urllib.urlopen(apiUrl).read().strip("\n");
            # 按照\n分割获取到的IP
            ips = res.split("\n");
            # 随机选择一个IP
            proxyip = random.choice(ips)
            proxies = {'http': proxyip}
            # 使用代理IP请求目标网址
            if requests.get(targetUrl, proxies=proxies, timeout=2).status_code == 200:
                logging.info('connect proxy ip is: ' + proxyip)
                logging.info('ok targetUrl is: ' + targetUrl)
                return proxyip
                break
        except Exception, e:
            logging.info('timeout proxy ip is: ' + proxyip)
            logging.info('error targetUrl is: ' + targetUrl)
            logging.info(e)
            pass
        time.sleep(1)


# selenium webdriver chrome 爬取页面url
def handle_url(_url, _choose, _value):
    # 设置浏览器headless及不加载图片
    chrome_options = Options()
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-javascript')
    chrome_options.add_argument('--disable-plugins')
    chrome_options.add_argument('--disable-extensions')
    # chrome_options.add_extension('/home/spider/AdBlock_v3.24.0.crx')
    prefs = {'profile.default_content_setting_values': {'images': 2}}
    chrome_options.add_experimental_option('prefs', prefs)
    chrome_options.add_argument('--headless')
    # linux下配置
    chrome_options.add_argument('--no-sandbox')
    # 是否动态代理IP
    # chrome_options.add_argument('--proxy-server=http://' + AutoProxy(url))
    driver = webdriver.Chrome(chrome_options=chrome_options)
    # 隐式等待（可和显式一同使用，取大），推荐使用显式
    if cf.getboolean('web', 'time_out'):
        # driver.implicitly_wait(15)
        driver.set_script_timeout(cf.getint('web', 'script_time'))
        driver.set_page_load_timeout(cf.getint('web', 'page_load'))
    try:
        driver.get(_url)
    except Exception, e:
        response = driver.page_source
        pass
    finally:
        driver.quit()
    # 显式等待（0.5秒查询一次，查询5秒，共查询10次）
    # EC.presence_of_all_elements_located（复数形式，查到所有的通过）
    # EC.presence_of_element_located（单数）
    try:
        if _choose == 'byid':
            WebDriverWait(driver, cf.getint('web', 'driver_wait'), 0.5).until(
                EC.presence_of_element_located((By.ID, _value)))
        elif _choose == 'byclass':
            WebDriverWait(driver, cf.getint('web', 'driver_wait'), 0.5).until(
                EC.presence_of_element_located((By.CLASS_NAME, _value)))
        else:
            logging.info('please choose crawl conditions')
        return response
    except TimeoutException:
        logging.info('WebDriverWait exception url is: ' + _url)

