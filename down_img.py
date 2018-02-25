# coding=utf-8
import multiprocessing
import logging
import handle_mysqldb
import urllib2
import time
import urllib
import random
import requests

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(filename='/home/spider/logs/down_img.log', level=logging.INFO, format=LOG_FORMAT, filemode='a')

db = handle_mysqldb.mysqldb()
path = '/home/spider/imgs/'

enable_proxy = False
order = "6fe364e27f29ff8e83f0cae046bd18c8";
apiUrl = "http://dynamic.goubanjia.com/dynamic/get/" + order + ".html";


# 动态代理IP
def AutoProxy(targetUrl):
    if enable_proxy:
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


def down_img(url_large, id, link):
    try:
        proxy_handler = urllib2.ProxyHandler({"http": AutoProxy(url_large)})
        null_proxy_handler = urllib2.ProxyHandler({})
        if enable_proxy:
            opener = urllib2.build_opener(proxy_handler)
        else:
            opener = urllib2.build_opener(null_proxy_handler)
        urllib2.install_opener(opener)
        headers = {'User-agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:22.0) Gecko/20100101 Firefox/22.0'}
        req = urllib2.Request(url=url_large, headers=headers)
        url = urllib2.urlopen(req, timeout=5)
        binary_data = url.read()
        file_name = link + '.jpg'
        temp_file = open(path + file_name, 'wb')
        temp_file.write(binary_data)
        temp_file.flush()
        temp_file.close()
        url.close()
        update_sql = "update img_urls set status=1 where id=%s" % (id)
        db.update_mysql(update_sql)
        logging.info('img download ok !')
    except Exception, e:
        logging.info(e)
        logging.info('url_large is: ' + url_large)


if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=10)
    select_sql = "select id,link,url_large from img_urls where status=0"
    res = db.select_mysql(select_sql)
    for row in res:
        id = row[0]
        link = row[1]
        url_large = row[2]
        pool.apply_async(down_img, (url_large, id, link))

    logging.info("Started processes")
    pool.close()
    pool.join()
    logging.info("Subprocess done.")
