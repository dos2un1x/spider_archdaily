# coding=utf-8
from bs4 import BeautifulSoup
import multiprocessing
import logging
import handle_mysqldb
import handle_urls

# 默认头URL
basic_url = 'https://www.archdaily.cn'

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(filename='/home/spider/logs/crawl_link.log', level=logging.INFO, format=LOG_FORMAT, filemode='a')

db = handle_mysqldb.mysqldb()


def crawl_link(link_url, id):
    try:
        page = handle_urls.handle_url(link_url, 'byid', 'single-content')
        if page is not None:
            soup = BeautifulSoup(page, 'lxml')
            # 打印soup内容，格式化输出
            # print soup.prettify()
            jsonurls = soup.find('a', class_='js-image-size__link lazy-anchor')
            if jsonurls is not None:
                hrefurl = jsonurls['href']
                jsonurl = basic_url + hrefurl
                logging.info('json url is: ' + jsonurl)
                insert_sql = "insert into json_urls(json_url) values('%s')" % (jsonurl)
                db.insert_mysql(insert_sql)
            update_sql = "update link_urls set status=1 where id=%s" % (id)
            db.update_mysql(update_sql)
    except Exception, e:
        logging.info(e)
        logging.info('jsonurls is: ' + jsonurls)
        logging.info('link_url is: ' + link_url)
        logging.info('hrefurl is: ' + hrefurl)


if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=20)
    select_sql = "select id,link_url from link_urls where status=0"
    res = db.select_mysql(select_sql)
    for row in res:
        id = row[0]
        link_url = row[1]
        pool.apply_async(crawl_link, (link_url, id))

    logging.info("Started processes")
    pool.close()
    pool.join()
    logging.info("Subprocess done.")
