# coding=utf-8
from bs4 import BeautifulSoup
import multiprocessing
import re
import logging
import handle_mysqldb
import handle_urls

# 默认头URL
basic_url = 'https://www.archdaily.cn'

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(filename='/home/spider/logs/parse_page.log', level=logging.INFO, format=LOG_FORMAT, filemode='a')

db = handle_mysqldb.mysqldb()


def parse_page(page_url, id):
    try:
        page = handle_urls.handle_url(page_url, 'byclass', 'afd-search-list__link')
        if page is not None:
            soup = BeautifulSoup(page, 'lxml')
            links = soup.find_all('a', class_='afd-search-list__link')
            if links is not None:
                for link in links:
                    hrefurl = link['href']
                    linkurl = basic_url + hrefurl
                    # title = link.find('img')['alt']
                    # title = re.sub("[\s+\.\!\/_,$%^*(+\"\')]+|[+——()?【】“”！，。？、~@#￥%……&*（）]+", "", name)
                    insert_sql = "insert into link_urls(link_url) values('%s')" % (linkurl)
                    db.insert_mysql(insert_sql)
                    update_sql = "update page_urls set status=1 where id=%s" % (id)
                    db.update_mysql(update_sql)
    except Exception, e:
        logging.info(e)
        logging.info('links is: ' + links)
        logging.info('page_url is: ' + page_url)
        logging.info('hrefurl is: ' + hrefurl)


if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=20)
    select_sql = "select id,page_url from page_urls where status=0"
    res = db.select_mysql(select_sql)
    for row in res:
        id = row[0]
        page_url = row[1]
        pool.apply_async(parse_page, (page_url, id))

    logging.info("Started processes")
    pool.close()
    pool.join()
    logging.info("Subprocess done.")
