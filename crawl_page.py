# coding=utf-8
from bs4 import BeautifulSoup
import multiprocessing
import logging
import handle_mysqldb
import handle_urls
import config

cf = config.get_conf()
db = handle_mysqldb.mysqldb()
config.set_log('crawl_page.log')


def crawl_page():
    try:
        page = handle_urls.handle_url(cf.get('web', 'start_url'), 'byid', 'pagination_container')
        if page is not None:
            soup = BeautifulSoup(page, 'lxml')
            last_page = soup.find('a', class_='last')
            page_num = int(last_page['href'].split('=')[1]) + 1
            for num in range(1, page_num):
                _url = 'https://www.archdaily.cn/cn/search/projects?page=' + str(num)
                logging.info(_url)
                insert_sql = "insert into page_urls(page_url) values('%s')" % (_url)
                db.insert_mysql(insert_sql)
    except Exception, e:
        logging.info(e)


if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=1)
    pool.apply_async(crawl_page)

    logging.info("Started processes")
    pool.close()
    pool.join()
    logging.info("Subprocess done.")
