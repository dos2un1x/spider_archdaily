# coding=utf-8
from bs4 import BeautifulSoup
import multiprocessing
import logging
import handle_mysqldb

# 默认头URL
basic_url = 'https://www.archdaily.cn'

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(filename='/home/spider/logs/parse_page.log', level=logging.INFO, format=LOG_FORMAT, filemode='a')

db = handle_mysqldb.mysqldb()

def parse_page(page_source, id):
    try:
        soup = BeautifulSoup(page_source, 'lxml')
        links = soup.find_all('a', class_='afd-search-list__link')
        if links is not None:
            for link in links:
                hrefurl = link['href']
                linkurl = basic_url + hrefurl
                insert_sql = "insert into link_urls(page_id,link_url) values(%s,'%s')" % (id,linkurl)
                db.insert_mysql(insert_sql)
            update_sql = "update page_urls set status=2 where id=%s" % (id)
            db.update_mysql(update_sql)
    except Exception, e:
        logging.info(e)


if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=4)
    select_sql = "select id,page_source from page_urls where status=1"
    res = db.select_mysql(select_sql)
    for row in res:
        id = row[0]
        page_source = row[1]
        pool.apply_async(parse_page, (page_source, id))

    logging.info("Started processes")
    pool.close()
    pool.join()
    logging.info("Subprocess done.")
