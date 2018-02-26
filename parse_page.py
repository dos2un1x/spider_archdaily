# coding=utf-8
from bs4 import BeautifulSoup
import multiprocessing
import logging
import handle_mysqldb
import config

cf = config.get_conf()
db = handle_mysqldb.mysqldb()
config.set_log('parse_page.log')


def parse_page(page_source, id):
    try:
        soup = BeautifulSoup(page_source, 'lxml')
        links = soup.find_all('a', class_='afd-search-list__link')
        if links is not None:
            for link in links:
                hrefurl = link['href']
                linkurl = cf.get('web', 'basic_url') + hrefurl
                insert_sql = "insert into link_urls(page_id,link_url) values(%s,'%s')" % (id, linkurl)
                db.insert_mysql(insert_sql)
            update_sql = "update page_urls set status=2 where id=%s" % (id)
            db.update_mysql(update_sql)
    except Exception, e:
        logging.info(e)


if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=cf.getint('web','pro_num'))
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
