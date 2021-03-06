# coding=utf-8
from bs4 import BeautifulSoup
import multiprocessing
import logging
import handle_mysqldb
import config

cf = config.get_conf()
db = handle_mysqldb.mysqldb()
config.set_log('parse_link.log')


def parse_link(page_source, id):
    try:
        soup = BeautifulSoup(page_source, 'lxml')
        jsonurls = soup.find('a', class_='js-image-size__link lazy-anchor')
        if jsonurls is not None:
            hrefurl = jsonurls['href']
            jsonurl = cf.get('web', 'basic_url') + hrefurl
            insert_sql = "insert into json_urls(link_id,json_url) values(%s,'%s')" % (id, jsonurl)
            db.insert_mysql(insert_sql)
            update_sql = "update link_urls set status=2 where id=%s" % (id)
            db.update_mysql(update_sql)
    except Exception, e:
        logging.info(e)


if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=cf.getint('web', 'pro_num'))
    select_sql = "select id,page_source from link_urls where status=1"
    res = db.select_mysql(select_sql)
    for row in res:
        id = row[0]
        page_source = row[1]
        pool.apply_async(parse_link, (page_source, id))

    logging.info("Started processes")
    pool.close()
    pool.join()
    logging.info("Subprocess done.")
