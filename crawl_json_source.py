# coding=utf-8
import multiprocessing
import pymysql
import logging
import handle_mysqldb
import handle_urls
import config

cf = config.get_conf()
db = handle_mysqldb.mysqldb()
config.set_log('crawl_json_source.log')


def crawl_json_source(json_url, id):
    try:
        page = handle_urls.handle_url(json_url, 'byclass', 'gallery-items')
        if page is not None:
            page = pymysql.escape_string(page)
            update_sql = "update json_urls set page_source='%s',status=1 where id=%s" % (page, id)
            status = db.update_mysql(update_sql)
            if status:
                logging.info('crawl json source ok')
    except Exception, e:
        logging.info(e)
        logging.info('error json_url is: ' + json_url)


if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=cf.getint('web', 'pro_num'))
    select_sql = "select id,json_url from json_urls where status=0"
    res = db.select_mysql(select_sql)
    for row in res:
        id = row[0]
        json_url = row[1]
        pool.apply_async(crawl_json_source, (json_url, id))

    logging.info("Started processes")
    pool.close()
    pool.join()
    logging.info("Subprocess done.")
