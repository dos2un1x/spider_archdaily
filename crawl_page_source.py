# coding=utf-8
import multiprocessing
import pymysql
import logging
import handle_mysqldb
import handle_urls
import config

cf = config.get_conf()
db = handle_mysqldb.mysqldb()
config.set_log('crawl_page_source.log')


def crawl_page_source(page_url, id):
    try:
        page = handle_urls.handle_url(page_url, 'byid', 'pagination_container')
        if page is not None:
            page = pymysql.escape_string(page)
            update_sql = "update page_urls set page_source='%s',status=1 where id=%s" % (page, id)
            status = db.update_mysql(update_sql)
            if status:
                logging.info('crawl page source ok')
    except Exception, e:
        logging.info(e)
        logging.info('error page_url is: ' + page_url)


if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=cf.getint('web','pro_num'))
    select_sql = "select id,page_url from page_urls where status=0"
    res = db.select_mysql(select_sql)
    for row in res:
        id = row[0]
        page_url = row[1]
        pool.apply_async(crawl_page_source, (page_url, id))

    logging.info("Started processes")
    pool.close()
    pool.join()
    logging.info("Subprocess done.")
