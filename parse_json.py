# coding=utf-8
from bs4 import BeautifulSoup
import multiprocessing
import pymysql
import logging
import handle_mysqldb
import config

cf = config.get_conf()
db = handle_mysqldb.mysqldb()
config.set_log('parse_json.log')


def parse_json(page_source, id):
    try:
        soup = BeautifulSoup(page_source, 'lxml')
        jsondata = soup.find('div', class_='gallery-items')['data-images']
        if jsondata is not None:
            jsondata = pymysql.escape_string(jsondata)
            insert_sql = "insert into json_datas(json_id,json_data) values(%s,'%s')" % (id, jsondata)
            db.insert_mysql(insert_sql)
            update_sql = "update json_urls set status=2 where id=%s" % (id)
            db.update_mysql(update_sql)
    except Exception, e:
        logging.info(e)


if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=cf.getint('web', 'pro_num'))
    select_sql = "select id,page_source from json_urls where status=1"
    res = db.select_mysql(select_sql)
    for row in res:
        id = row[0]
        page_source = row[1]
        pool.apply_async(parse_json, (page_source, id))

    logging.info("Started processes")
    pool.close()
    pool.join()
    logging.info("Subprocess done.")
