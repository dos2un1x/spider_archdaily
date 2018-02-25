# coding=utf-8
from bs4 import BeautifulSoup
import multiprocessing
import logging
import handle_mysqldb
import handle_urls

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(filename='/home/spider/logs/parse_link.log', level=logging.INFO, format=LOG_FORMAT, filemode='a')

db = handle_mysqldb.mysqldb()


def parse_link(json_url, id):
    try:
        page = handle_urls.handle_url(json_url, 'byid', 'gallery-items')
        if page is not None:
            soup = BeautifulSoup(page, 'lxml')
            # 打印soup内容，格式化输出
            # print soup.prettify()
            jsondata = soup.find('div', class_='gallery-items')['data-images']
            if jsondata is not None:
                insert_sql = "insert into json_datas(json_data) values('%s')" % (jsondata)
                db.insert_mysql(insert_sql)
                update_sql = "update json_urls set status=1 where id=%s" % (id)
                db.update_mysql(update_sql)
                logging.info('parse link json data ok !')
    except Exception, e:
        logging.info(e)
        logging.info('json_url is: ' + json_url)


if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=20)
    select_sql = "select id,json_url from json_urls where status=0"
    res = db.select_mysql(select_sql)
    for row in res:
        id = row[0]
        json_url = row[1]
        pool.apply_async(parse_link, (json_url, id))

    logging.info("Started processes")
    pool.close()
    pool.join()
    logging.info("Subprocess done.")
