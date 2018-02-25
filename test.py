# coding=utf-8
import multiprocessing
import logging
import handle_mysqldb
import time
import urllib

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(filename='/home/spider/logs/down_img.log', level=logging.INFO, format=LOG_FORMAT, filemode='a')

db = handle_mysqldb.mysqldb()
path = '/home/spider/imgs/'

def callbackfunc(blocknum, blocksize, totalsize):
    '''回调函数
    @blocknum: 已经下载的数据块
    @blocksize: 数据块的大小
    @totalsize: 远程文件的大小
    '''
    percent = 100.0 * blocknum * blocksize / totalsize
    if percent > 100:
        percent = 100
    print "%.2f%%"% percent

def down_img(url_large, id, link):
    try:
        file_name = link + '.jpg'
        download = urllib.urlretrieve(url_large, path + file_name, callbackfunc)
        update_sql = "update img_urls set status=1 where id=%s" % (id)
        db.update_mysql(update_sql)
        logging.info('img download ok !')
    except Exception, e:
        logging.info(e)
        logging.info('url_large is: ' + url_large)


if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=20)
    select_sql = "select id,link,url_large from img_urls where status=0"
    res = db.select_mysql(select_sql)
    for row in res:
        id = row[0]
        link = row[1]
        url_large = row[2]
        pool.apply_async(down_img, (url_large, id, link))

    logging.info("Started processes")
    pool.close()
    pool.join()
    logging.info("Subprocess done.")



# coding=utf-8
import multiprocessing
import logging
import handle_mysqldb
import urllib2
import time
import urllib

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(filename='/home/spider/logs/down_img.log', level=logging.INFO, format=LOG_FORMAT, filemode='a')

db = handle_mysqldb.mysqldb()
path = '/home/spider/imgs/'


def down_img(url_large, id, link):
    try:
        headers = {'User-agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:22.0) Gecko/20100101 Firefox/22.0'}
        req = urllib2.Request(url=url_large, headers=headers)
        url = urllib2.urlopen(req, timeout=5)
        binary_data = url.read()
        file_name = link + '.jpg'
        temp_file = open(path + file_name, 'wb')
        temp_file.write(binary_data)
        temp_file.flush()
        temp_file.close()
        url.close()
        update_sql = "update img_urls set status=1 where id=%s" % (id)
        db.update_mysql(update_sql)
        logging.info('img download ok !')
    except Exception, e:
        logging.info(e)
        logging.info('url_large is: ' + url_large)


if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=20)
    select_sql = "select id,link,url_large from img_urls where status=0"
    res = db.select_mysql(select_sql)
    for row in res:
        id = row[0]
        link = row[1]
        url_large = row[2]
        pool.apply_async(down_img, (url_large, id, link))

    logging.info("Started processes")
    pool.close()
    pool.join()
    logging.info("Subprocess done.")
