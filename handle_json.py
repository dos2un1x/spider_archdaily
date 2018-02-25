# coding=utf-8
import multiprocessing
import logging
import json
import handle_mysqldb

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(filename='/home/spider/logs/handle_json.log', level=logging.INFO, format=LOG_FORMAT, filemode='a')

db = handle_mysqldb.mysqldb()


def handle_data(json_data, id):
    try:
        js = json.loads(json_data)
        for value in js:
            link = value['link']
            url_large = value['url_large']
            url_slideshow = value['url_slideshow']
            url_medium = value['url_medium']
            vtype = value['type']
            media_provider_id = value['media_provider_id']
            caption = value['caption']
            image_alt = value['image_alt']
            insert_sql = "insert into img_urls(link,url_large,url_slideshow,url_medium,vtype,media_provider_id,caption,image_alt) values('%s','%s','%s','%s','%s','%s','%s','%s')" % (
                link, url_large, url_slideshow, url_medium, vtype, media_provider_id, caption, image_alt)
            db.insert_mysql(insert_sql)
            update_sql = "update json_datas set status=1 where id=%s" % (id)
            db.update_mysql(update_sql)
    except Exception, e:
        logging.info(e)


if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=20)
    # select_sql = "select id,title,json_data from json_datas where status=0 limit 100"
    select_sql = "select id,json_data from json_datas where status=0"
    res = db.select_mysql(select_sql)
    for row in res:
        id = row[0]
        json_data = row[1]
        pool.apply_async(handle_data, (json_data, id))

    logging.info("Started processes")
    pool.close()
    pool.join()
    logging.info("Subprocess done.")
