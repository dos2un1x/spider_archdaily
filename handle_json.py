# coding=utf-8
import multiprocessing
import logging
import json
import pymysql
import handle_mysqldb
import config

cf = config.get_conf()
db = handle_mysqldb.mysqldb()
config.set_log('handle_json.log')


def handle_data(json_data, id):
    try:
        js = json.loads(json_data)
        for value in js:
            link = value['link']
            url_large = pymysql.escape_string(value['url_large'])
            url_slideshow = pymysql.escape_string(value['url_slideshow'])
            url_medium = pymysql.escape_string(value['url_medium'])
            vtype = value['type']
            media_provider_id = value['media_provider_id']
            caption = pymysql.escape_string(value['caption'])
            image_alt = pymysql.escape_string(value['image_alt'])
            insert_sql = "insert into img_urls(data_id,link,url_large,url_slideshow,url_medium,vtype,media_provider_id,caption,image_alt) values(%s,'%s','%s','%s','%s','%s','%s','%s','%s')" % (
                id, link, url_large, url_slideshow, url_medium, vtype, media_provider_id, caption, image_alt)
            db.insert_mysql(insert_sql)
        update_sql = "update json_datas set status=1 where id=%s" % (id)
        status = db.update_mysql(update_sql)
        if status:
            logging.info('handle json ok' + str(id))
    except Exception, e:
        logging.info(e)
        logging.info('handle json error' + str(id))


if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=cf.getint('web', 'pro_num'))
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
