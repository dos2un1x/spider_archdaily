# coding=utf-8
import pymysql
import logging
from DBUtils.PooledDB import PooledDB

dbhost = '192.168.1.12'
dbport = 3306
dbuser = 'spider'
dbpasswd = 'dos2unix'
dbname = 'spiderdb'
dbcharset = 'utf8'


class mysqldb:
    def connect_mysql(self):
        try:
            dbpool = PooledDB(pymysql, host=dbhost, port=dbport, user=dbuser, passwd=dbpasswd, db=dbname,
                              charset=dbcharset, mincached=0, maxcached=0, maxshared=0, maxconnections=300,
                              blocking=False, maxusage=False)
            return dbpool.connection()
        except Exception, e:
            logging.info(e)
            logging.info('mysql connect error !')

    def insert_mysql(self, _sql):
        try:
            conn = self.connect_mysql()
            cur = conn.cursor()
            cur.execute(_sql)
            conn.commit()
        except Exception, e:
            logging.info(e)
            logging.info(_sql)
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def update_mysql(self, _sql):
        try:
            conn = self.connect_mysql()
            cur = conn.cursor()
            cur.execute(_sql)
            conn.commit()
        except Exception, e:
            logging.info(e)
            logging.info(_sql)
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    def select_mysql(self, _sql):
        try:
            conn = self.connect_mysql()
            cur = conn.cursor()
            cur.execute(_sql)
            res = cur.fetchall()
            return res
        except Exception, e:
            logging.info(e)
            logging.info(_sql)
        finally:
            cur.close()
            conn.close()
