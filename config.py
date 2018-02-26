# coding=utf-8
import logging
import ConfigParser


def get_conf():
    conf = ConfigParser.ConfigParser()
    conf.read('config.ini')
    return conf


def set_log(filename):
    cf = get_conf()
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(filename=cf.get('log', 'logpath') + filename, level=logging.INFO, format=log_format,
                        filemode='a')
