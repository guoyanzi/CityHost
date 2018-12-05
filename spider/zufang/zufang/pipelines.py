# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import pymysql
import pymongo
import redis
import hashlib
import sqlite3


class ZufangPipeline(object):
    def open_spider(self, spider):
        self.fp = open('lianjia.txt', 'w', encoding='utf8')

    def close_spider(self, spider):
        self.fp.close()

    def process_item(self, item, spider):
        dic = dict(item)
        string = json.dumps(dic, ensure_ascii=False)
        self.fp.write(string + '\n')
        return item


class MysqlZufangPipeline(object):
    def open_spider(self, spider):
        # decode_responses=True 写入的键值对中的value为str类型，不加这个参数写入的则为字节类型
        self.redis_con = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)
        self.db = pymysql.Connect(host='localhost', port=3306, user='root', password='123456', database='zufang',
                                  charset='utf8')

    def close_spider(self, spider):
        self.db.close()

    def save_to_mysql(self, item):
        cursor = self.db.cursor()
        sql = 'insert into lianjia(address,name,price,area,type,floor,direction,insert_time,detail_page,img) values("%s","%s","%s","%s","%s","%s","%s","%s","%s","%s")' % (
            item['address'], item['name'], item['price'], item['area'], item['type'], item['floor'], item['direction'],
            item['insert_time'], item['detail_page'], item['img'],)
        try:
            cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            print(e)
            self.db.rollback()

    def process_item(self, item, spider):
        dic_key_temp = dict(item.copy())
        dic_key = dic_key_temp["detail_page"]
        dic_value = dict(item.copy())
        dic_md5_key = hashlib.md5(str(dic_key).encode(encoding='utf8')).hexdigest()
        # dic_md5_key = dic_key
        dic_md5_value = hashlib.md5(str(dic_value).encode(encoding='utf8')).hexdigest()
        # dic_md5_value = dic_value
        if self.redis_con.get(dic_md5_key) == dic_md5_value:
            print("该条数据已存在")
        else:
            self.redis_con.set(dic_md5_key, dic_md5_value)
            self.save_to_mysql(item)
        return item

class SqliteZufangPipeline(object):
    def open_spider(self, spider):
        # decode_responses=True 写入的键值对中的value为str类型，不加这个参数写入的则为字节类型
        self.redis_con = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)
        self.db = sqlite3.connect('../home.db')


    def close_spider(self, spider):
        self.db.close()

    def save_to_sqllite(self, item):
        cursor = self.db.cursor()
        sql = 'insert into lianjia(city,address,name,price,price_num,area,area_num,type,floor,direction,insert_time,detail_page,img,source) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        parms = (item['city'],item['address'], item['name'], item['price'], item['price_num'], item['area'], item['area_num'], item['type'], item['floor'], item['direction'],
            item['insert_time'], item['detail_page'], item['img'], item['source'])
        try:
            cursor.execute(sql, parms)
            self.db.commit()
        except Exception as e:
            print(e)
            self.db.rollback()

    def process_item(self, item, spider):
        dic_key_temp = dict(item.copy())
        dic_key = dic_key_temp["detail_page"]
        dic_value = dict(item.copy())
        dic_md5_key = hashlib.md5(str(dic_key).encode(encoding='utf8')).hexdigest()
        # dic_md5_key = dic_key
        dic_md5_value = hashlib.md5(str(dic_value).encode(encoding='utf8')).hexdigest()
        # dic_md5_value = dic_value
        if self.redis_con.get(dic_md5_key) == dic_md5_value:
            print("该条数据已存在")
        else:
            self.redis_con.set(dic_md5_key, dic_md5_value)
            self.save_to_sqllite(item)
        return item


class MongodbZufangPipeline(object):
    # i = 0
    def open_spider(self, spider):
        self.client = pymongo.MongoClient(host='localhost', port=27017)
        # decode_responses=True 写入的键值对中的value为str类型，不加这个参数写入的则为字节类型
        self.redis_con = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)

    def close_spider(self, spider):
        # print(self.i)
        self.client.close()

    def process_item(self, item, spider):
        dic_key_temp = dict(item.copy())
        dic_key = dic_key_temp["detail_page"]
        city_name = dic_key_temp["city"]
        dic_value = dict(item.copy())
        dic_md5_key = hashlib.md5(str(dic_key).encode(encoding='utf8')).hexdigest()
        # dic_md5_key = dic_key
        dic_md5_value = hashlib.md5(str(dic_value).encode(encoding='utf8')).hexdigest()
        # dic_md5_value = dic_value
        if self.redis_con.get(dic_md5_key) == dic_md5_value:
            # self.i += 1
            print("这条数据已重复")
        else:
            self.redis_con.set(dic_md5_key, dic_md5_value)
            db = self.client.lianjia
            col = db[city_name]
            dic = dict(item)
            col.insert(dic)
        return item
