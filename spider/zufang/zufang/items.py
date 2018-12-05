# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ZufangItem(scrapy.Item):
    city = scrapy.Field()
    address = scrapy.Field()
    name = scrapy.Field()
    price = scrapy.Field()
    price_num = scrapy.Field()
    area = scrapy.Field()
    area_num = scrapy.Field()
    type = scrapy.Field()
    floor = scrapy.Field()
    direction = scrapy.Field()
    insert_time = scrapy.Field()
    detail_page = scrapy.Field()
    img = scrapy.Field()
    source = scrapy.Field()
