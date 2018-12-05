# -*- coding: utf-8 -*-
import scrapy
import re
import time
from zufang.items import ZufangItem


class LianjiaSpider(scrapy.Spider):
    name = 'lianjia'
    city_name = ['zz', 'bj', 'hz', 'sh', 'sz', 'gz']
    allowed_domains = ['%s.lianjia.com/zufang/' % i for i in city_name]
    start_urls = ['https://zz.lianjia.com/zufang/']
    page = 1
    url = 'https://{}.lianjia.com/zufang/pg{}/'
    sign = False
    page_data = 1
    city_num = 0

    def parse(self, response):
        if not self.sign:
            data = response.xpath('//div[@class="page-box house-lst-page-box"]/@page-data')[0].extract()
            # print('*'*50)
            c = re.compile(r'\d+')
            s = c.search(data)
            self.page_data = int(s.group())
            self.sign = True
            print(self.page_data)
        else:
            alldiv_list = response.xpath('//div[@class="list-wrap"]/ul[@id="house-lst"]/li')
            for odiv in alldiv_list:
                details = odiv.xpath('.//div[@class="info-panel"]/h2/a/@href').extract()[0]
                # print(details)
                yield scrapy.Request(url=details, callback=self.detail_page, dont_filter=True)

        if self.page <= self.page_data:
            self.page += 1
            url = self.url.format(self.city_name[self.city_num], self.page)
            yield scrapy.Request(url=url, callback=self.parse, dont_filter=True)
        else:
            # print(self.page_data, '***')
            self.city_num += 1
            self.sign = False
            self.page = 1
            # try:
            #     url = self.url.format(self.city_name[self.city_num], self.page)
            # except Exception as e:
            #     print(e)
            # with open('url.txt', 'w') as f:
            #     f.write(url + '\n')
            url = self.url.format(self.city_name[self.city_num], self.page)
            yield scrapy.Request(url=url, callback=self.parse, dont_filter=True)

    def detail_page(self, response):
        city = response.xpath('//div[@class="fl l-txt"]/a[2]/text()').extract()[0][:-2]
        address_temp = response.xpath('//div[@class="zf-room"]/p[7]/a/text()').extract()
        address = ''.join(address_temp)
        name = response.xpath('//div[@class="zf-room"]/p[6]/a/text()').extract()[0]
        price_temp = response.xpath('//div[@class="price "]//span[1]/text()').extract()
        price = ''.join(price_temp)
        price_num = price_temp[0]
        area = response.xpath('//div[@class="zf-room"]/p[1]/text()').extract()[0]
        area_num = area[:-2]
        type = response.xpath('//div[@class="zf-room"]/p[2]/text()').extract()[0].replace(' ', '')
        floor = response.xpath('//div[@class="zf-room"]/p[3]/text()').extract()[0]
        direction = response.xpath('//div[@class="zf-room"]/p[4]/text()').extract()[0]
        date_temp = response.xpath('//div[@class="zf-room"]/p[8]/text()').extract()[0]
        insert_time = self.detail_date(date_temp)
        detail_page = response.url
        try:
            img = response.xpath('//div[@class="thumbnail"]/ul/li[1]/@data-src').extract()[0]
        except Exception as e:
            img = 'none'
        source = 'lianjia'
        item = ZufangItem()
        for field in item.fields.keys():
            item[field] = eval(field)
        yield item

    def detail_date(self, date_temp):
        c = re.compile(r'\d+')
        date = c.search(date_temp).group()
        t = int(time.time())
        data_middle = time.localtime(t - (int(date) * 3600 * 24))
        date_finish = '-'.join('%s' % a for a in [data_middle.tm_year, data_middle.tm_mon, data_middle.tm_mday])
        return date_finish
