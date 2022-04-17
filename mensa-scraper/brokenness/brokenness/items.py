# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Field, Item


class BrokennessItem(Item):
    brand = Field()
    channel = Field()
    name = Field()
    skuid = Field()
    product_href = Field()
    total_skus = Field()
    available_skus = Field()
    score = Field()
    extracted_date = Field()
