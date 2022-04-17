# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Field

class ServiceabilityItem(scrapy.Item):
    product_href = Field()
    product_name = Field()
    pincode = Field()
    city = Field()
    region = Field()
    skuId = Field()
    extracted_date = Field()
    delivery_date = Field()
    days_for_delivery = Field()
    brand = Field()
    competitor_of_brands = Field()
    marketplace = Field()
