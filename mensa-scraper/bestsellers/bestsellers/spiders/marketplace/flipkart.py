from typing import List

from ..base import AbstractMarketplace


class Flipkart(AbstractMarketplace):

    def __init__(self, brand, **kwargs):
        pass

    def start_requests(self, sku_ids: List[str]):
        pass

    def extract_style(self, response, **kwargs):
        pass
