from typing import List

from ..base import MarketPlaceFactory


class Nykaa(MarketPlaceFactory):

    def __init__(self, brand, **kwargs):
        pass

    def start_requests(self, start_urls: List[str]):
        pass

    def extract_list(self, response, **kwargs):
        pass

    def extract_style(self, response, **kwargs):
        pass