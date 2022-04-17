from abc import ABC, abstractmethod
from typing import List

from ...items import BrokennessItem


class MarketPlaceFactory(ABC):
    @abstractmethod
    def start_requests(self, start_urls: List[str]):
        pass

    @abstractmethod
    def extract_list(self, response, **kwargs) -> List[str]:
        pass

    @abstractmethod
    def extract_style(self, response, **kwargs) -> BrokennessItem:
        pass
