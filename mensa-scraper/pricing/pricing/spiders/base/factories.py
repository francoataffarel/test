from abc import ABC, abstractmethod
from typing import List

from ...items import PricingItem

class MarketplaceBotMiddleware(ABC):
    @abstractmethod
    def is_bot_discovered(self, response) -> bool:
        """
        Method to check if our bots has been detected by marketplace
        :param response:
        :return: true if bot detected else false
        """
        pass


class MarketPlaceFactory(ABC):
    @abstractmethod
    def start_requests(self, start_urls: List[str]):
        pass

    def extract_list(self, response, **kwargs) -> List[str]:
        pass

    @abstractmethod
    def extract_style(self, response, **kwargs) -> PricingItem:
        pass