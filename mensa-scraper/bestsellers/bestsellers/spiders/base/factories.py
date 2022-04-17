from abc import ABC, abstractmethod
from typing import List

from ...items import BestsellersItem


class AbstractMarketplace(ABC):
    @abstractmethod
    def start_requests(self, sku_ids: List[str]):
        pass

    @abstractmethod
    def extract_style(self, response, **kwargs) -> BestsellersItem:
        pass


class MarketplaceBotMiddleware(ABC):
    @abstractmethod
    def is_bot_discovered(self, response) -> bool:
        """
        Method to check if our bots has been detected by marketplace
        :param response:
        :return: true if bot detected else false
        """
        pass
