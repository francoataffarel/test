from abc import ABC, abstractmethod
from . import AbstractStyleList


class AbstractMarketplace(ABC):

    @abstractmethod
    def parse_style_list(self) -> AbstractStyleList:
        """
        Creator method to return a concrete implementation of AbstractStyleList
        :return: AbstractStyleList
        """
        pass

    @abstractmethod
    def start_requests(self, **kwargs):
        pass
