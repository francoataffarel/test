from abc import ABC, abstractmethod
from typing import List
from ...items import DiscoverabilityStyleItem


class AbstractStyleList(ABC):

    @abstractmethod
    def selectors(self, **kwargs) -> List[str]:
        """
        Method to define a container level xpath selectors
        :param kwargs:
        :return: Return list of xpath selectors
        """
        pass

    @abstractmethod
    def clean(self, selection, **kwargs) -> List[DiscoverabilityStyleItem]:
        """
        Method to apply a container level xpath selector to extract a list of StyleReviewItem object
        :param selection:
        :param kwargs:
        :return: StyleReviewItem
        """
        pass
