from ..base import AbstractStyleList, AbstractMarketplace


class NykaaFashion(AbstractMarketplace):

    def parse_style_list(self) -> AbstractStyleList:
        """
        Creator method to return a concrete implementation of AbstractStyleList
        :return: AbstractStyleList
        """
        pass

    def start_requests(self, nav_type, **kwargs):
        pass
