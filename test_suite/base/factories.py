from abc import ABC, abstractmethod
from typing import List


class Validator(ABC):
    @abstractmethod
    def validate_schema(self, **kwargs):
        """
        Method to validate the json
        """
