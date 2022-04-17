from enum import Enum


class ChannelType(str, Enum):
    AmazonIn = 'Amazon IN'
    AmazonCom = 'Amazon COM'
    Flipkart = 'Flipkart'
    Myntra = 'Myntra'
    Nykaa = 'Nykaa'
    NykaaFashion = 'Nykaa Fashion'
    Ajio = 'Ajio'
