from enum import Enum, unique

LOG_NAME = "Eurostat"
PACKAGE_KEYWORD = "EUROSTAT"
DELIM = "/"

@unique
class Agency(Enum):
    ESTAT = 'Eurostat', 'https://ec.europa.eu/eurostat/api/dissemination'
    COMP  = 'DG COMP' , 'https://webgate.ec.europa.eu/comp/redisstat/api/dissemination'
    EMPL  = 'DG EMPL' , 'https://webgate.ec.europa.eu/empl/redisstat/api/dissemination'
    GROW  = 'DG GROW' , 'https://webgate.ec.europa.eu/grow/redisstat/api/dissemination'
    def __init__(self, label, base_uri):
        self.label = label
        self.base_uri = base_uri
    @property
    def category_schemes_url(self):
        return f'{self.base_uri}/sdmx/2.1/categoryscheme/{self.name}/all'
    @property
    def categorisations_url(self):
        return f'{self.base_uri}/sdmx/2.1/categorisation/{self.name}/all'
    @property
    def dataflows_url(self):
        return f'{self.base_uri}/sdmx/2.1/dataflow/{self.name}/all?detail=allstubs'
