# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals
from enum import Enum, unique

LOG_NAME = "Eurostat"
PACKAGE_KEYWORD = "EUROSTAT"
DELIM = "/"
NUM_THREADS = 10

ACCOUNT_NAME = "ACCOUNT_NAME"
ACCOUNT_KEY = "ACCOUNT_KEY"

# Transformer parameters
ATTR_PREFIX = "__eurostat_"
PARAM_CONNECTION = ATTR_PREFIX + "connection"
PARAM_CREDENTIAL_SOURCE = "CREDENTIAL_SOURCE"
PARAM_PROJECT_ID = "_FME_PROJECT_ID"
PARAM_OPERATION_TYPE = "OPERATION_TYPE"
PARAM_BUCKET = "_FME_BUCKET"
PARAM_LIST_PATH = "_FME_FILES"
PARAM_INCLUDE_SUBFOLDERS = "_FME_INCLUDE_SUBFOLDERS"
PARAM_ATTRIBUTES_TO_ADD = "_FME_ATTRIBUTES_TO_ADD"
PARAM_TARGET_FOLDER = "_FME_TARGET_FOLDER"
PARAM_TARGET_ATTRIBUTE = "_FME_TARGET_ATTRIBUTE"
PARAM_HANDLE_EXISTING_FILE = "_FME_HANDLE_EXISTING_FILE"
PARAM_ENCODING_TYPE = "_FME_ENCODING_TYPE"
PARAM_CUSTOM_HEADERS = "_FME_CUSTOM_HEADERS"
PARAM_METADATA_GROUP = "_FME_METADATA_GROUP"
PARAM_CONTENTS_ONLY = "_FME_CONTENTS_ONLY"

OUTPUT_DOWNLOAD_PATH = "_download_path"

SOURCE_WEBCONN = "WEBCONN"
SOURCE_ANON = "ANONYMOUS"
SOURCE_ENVVAR = "ENVVAR"

PARAM_ACL = "_FME_GCS_ACL"
ACL_PRIVATE = "private"
ACL_PUBLIC_READ = "public-read"

# Messages
BAD_CLIENT_9900051 = 9900051
CANT_DOWNLOAD_9900052 = 9900052
CANT_UPLOAD_9900053 = 9900053
CANT_LIST_BUCKET_ANON_9900054 = 9900054
UNAUTHORISED_9900055 = 9900055
GOOGLE_CLOUD_GENERAL_9900059 = 9900059
BAD_DEFAULT_CLIENT_9900058 = 9900058
ERROR_LISTING_PROJECTS_9900060 = 9900060
FORBIDDEN_LISTING_PROJECTS_9900061 = 9900061
MUST_USE_LITERAL_9900062 = 9900062
UNSUPPORTED_CONNECTION_TYPE_9900063 = 9900063
ERROR_LISTING_BUCKETS_9900064 = 9900064
GOOGLE_CLOUD_STORAGE_ERROR = "GOOGLE_CLOUD_STORAGE_ERROR"


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
