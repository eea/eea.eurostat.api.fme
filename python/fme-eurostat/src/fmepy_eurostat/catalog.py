from dataclasses import dataclass
from typing import List
from fmeobjects import FMESession, FMEFeature, FMEFactoryPipeline
from .constants import (LOG_NAME, Agency, PACKAGE_KEYWORD)
from ._vendor.webserviceconnector.fmewebfs import (
    FMEWebFilesystemDriver,
    ContainerContentResponse,
    ContainerItem,
    FMEWebFilesystem,
    IFMEWebFilesystem
)


import os.path
XFMAP = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'xfmap', 'data_discovery.xmp')

@dataclass
class Item:
    name: str
    id: str
    key: str


@dataclass
class Container(Item):
    children: List[any]

session = FMESession()

from fmegeneral.fmelog import get_configured_logger
logger = get_configured_logger('CATALOG')

XFMAP_ENCODED = session.encodeToFMEParsableText(XFMAP)

def read_catalog(datasets, tree=dict(), items=dict(), item_key_func=lambda dataflow_id: dataflow_id):
    """
    Read categorization xml files into tree structure
    """
    pipeline_directives = []
    pipeline = FMEFactoryPipeline('read xml', pipeline_directives)
    factory_lines = [
        ['FACTORY_DEF', '*', 'QueryFactory'
            ,'FACTORY_NAME', 'FeatureReader'
            ,'INPUT', 'FEATURE_TYPE', '*'
            ,'FCTQUERY_INTERACTION', 'NONE'
            ,'COMBINE_ATTRIBUTES', 'RESULT_ONLY'
            ,'COMBINE_GEOMETRY', 'RESULT_ONLY'
            ,'QUERYFCT_TABLE_SEPARATOR', 'SPACE'
            ,'ENABLE_CACHE', 'YES'
            ,'CACHE_TIMEOUT_HRS', '6'
            ,'READER_TYPE', 'XML'
            ,'READER_DATASET', '@Value(dataset)'
            ,'READER_DIRECTIVES', 'METAFILE,XML'
            ,'QUERYFCT_OUTPUT', 'DATA_ONLY'
            ,'CONTINUE_ON_READER_ERROR', 'YES'
            ,'QUERYFCT_SET_FME_FEATURE_TYPE', 'YES'
            ,'READER_PARAMS_WWJD', f'FILE_TYPE,xfMap,XFMAP,{XFMAP_ENCODED}'
            ,'TREAT_READER_PARAM_AMPERSANDS_AS_LITERALS', 'YES'
            ,'OUTPUT', 'RESULT', 'FEATURE_TYPE', 'FeatureReader_OK'
            ,'OUTPUT', 'READER_ERROR', 'FEATURE_TYPE', 'FeatureReader_ERROR'
        ]
    ]


    for factory_def in factory_lines:
        pipeline.addFactory(factory_def)
    logger.info('Factory pipeline initialized')

    for dataset in datasets:
        feature = FMEFeature()
        feature.setAttribute('dataset', dataset)
        logger.info('Reading dataset `%s`', dataset)
        pipeline.processFeature(feature)
    pipeline.allDone()
    tree = dict()
    items = dict()
    logger.info('Emptying factory pipeline')
    while True:
        feature = pipeline.getOutputFeature()
        if feature is None:
            break
        if 'FeatureReader_ERROR' == feature.getFeatureType():
            raise Exception(feature.getAttribute('_reader_error'))
        
        fme_feature_type = feature.getAttribute('fme_feature_type')
        if 'Category' == fme_feature_type:
            if 'T' == feature.getAttribute('CategoryScheme.dissemination_perspective_id'):
                continue
            category_scheme_id = feature.getAttribute('CategoryScheme.id')
            id = feature.getAttribute('Category.id')
            urn = feature.getAttribute('Category.urn')
            name = feature.getAttribute('Category.name')
            if not urn.endswith(f'.{id}'):
                logger.warn('Skipping Category %s with unexpected urn `%s`', id, urn)

            container =  tree.get(urn, Container(name, id, urn, []))
            if not urn in tree:
                tree[urn] = container
            else:
                container.name = name
                container.id = id

            parent_key = urn[:-(len(id)+1)]
            if parent_key.endswith(')'):
                parent_key = category_scheme_id
            parent = tree.get(parent_key, Container(None, None, parent_key, [container]))
            if not parent_key in tree:
                tree[parent_key] = parent
            else:
                parent.children.append(container)
        elif 'CategoryScheme' == fme_feature_type:
            if 'T' == feature.getAttribute('CategoryScheme.dissemination_perspective_id'):
                continue
            id = feature.getAttribute('CategoryScheme.id')
            name = feature.getAttribute('CategoryScheme.name')
            if not id in tree:
                tree[id] = Container(name, id, id, [])
            else:
                tree[id].name = name
        elif 'Categorisation' == fme_feature_type:
            if 'T' == feature.getAttribute('Categorisation.dissemination_perspective_id'):
                continue
            target_urn = feature.getAttribute('Categorisation.Target.urn')
            dataflow_id = feature.getAttribute('Categorisation.Source.id')
            key = item_key_func(dataflow_id)
            item = items.get(key, Item(dataflow_id, dataflow_id, key))
            if not key in items:
                items[key] = item
            target = tree.get(target_urn, Container(None, None, target_urn, [item]))
            if not target_urn in tree:
                tree[target_urn] = target
            else:
                target.children.append(item)
        elif 'Dataflow' == fme_feature_type:
            '''
            These are the leaves. The ID that we communicate with FME here must conform to some rules
            in order for FME to recognize it later on
            '''
            dataflow_id = feature.getAttribute('Dataflow.id')
            key = item_key_func(dataflow_id)
            # fme://safe.microsoft-sharepoint.microsoft-sharepoint
            #  /Orderv%C3%A4rde%20skolval%202016.xlsx
            #  ?id=%2FOrderv%C3%A4rde%20skolval%202016.xlsx
            #  &module=fmepy_microsoft_sharepoint.web_select
            #  &webservice=safe.microsoft-sharepoint.Microsoft%20SharePoint
            #  &connection=sepesd%20Microsoft%20SharePoint%20Online%20%28safe.microsoft-sharepoint%29
            #  &__microsoftsharepoint_site=swecogroup.sharepoint.com%2C87bf4075-1cb8-45b4-9142-0a108f1b0559%2C6ff5571a-fc7c-4ffd-957d-0d9c90445536&__microsoftsharepoint_document_library=b%21dUC_h7gctEWRQgoQjxsFWRpX9W98_P1PlX0NnJBEVTaF5AyJwn_6Q5N3aSujOWul
            #  &mode=file
            # fme://<publisher_uid>.<uid>.<python_package>
            name = feature.getAttribute('Dataflow.name')
            item = items.get(key, Item(name, dataflow_id, key))
            if not key in items:
                items[key] = item
            else:
                item.name = name

    return tree, items

def makeInstance(args):
    """
    The entry point for FME Workbench to directly use the Eurostate filesystem integration.

    :param dict args: Initialization arguments.
    :rtype: FMEWebFilesystem
    """
    log = get_configured_logger(LOG_NAME)
    log.info('makeInstance: %s', str(args))
    agency = Agency[args.get('AGENCY', 'ESTAT')]
    log.info('Agency now set to: %s', agency)
    #driver = EurostatFilesystemDriver(agency)
    return EurostatFilesystem(agency)

class EurostatFilesystem(IFMEWebFilesystem):
    def __init__(self, agency):
        #super(EurostatFilesystemDriver, self).__init__()
        self._session = None
        self._log = get_configured_logger(self.keyword)
        self._agency = agency
        self._tree = None
        self._items = None
        self._log.info('EurostatFilesystem initialized with keyword %s', self.keyword)

    def make_dataflow_url_key(self, dataflow_id):
        # fme://eea.fme-eurostat.fme-eurostat/FOR_ECO_CP?id=FOR_ECO_CP&module=fmepy_eurostat.catalog&webservice=eea.eurostat.Eurostat&asdf=bsdf
        return f'fme://eea.fme-eurostat.fme-eurostat/{dataflow_id}.csv?id={dataflow_id}&module=fmepy_eurostat.catalog&webservice=eea.eurostat.Eurostat&agency={self._agency.name}'

    @property
    def _driver(self):
        raise Exception('Property _driver is private!!!')
    @property
    def name(self):
        return LOG_NAME
    @property
    def keyword(self):
        return PACKAGE_KEYWORD
    def getContainerContents(self, args):
        """
        Called by the `WEB_SELECT` GUI type to obtain a listing of items from a container.
        What this means will vary depending on the terminology and concepts of the underlying Web Filesystem.
        Typically, a container is a folder, and items are subfolders and files.

        :type args: dict
        :param args: Contains these keys:

            - `CONTAINER_ID`: Identifier for the container for which to list items.
            - `QUERY`: Optional. Query/search/filter string. This is a user-specified freeform value.
              The underlying Web Filesystem implementation is responsible for understanding it.
            - LIMIT: Optional. Integer for the maximum number of items to return per page.
            - Any other arbitrary key-value pairs specified by the Web Filesystem's GUI implementation.

        :returns: A dict with keys:

            - `CONTENT`: A list. Each element is a dict with these keys:

                - `IS_CONTAINER`: Boolean for whether this item represents a type of container, e.g. a folder.
                - `ID`: Identifier for the item.
                - `NAME`: Human-readable name of the item.
                - `ICON`: Optional. Path to the item's icon relative to ``FME_HOME``, or a precompiled resource.

            - `CONTINUE`: Optional. A dict with key `ARGS`. The value of `ARGS` is a dict of arbitrary data
              that will be included with the next pagination call to this method.

        If `CONTINUE` is not present, then there are no more pages in the response.

        :rtype: dict
        """
        #raise Exception('getContainerContents not implemented')
        self._log.info('getContainerContents %s', str(args))
        return self._get_container_contents(args.get('CONTAINER_ID'))
    def _get_container_contents(self, container_id, query=None, page_size=0, **kwargs):
        """
        Get a directory listing, returned in the form expected by FME Workbench.
        The caller is responsible for proceeding through pagination, if applicable.

        :param str container_id: Identifier for the container (e.g. folder) for which contents are being listed.
        :param str query: Query or filter string for the request.
            This is an arbitrary string specific to the underlying Web Filesystem.
        :param int page_size: Requested maximum number of items to return per page.
        :returns: An dict-like object representing the directory listing.
            This object is in the form expected by FME Workbench, and represents one page of results.
            It may contain info needed to proceed to the next page.
        :rtype: ContainerContentResponse
        """
        if self._tree is None:
            datasets = [
                  self._agency.category_schemes_url
                , self._agency.categorisations_url
                , self._agency.dataflows_url
            ]
            self._tree, self._items = read_catalog(datasets, item_key_func=self.make_dataflow_url_key)
        container_key = container_id or kwargs.get('__eurostat_category_scheme')
        if not container_key:
            return ContainerContentResponse(
                [
                    ContainerItem(True, k, v.name)
                    for k,v in self._tree.items()
                    if not k.startswith('urn:')
                ]
            )
        elif container_key in self._tree:
            container = self._tree[container_key]
            if not Container == type(container):
                return ContainerContentResponse([]) 
            return ContainerContentResponse(
                [
                    ContainerItem(Container == type(item), item.key, item.name)
                    for item in container.children
                ]
            )
        return ContainerContentResponse([])

    def _download_file(self, args):
        """
        Download a single file.

        :param str file_id: Identifier for the file to download.
        :param io.BytesIO dest_file: File-like object to write into.
        """
        self._log.info('download_file %s', str(args))
        pass

    def downloadFile(self, args):
        """
        Called by Workbench to download a single file.
        Used in the context of the reader dataset value referring to a file on a Web Filesystem.

        :type args: dict
        :param args: Contains these keys:

            - `FILE_ID`: Identifier for the file to download.
            - `TARGET_FOLDER`: Local filesystem folder path to write file to.
            - `FILENAME`: Optional. Name of destination file to write to.

        :rtype: None
        """
        self._log.info('downloadFile %s', str(args))
        #downloadFile {'FILE_ID': 'FOR_VOL', 'TARGET_FOLDER': 'C:/Users/sepesd/AppData/Local/Temp/wbrun_1675946745961_15424/fmetmp_4/TempFS_1675947095053_14388', 'FILENAME': 'FOR_VOL.csv', 'AGENCY': 'ESTAT'}
        dataflow_id = args['FILE_ID']
        target_folder = args['TARGET_FOLDER']
        filename = args['FILENAME']
        url = f'{self._agency.base_uri}/sdmx/2.1/data/{dataflow_id}?format=SDMX-CSV'
        import requests
        import shutil
        import os.path
        with requests.get(url, stream=True) as r:
            with open(os.path.join(target_folder, filename), 'wb') as f:
                shutil.copyfileobj(r.raw, f)


    def downloadFolder(self, args):
        """
        Called by Workbench to download a folder, and optionally all its subfolders.
        Used in the context of the reader dataset value referring to a folder on a Web Filesystem.

        :type args: dict
        :param args: Contains these keys:

            - `CONTAINER_ID`: Identifier for the folder (container) to download.
            - `TARGET_FOLDER`: Local filesystem folder path to write to.
            - `EXCLUDE_SUB_FOLDERS`: Optional. If this key is present, then subfolders and their contents are not to be downloaded.

        :rtype: None
        """
        raise Exception('not implemented')
        pass

    def get_item_info(self, item_id, **kwargs):
        """
        Get the metadata for a single file or container.

        :param str item_id: Identifier for the item.
        :return: Metadata about the item.
        :rtype: IContainerItem
        """
        self._log.warn('get_item_info %s %s', item_id, str(kwargs))
        if self._tree is None:
            datasets = [
                  self._agency.category_schemes_url
                , self._agency.categorisations_url
                , self._agency.dataflows_url
            ]
            self._tree, self._items = read_catalog(datasets, item_key_func=self.make_dataflow_url_key)
        item = self._tree.get(item_id, self._items.get(item_id))
        if not item:
            return None
        return ContainerItem(Container == type(item), item.key, item.name)
        mode = kwargs.get("mode", None)
        if 'Category' == mode:
            container_id = kwargs.get('CONTAINER_ID')
            return self.api.info(container_id)
        pass

if __name__ == '__main__':
    def print_item(item, indent=''):
        print(indent, item.id, item.name)
        if Container == type(item):
            for child in item.children:
                print_item(child, indent + ' ')
    datasets = [
          'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/categoryscheme/ESTAT/all'
        , 'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/categorisation/ESTAT/all'
        , 'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all?detail=allstubs'
    ]
    tree, items = read_catalog(datasets, item_key_func=self.make_dataflow_url_key)
    for k,v in tree.items():
        if k.startswith('urn:'): continue
        print(k,  v.name, len(v.children))
        print_item(v)
    print(len(items))