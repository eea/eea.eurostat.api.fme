from dataclasses import dataclass
from typing import List
from fmeobjects import FMESession, FMEFeature, FMEFactoryPipeline
from .constants import (LOG_NAME, Agency, PACKAGE_KEYWORD)
from ._vendor.webserviceconnector.fmewebfs import (
    FMEWebFilesystemDriver
)
from ._vendor.webserviceconnector.fmewebfs import (
    ContainerContentResponse,
    ContainerItem,
    FMEWebFilesystem
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

def read_catalog(datasets, tree=dict(), items=dict()):
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
            id = feature.getAttribute('Categorisation.Source.id')
            item = items.get(id, Item(id, id, id))
            if not id in items:
                items[id] = item
            target = tree.get(target_urn, Container(None, None, target_urn, [item]))
            if not target_urn in tree:
                tree[target_urn] = target
            else:
                target.children.append(item)
        elif 'Dataflow' == fme_feature_type:
            id = feature.getAttribute('Dataflow.id')
            name = feature.getAttribute('Dataflow.name')
            item = items.get(id, Item(name, id, id))
            if not id in items:
                items[id] = item
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
    agency = Agency[args.get('AGENCY', 'ESTAT')]
    log.info('Agency set to: %s', agency)
    driver = EurostatFilesystemDriver(agency)
    return FMEWebFilesystem(driver)

class EurostatFilesystemDriver(FMEWebFilesystemDriver):
    def __init__(self, agency):
        super(EurostatFilesystemDriver, self).__init__()
        self._session = None
        self._log = get_configured_logger(self.keyword)
        self._agency = agency
        self._tree = None
        self._items = None

    @property
    def name(self):
        return LOG_NAME
    @property
    def keyword(self):
        return PACKAGE_KEYWORD

    def get_container_contents(self, container_id, query=None, page_size=0, **kwargs):
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
            self._tree, self._items = read_catalog(datasets)
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

    def download_file(self, file_id, dest_file, **kwargs):
        """
        Download a single file.

        :param str file_id: Identifier for the file to download.
        :param io.BytesIO dest_file: File-like object to write into.
        """
        pass

    def delete_item(self, item_id, **kwargs):
        """
        Delete an item.

        :param str item_id: Identifier for the item to delete.
            It may be a file or a folder. It might not exist.
        """
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
            self._tree, self._items = read_catalog(datasets)
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
    tree, items = read_catalog(datasets)
    for k,v in tree.items():
        if k.startswith('urn:'): continue
        print(k,  v.name, len(v.children))
        print_item(v)
    print(len(items))