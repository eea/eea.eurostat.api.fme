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
import xml.parsers.expat

import os.path
XFMAP = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'xfmap', 'data_discovery.xmp')

@dataclass
class Item:
    name: str
    id: str

@dataclass
class Container(Item):
    children: List[any]

@dataclass
class CategoryScheme(Container):
    xml_id: str

@dataclass
class Category(Container):
    urn: str
    xml_id: str
    xml_parent_id: str

@dataclass
class Categorisation:
    category_urn: str
    dataflow_key: str


session = FMESession()

from fmegeneral.fmelog import get_configured_logger
logger = get_configured_logger('CATALOG')

XFMAP_ENCODED = session.encodeToFMEParsableText(XFMAP)

def read_catalog(datasets, item_key_func=lambda dataflow_id: dataflow_id):
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
    containers_by_xml_id = dict()
    containers_by_urn = dict()
    categorizations = []
    logger.info('Emptying factory pipeline')
    # Iterating records twice because we need to resolve relations...
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
            id = feature.getAttribute('Category.id')
            urn = feature.getAttribute('Category.urn')
            xml_id = feature.getAttribute('xml_id')
            xml_parent_id = feature.getAttribute('xml_parent_id')
            name = feature.getAttribute('Category.name')
            children = []
            category = Category(name, id, children, urn, xml_id, xml_parent_id)
            containers_by_xml_id[xml_id] = category
            containers_by_urn[urn] = category
            tree[id] = category
        elif 'CategoryScheme' == fme_feature_type:
            if 'T' == feature.getAttribute('CategoryScheme.dissemination_perspective_id'):
                continue
            id = feature.getAttribute('CategoryScheme.id')
            xml_id = feature.getAttribute('xml_id')
            name = feature.getAttribute('CategoryScheme.name')
            children = []
            category_scheme = CategoryScheme(name, id, children, xml_id)
            containers_by_xml_id[xml_id] = category_scheme
            tree[id] = category_scheme
        elif 'Categorisation' == fme_feature_type:
            if 'T' == feature.getAttribute('Categorisation.dissemination_perspective_id'):
                continue
            category_urn = feature.getAttribute('Categorisation.Target.urn')
            dataflow_id = feature.getAttribute('Categorisation.Source.id')
            dataflow_key = item_key_func(dataflow_id)
            categorization = Categorisation(category_urn, dataflow_key)
            categorizations.append(categorization)
            
        elif 'Dataflow' == fme_feature_type:
            '''
            These are the leaves. The ID that we communicate with FME here must conform to some rules
            in order for FME to recognize it later on
            '''
            dataflow_id = feature.getAttribute('Dataflow.id')
            id = item_key_func(dataflow_id)
            name = feature.getAttribute('Dataflow.name')
            item = Item(name, id)
            items[id] = item

    orphans = []
    for k,v in tree.items():
        if Category == type(v):
            parent = containers_by_xml_id.get(v.xml_parent_id)
            if parent is None:
                orphans.append(v)
                logger.warn('Orphan detected: %s %s', k, v.xml_parent_id)
                continue
            parent.children.append(v)
    if orphans:
        logger.warn('Orphans detected: %s', len(orphans))
    for categorization in categorizations:
        container = containers_by_urn.get(categorization.category_urn)
        item = items.get(categorization.dataflow_key)
        if Category == type(container) and Item == type(item):
            container.children.append(item)
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
                    if isinstance(v, CategoryScheme)
                ]
            )
        elif container_key in self._tree:
            container = self._tree[container_key]
            if not isinstance(container, Container):
                return ContainerContentResponse([]) 
            return ContainerContentResponse(
                [
                    ContainerItem(isinstance(item, Container), item.id, item.name)
                    for item in container.children
                ]
            )
        return ContainerContentResponse([])

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

        """
        https://<api_base_uri>/sdmx/2.1/<resource>/<flowRef>/<key>?startPeriod=value&endPeriod=value
        The start and end of the time period in the filter are determined as startPeriod and endPeriod. Following time periods are supported:
            Annual        YYYY-A1 or YYYY
            Semester      YYYY-S[1-2]
            Quarter       YYYY-Q[1-4]
            Monthly       YYYY-M[01-12] or YYYY-[01-12]
            Weekly        YYYY-W[01-53]
            Daily         YYYY-D[001-366]
            Year interval YYYY/P[01-99]Y
        """
        self._log.info('downloadFile %s', str(args))
        #downloadFile {'FILE_ID': 'FOR_VOL', 'TARGET_FOLDER': 'C:/Users/sepesd/AppData/Local/Temp/wbrun_1675946745961_15424/fmetmp_4/TempFS_1675947095053_14388', 'FILENAME': 'FOR_VOL.csv', 'AGENCY': 'ESTAT'}
        dataflow_id = args['FILE_ID']
        target_folder = args['TARGET_FOLDER']
        filename = args['FILENAME']
        start_period = args.get('STARTPERIOD')
        end_period = args.get('ENDPERIOD')
        params = {
            'format': 'SDMX-CSV'
        }

        if start_period and end_period:
            #from datetime import datetime
            #ts = datetime.strptime(start_period[:8], '%Y%m%d') #yyyymmdd
            #params['startPeriod'] = ts.strftime('%Y-D%j')
            try:
                start_period = int(start_period)
                end_period = int(end_period)
                if 1900 < start_period and start_period <= end_period and 9999 >= end_period:
                    params['startPeriod'] = start_period
                    params['endPeriod'] = end_period
                else:
                    raise Exception('start/end-period out of bounds')
            except Exception as e:
                self._log.warn(str(e))

        url = f'{self._agency.base_uri}/sdmx/2.1/data/{dataflow_id}'
        self._log.info(' url: %s', url)
        self._log.info(' params: %s', str(params))
        import requests
        import shutil
        import os.path
        with requests.get(url, params=params, stream=True) as r:
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
        return ContainerItem(Container == type(item), item.id, item.name)
        mode = kwargs.get("mode", None)
        if 'Category' == mode:
            container_id = kwargs.get('CONTAINER_ID')
            return self.api.info(container_id)
        pass


class ExpatCatalogParser:
    '''
    Not used for now but can remain as (bad) reference of how to (not) 
    read xml using expat if we ever would like to come back that...
    '''
    NS_SEP = ' '
    def __init__(self):
        self._parser = xml.parsers.expat.ParserCreate(encoding=None, namespace_separator=ExpatCatalogParser.NS_SEP)
        self._parser.StartElementHandler = self._start_element
        self._parser.EndElementHandler = self._end_element
        #p.CharacterDataHandler = char_data
        self.path = []
        self._collect_at_path = dict()
        self._items = []
        self._tree = dict()
    
    def _start_element(self, name, attributes):
        ns_uri, local_name = name.split(ExpatCatalogParser.NS_SEP)
        self.path.append(local_name)
        if local_name in ['CategoryScheme', 'Category']:
            path = tuple(self.path)
            self._collect_at_path[(*path, 'Name')] = []
            self._collect_at_path[path] = ({
                [('_', k),k.split(ExpatCatalogParser.NS_SEP)][ExpatCatalogParser.NS_SEP in k][1]: v
                for k,v in attributes.items()
            }, [])
            if 'CategoryScheme' == local_name:
                self._collect_at_path[(*path, 'Annotations', 'Annotation')] = []
            

        elif 'Name' == local_name:
            for k,v in attributes.items():
                # Some attribute names are prepended with namespace, i.e. xml:lang
                ns_uri, attr_local_name = [('_', k),k.split(ExpatCatalogParser.NS_SEP)][ExpatCatalogParser.NS_SEP in k]
                if 'lang' == attr_local_name and 'en' == v:
                    path = tuple(self.path)
                    if path in self._collect_at_path:
                        self._parser.CharacterDataHandler = self._collect_at_path[path].append
        elif 'Annotation' == local_name:
            path = tuple(self.path)
            if path in self._collect_at_path:
                self._collect_at_path[path].append({})
                self._collect_at_path[(*path, 'AnnotationType')] = []
                self._collect_at_path[(*path, 'AnnotationTitle')] = []
        elif local_name in ['AnnotationType', 'AnnotationTitle']:
            path = tuple(self.path)
            if path in self._collect_at_path:
                self._parser.CharacterDataHandler = self._collect_at_path[path].append

    def _end_element(self, name):
        ns_uri, local_name = name.split(ExpatCatalogParser.NS_SEP)
        if local_name in ['CategoryScheme', 'Category']:
            path = tuple(self.path)
            name_path = (*path, 'Name')
            collected_name = ''.join(self._collect_at_path.get(name_path, []))
            attributes, children = self._collect_at_path.get(path, ({}, []))
            if 'CategoryScheme' == local_name:
                for annotation in self._collect_at_path.get((*path, 'Annotations', 'Annotation'), []):
                    if 'DISSEMINATION_PERSPECTIVE_ID' == annotation.get('AnnotationType'):
                        attributes['DISSEMINATION_PERSPECTIVE_ID'] = annotation.get('AnnotationTitle')
                if 'id' in attributes:
                    id = attributes['id']
                    self._tree[id] = (path, collected_name, attributes, children)
            elif 'Category' == local_name:
                urn = attributes.get('urn')
                item = (path, collected_name, attributes, children)
                self._tree[urn] = item
                parent_path = tuple(self.path[:-1])
                parent_attributes, parent_children = self._collect_at_path.get(parent_path)
                parent_children.append(item)
                #parent_local_name = parent_path[-1]
                #parent_key = parent_attributes.get(['urn', 'id']['CategoryScheme' == parent_local_name])
                #print(parent_local_name, parent_key)
            self._items.append((path, collected_name, attributes, children))
        elif 'AnnotationType' == local_name:
            path = tuple(self.path)
            if path in self._collect_at_path:
                value = ''.join(self._collect_at_path.get(path, []))
                parent_path = tuple(self.path[:-1])
                current_annotation = self._collect_at_path.get(parent_path)[-1]
                current_annotation['AnnotationType'] = value
        elif 'AnnotationTitle' == local_name:
            path = tuple(self.path)
            if path in self._collect_at_path:
                value = ''.join(self._collect_at_path.get(path, []))
                parent_path = tuple(self.path[:-1])
                current_annotation = self._collect_at_path.get(parent_path)[-1]
                current_annotation['AnnotationTitle'] = value

        expected = self.path.pop()
        self._parser.CharacterDataHandler = None
    def parse(self, data, isfinal=False):
        self._parser.Parse(data, isfinal)
    def parse_file(self, file):
        self._parser.ParseFile(file)

            

if __name__ == '__main__':
    def print_item(item, indent=''):
        print(indent, item.id, item.name)
        if Container == type(item):
            for child in item.children:
                print_item(child, indent + ' ')
    def print_item2(path, collected_name, attributes, children, indent=''):
        print(indent, collected_name, attributes)
        for child in children:
            print_item2(*child, indent=indent+' ')

    datasets = [
          'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/categoryscheme/ESTAT/all'
        #, 'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/categorisation/ESTAT/all'
        #, 'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/all?detail=allstubs'
    ]
    for url in datasets:
        import requests
        r = requests.get(url)
        print(r, len(r.text))
        p = ExpatCatalogParser()
        p.parse(r.text)
        for path, collected_name, attributes, children in p._items:
            #if not 'CategoryScheme' == path[-1]: continue
            pass #print(path, collected_name, attributes, children)
        for k,(path, collected_name, attributes, children) in p._tree.items():
            print_item2(path, collected_name, attributes, children, indent=k)