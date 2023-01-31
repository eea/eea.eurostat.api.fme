import fmeobjects
from fmeobjects import FMEUniversalReader
from fmegeneral.fmelog import get_configured_logger

from ._vendor.webserviceconnector.fmewebfs import FMEWebFilesystem

from ._vendor.webserviceconnector.fmewebfs import (
    ContainerContentResponse,
    ContainerItem,
)
from .driver import EurostatFilesystemDriver
from .constants import (PARAM_CREDENTIAL_SOURCE, LOG_NAME, PARAM_PROJECT_ID, Agency)

import xml.parsers.expat

NS_SEP = ' '
XML_NS = 'http://www.w3.org/XML/1998/namespace'
XML_LANG = NS_SEP.join([XML_NS, 'lang'])

def makeInstance(args):
    """
    The entry point for FME Workbench to directly use the Eurostate filesystem integration.

    :param dict args: Initialization arguments.
    :rtype: FMEWebFilesystem
    """
    log = get_configured_logger(LOG_NAME)
    agency = Agency[args.get('AGENCY', 'ESTAT')]
    log.info('Agency set to: %s', agency)
    driver = EurostatFilesystemDriver()


    def apifunc():
        # https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/categoryscheme/ESTAT/all
        return ThemesEnumeratorFactoryPipelineExpat(agency, log)

    driver.set_api(apifunc=apifunc)

    return FMEWebFilesystem(driver)

class Resource(ContainerItem):
    def __init__(self, id, name):
        super(Resource, self).__init__(True, id, name)
        self._item = name

class ThemesEnumeratorFactoryPipelineExpat():   
    def __init__(self, agency, log):
        self.agency = agency
        self.log = log
        self.items = None
        self.tree = None
    
    def list_category_schemes(self):
        if self.items is None:
            self.init_items()
        return ContainerContentResponse(
            [
                ContainerItem(False, path[0], name)
                for path, name, annotations
                in self.items
                if 1 == len(path)
            ]
        )
    def info(self, container_id):
        if self.items is None:
            self.init_items()
        if self.tree is None:
            self.tree = {
                '/'.join(p): n
                for p,n,_ in self.items
            }
        return ContainerItem(True, container_id, self.tree.get(container_id, container_id))
    def list_categories(self, parent):
        if self.items is None:
            self.init_items()
        parent_path = tuple(parent.split('/'))
        depth = len(parent_path)  + 1
        return ContainerContentResponse(
            [
                ContainerItem(True, '/'.join(path), name)
                for path, name, annotations
                in self.items
                if len(path) == depth and path[:-1] == parent_path
            ]
        )

    def init_items(self):
        import sqlite3
        import os.path
        db_fp = os.path.join(__file__, 'cache.sqlite3')
        self.log.info('Cache filepath; `%s`', db_fp)
        try:
            conn = sqlite3.connect(db_fp)
            conn.close()
        except Exception as e:
            raise Exception(f'Failed to connect to sqlite using file path `{db_fp}`', e)
        directives = []
        pipeline = fmeobjects.FMEFactoryPipeline('read xml', directives)
        #dataset = r'D:\eea\eurostat\samples\ec.europa.eu\eurostat\api\dissemination\sdmx\2.1\categoryscheme\ESTAT\all.xml'
        #dataset = 'https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/categoryscheme/ESTAT/all'
        dataset = self.agency.category_schemes_url
        factory_lines = [
            ['FACTORY_DEF', '*', 'QueryFactory'
                , 'FACTORY_NAME', 'FeatureReader'
                , 'INPUT', 'FEATURE_TYPE', '*'
                , 'FCTQUERY_INTERACTION', 'NONE'
                , 'COMBINE_ATTRIBUTES', 'RESULT_ONLY'
                , 'COMBINE_GEOMETRY', 'RESULT_ONLY'
                , 'ENABLE_CACHE', 'YES'
                , 'CACHE_TIMEOUT_HRS', '6'
                , 'READER_TYPE', 'TEXTLINE'
                , 'READER_DATASET', '@Value(dataset)'
                , 'READER_DIRECTIVES', 'METAFILE,TEXTLINE'
                , 'QUERYFCT_OUTPUT', 'DATA_ONLY'
                , 'CONTINUE_ON_READER_ERROR', 'YES'
                , 'QUERYFCT_SET_FME_FEATURE_TYPE', 'YES'
                , 'READER_PARAMS', 'ENCODING','utf-8','READ_WHOLE_FILE_AT_ONCE','YES', 'CREATE_FEATURE_TABLES_FROM_DATA', 'No'
                , 'TREAT_READER_PARAM_AMPERSANDS_AS_LITERALS', 'YES'
                , 'OUTPUT', 'RESULT', 'FEATURE_TYPE', 'FeatureReader_OK'
                , 'OUTPUT', 'READER_ERROR', 'FEATURE_TYPE', 'FeatureReader_ERROR'
            ]
        ]
        for factory_def in factory_lines:
            pipeline.addFactory(factory_def)

        feature = fmeobjects.FMEFeature()
        feature.setAttribute('dataset', dataset)
        pipeline.processFeature(feature)
        pipeline.allDone()
        feature = pipeline.getOutputFeature()
        if feature is None:
            raise Exception('Unexpected')
        if 'FeatureReader_ERROR' == feature.getFeatureType():
            raise Exception(feature.getAttribute('_reader_error'))

        xml_data = feature.getAttribute('text_line_data')

        self.items = []
        parse_string(xml_data, callback_fn=self.items.append)

class ThemesEnumeratorUniversalReaderXmp():
    def __init__(self, agency, log):
        self.agency = agency
        self.log = log
    def list_themes(self):
        def iter_themes():
            import os.path
            xfmap = os.path.join(os.path.dirname(__file__), 'xfmap', 'category_scheme.xmp')
            dataset = self.agency.category_schemes_url
                    
            reader_type = 'XML'
            directives = []
            cache_feature = False
            reader = FMEUniversalReader(reader_type, cache_feature, directives)

            parameters = ['XFMAP', xfmap]

            reader.open(dataset, parameters)
            while True:
                feature = reader.read()
                if feature is None: break
                id, name = feature.getAttribute('id'), feature.getAttribute('name')
                resource = Resource(id, name)
                self.log.info(' %s %s %s', id, name, resource)
                yield resource
            reader.close()
        return ContainerContentResponse(
                    list(iter_themes())
                )




class Handler:
    def __init__(self, callback=print):
        self.data = []
        self.category_path = []
        self.collect_data = False
        self.callback = callback
        self.annotation_title = None
        self.annotation_type = None
        self.annotations = {}
        self.category_scheme_annotations = {}

    def start_element(self, qname, attrs):
        ns_uri, local_name = qname.split(NS_SEP)
        if 'CategoryScheme' == local_name or 'Category' == local_name:
            self.category_path.append(attrs.get('id'))
            self.annotations = {}
        if 'CategoryScheme' == local_name:
            self.category_scheme_annotations = {}
        if 'Name' == local_name and 'en' == attrs.get(XML_LANG) or 'AnnotationTitle' == local_name or 'AnnotationType' == local_name:
            self.collect_data = True

    def end_element(self, qname):
        ns_uri, local_name = qname.split(NS_SEP)
        if 'CategoryScheme' == local_name or 'Category' == local_name:
            self.category_path.pop()
        if 'Name' == local_name and self.data:
            name = ''.join(self.data)
            path = tuple(self.category_path)
            if 1 == len(self.category_path):
                self.category_scheme_annotations = self.annotations
            if not 'T' == self.category_scheme_annotations.get('DISSEMINATION_PERSPECTIVE_ID'):
                self.callback((path, name, self.annotations))
        elif 'AnnotationTitle' == local_name:
            self.annotation_title = ''.join(self.data)
        elif 'AnnotationType' == local_name:
            self.annotation_type = ''.join(self.data)
        elif 'Annotation' == local_name and not self.annotation_type.endswith('ICON'):
            self.annotations[self.annotation_type] = self.annotation_title

        self.collect_data = False
        self.data = []

    def char_data(self, data):
        if not self.collect_data:
            return
        stripped = data.strip()
        if len(stripped):
            self.data.append(stripped)

def parse_string(xml_data, callback_fn=print):
    handler = Handler(callback_fn)
    parser = xml.parsers.expat.ParserCreate(namespace_separator=NS_SEP)
    parser.StartElementHandler = handler.start_element
    parser.EndElementHandler = handler.end_element
    parser.CharacterDataHandler = handler.char_data
    parser.Parse(xml_data)