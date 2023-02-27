'''
Get the name, descriptions and annotations for a specified codelist:
https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/codelist/ESTAT/AIRPOL?detail=referencestubs&completestub=true
'''
from .constants import Agency
from fmeobjects import FMESession, FMEFeature, FMEFactoryPipeline
from fmegeneral.fmelog import get_configured_logger
import os.path
from dataclasses import dataclass

@dataclass
class CodeList:
    agencyID: str
    id: str
    isFinal: str
    urn: str
    version: str
    name: str
    values: dict

    def __str__(self):
        return f'CodeList(name={self.name}, id={self.id}, version={self.version}, values: {len(self.values)})'


def get(agency: Agency, codelist_ids: list, lang='en'):
    """
    Download and interpret codelist xml files
    """
    logger = get_configured_logger('codelist')
    session = FMESession()
    session.updateSettings('XFMAP_KEYWORD', f'lang {lang}')
    xfmap = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'xfmap', 'codelist.xmp')
    xfmap_encoded = session.encodeToFMEParsableText(xfmap)
    pipeline_directives = []
    pipeline = FMEFactoryPipeline('read xml', pipeline_directives)
    '''

    Copied from docs.safe.com:    
    Additional reader directives can be given through the READER_DIRECTIVES 
    clause.  Reader directives govern how a reader is created and configured (see
    IFMESession::createReader documentation for details).  If READER_PARAMS(_FMEParsableText)
    is also specified, then they will replace the value of RUNTIME_MACROS in the
    directives.  If READER_COORDSYS is also specified, it will replace the value 
    of COORDSYS in the directives.
    '''
    factory_lines = [
        ['FACTORY_DEF', '*', 'QueryFactory', 'FACTORY_NAME', 'FeatureReader', 'INPUT', 'FEATURE_TYPE', '*'
         , 'FCTQUERY_INTERACTION', 'NONE', 'COMBINE_ATTRIBUTES', 'PREFER_RESULT', 'COMBINE_GEOMETRY', 'RESULT_ONLY', 'QUERYFCT_TABLE_SEPARATOR', 'SPACE'
         , 'ENABLE_CACHE', 'YES', 'CACHE_TIMEOUT_HRS', '6'
         , 'READER_TYPE', 'XML', 'READER_DATASET', '@Value(dataset)'
         , 'READER_DIRECTIVES', 'METAFILE,XML'
         , 'QUERYFCT_OUTPUT', 'DATA_ONLY', 'CONTINUE_ON_READER_ERROR', 'YES', 'QUERYFCT_SET_FME_FEATURE_TYPE', 'YES'
         , 'READER_PARAMS_WWJD', f'FILE_TYPE,xfMap,XFMAP,{xfmap_encoded}'
         , 'TREAT_READER_PARAM_AMPERSANDS_AS_LITERALS', 'YES'
         , 'OUTPUT', 'RESULT', 'FEATURE_TYPE', 'FeatureReader_OK'
         , 'OUTPUT', 'READER_ERROR', 'FEATURE_TYPE', 'FeatureReader_ERROR'
         ]
    ]

    for factory_def in factory_lines:
        pipeline.addFactory(factory_def)
    logger.info('Factory pipeline initialized')

    for id in codelist_ids:
        feature = FMEFeature()
        dataset = f'{agency.base_uri}/sdmx/2.1/codelist/ESTAT/{id}?detail=referencestubs&completestub=true'
        feature.setAttribute('dataset', dataset)
        feature.setAttribute('codelist_id', id)
        logger.info('Reading dataset `%s`', dataset)
        pipeline.processFeature(feature)
    pipeline.allDone()
    codelists = {id: CodeList(agency.name, id, None,
                              None, None, None, dict()) for id in codelist_ids}
    logger.info('Emptying factory pipeline')
    while True:
        feature = pipeline.getOutputFeature()
        if feature is None:
            break
        if 'FeatureReader_ERROR' == feature.getFeatureType():
            raise Exception(feature.getAttribute('_reader_error'))
        codelist = codelists[feature.getAttribute('codelist_id')]
        fme_feature_type = feature.getAttribute('fme_feature_type')
        if 'Codelist' == fme_feature_type:
            # Updating attributes from codelist feature
            codelist.isFinal = feature.getAttribute('Codelist.isFinal')
            codelist.urn = feature.getAttribute('Codelist.urn')
            codelist.version = feature.getAttribute('Codelist.version')
            codelist.name = feature.getAttribute(f'Codelist.name.{lang}')
            continue
        # Adding kvp to the codelist
        key = feature.getAttribute('Code.id')
        value = feature.getAttribute(f'Code.name.{lang}')
        codelist.values[key] = value
    return codelists.values()


if __name__ == '__main__':
    from fmeobjects import FMELogFile
    log = FMELogFile()
    log.setCallBack(print)
    agency = Agency.ESTAT
    codelist_ids = ['AGRIPROD', 'GEO', 'FREQ', 'UNIT', 'OBS_FLAG']
    for codelist in get(agency, codelist_ids, lang='en'):
        print(codelist)
        for k,v in codelist.values.items():
            print('', k, v)
            break
