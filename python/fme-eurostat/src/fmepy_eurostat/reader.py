from pluginbuilder import FMEReader
from fmeobjects import FMEFeature

class EurostatReader(FMEReader):
    def __init__(self, mapping_file, log):
        self._log = log
        self._log.info('EurostatReader initiated')
        ''' mapping file wrapper methods:
        defLines
        fetchWithPrefix
        get
        getFeatureTypes
        getFlag
        getSearchEnvelope
        mappingFile
        '''
        self._log.info('CONNECTION: %s', mapping_file.get('CONNECTION'))
        self._log.info('DATAFLOW: %s', mapping_file.get('DATAFLOW'))
        self._schema_iterator = iter([
            ('Sample', [('asdf', 'str'),('bsdf', 'int')])
        ])
        
    def abort(self):
        pass
    def close(self):
        pass
    def getProperties(self, propertyCategory):
        self._log.debug('getProperties %s', propertyCategory)
        return {
                'fme_search_type': [
                      'fme_search_type', 'fme_all_features'
                ]
                , 'fme_all_features': [
                      'fme_all_features', 'fme_where'
                    , 'fme_all_features', 'fme_feature_type'
                ]
            }.get(propertyCategory, None)
    def open(self, datasetName, parameters):
        self._log.info('open')
        self._log.info(' datasetName: %s', datasetName)
        self._log.info(' parameters: %s', parameters)
    def read(self):
        return None

    def readSchema(self):
        schema_record = next(self._schema_iterator, None)
        self._log.info('schema record: %s', schema_record)
        if schema_record:
            feature_type, attributes = schema_record    
            feature = FMEFeature()
            feature.setFeatureType(feature_type)
            for n,t in attributes:
                feature.setSequencedAttribute(n, t)
            feature.setAttribute('fme_geometry{}', ['eurostat_none'])
            return feature
        return None
    def setConstraints(self, feature):
        pass
    def spatialEnabled(self):
        return True
    
