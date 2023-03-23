from pluginbuilder import FMEReader
from fmeobjects import FMEFeature
from fmegeneral.parsers import OpenParameters, parse_def_line
from urllib.parse import (urlparse, parse_qs)

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
        self._log.info('START_PERIOD: %s', mapping_file.get('START_PERIOD'))


        self._schema_iterator = None
        self._feature_iterator = None
        self._mapping_file = mapping_file

        self._feature_types = set()

        
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
        parsed_parameters = OpenParameters(datasetName, parameters)
        self._log.info(' parsed_parameters: %s', parsed_parameters)
        mapping_file_parameters = {k: self._mapping_file.get(k) for k in ['CONNECTION', 'START_PERIOD', 'END_PERIOD', 'FIRST_N_OBSERVATIONS', 'LAST_N_OBSERVATIONS']}
        self._log.info(' mapping_file_parameters: %s', mapping_file_parameters)
        ids = parsed_parameters.get('+ID')
        if str == type(ids):
            ids = [ids]
        elif ids is None:
            ids = []
        for feature_type_id in ids:
            self._log.info('  feature_type_id: %s', feature_type_id)
            feature_type_url = urlparse(feature_type_id) # expecting fme://-url from web select here
            #self._log.info('  feature_type_url: %s', feature_type_url)
            query_params = parse_qs(feature_type_url.query)
            #self._log.info('  query_params: %s', query_params)
            dataflow_id, *_ = query_params.get('id', [None])
            #self._log.info('  dataflow_id: %s', dataflow_id)
            self._feature_types.add(dataflow_id)
        self._log.info('Parsing mapping file def-lines')
        for defline in self._mapping_file.defLines():
            feature_type, attributes, options = parse_def_line(defline, ['fme_attribute_reading', 'eurostat_where_clause'])
            self._log.info(' %s %s %s', feature_type, attributes, options)
            self._feature_types.add(feature_type)
    def read(self):
        if self._feature_iterator is None:
            self._feature_iterator = iter([
                (ft, [('asdf', 'Hello World!'),('bsdf', 42)])
                for ft in self._feature_types
            ])
        record = next(self._feature_iterator, None)
        if record:
            feature_type, attributes = record    
            feature = FMEFeature()
            feature.setFeatureType(feature_type)
            for n,t in attributes:
                feature.setAttribute(n, t)
            return feature
        return None


    def readSchema(self):
        if self._schema_iterator is None:
            self._schema_iterator = iter([
                (ft, [('asdf', 'str'),('bsdf', 'int')])
                for ft in self._feature_types
            ])
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
    
