from .reader import EurostatReader
from .constants import LOG_NAME
from fmegeneral.fmelog import get_configured_logger
from fmegeneral.plugins import FMEMappingFileWrapper
from fmegeneral import fmeconstants

READER_IMPL = {
    'EEA.EUROSTAT.EUROSTATPY': EurostatReader
}

def FME_createReader(reader_type, src_keyword, mapping_file):
    log = get_configured_logger(
          src_keyword
        , mapping_file.fetch(fmeconstants.kFME_DEBUG) is not None
    )
    if not reader_type in READER_IMPL:
        log.warn('FME_createReader %s %s - Not implemented', reader_type, src_keyword)
        return None
    mapping_file_wrapper = FMEMappingFileWrapper(mapping_file, src_keyword, reader_type)
    reader_cls = READER_IMPL.get(reader_type)
    return reader_cls(mapping_file_wrapper, log)

def FME_createWriter(writer_type, dest_keyword, mapping_file):
    #log.warn('FME_createWriter %s %s - Not implemented', writer_type, dest_keyword)
    
    return None
    