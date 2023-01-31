from .reader import EurostatReader
import logging
from .constants import LOG_NAME
log = logging.getLogger(LOG_NAME)
log.info('Module loaded')

def FME_createReader(reader_type, src_keyword, mapping_file):
    """
    Reader entry point called by FME.
    FME expects this function name and signature.
    """
    #from . import reader as reader_impl
    log.warn('FME_createReader %s %s - Not implemented', reader_type, src_keyword)
    return None #reader_impl.Reportnet3Reader(reader_type, src_keyword, mapping_file)

def FME_createWriter(writer_type, dest_keyword, mapping_file):
    log.warn('FME_createWriter %s %s - Not implemented', writer_type, dest_keyword)
    
    return None
    