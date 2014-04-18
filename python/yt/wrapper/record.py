import config
from yamr_record import SimpleRecord, SubkeyedRecord

""" Methods for records conversion """
# Deprecated
def record_to_line(rec, format=None, eoln=True):
    if format is None: format = config.format.TABULAR_DATA_FORMAT

    return format.dumps_row(rec)

def line_to_record(line, format=None):
    if format is None: format = config.format.TABULAR_DATA_FORMAT

    return format.loads_row(line)

def extract_key(rec, fields):
    if isinstance(rec, SimpleRecord) or isinstance(rec, SubkeyedRecord):
        return rec.key
    else:
        return dict((key, rec[key]) for key in fields if key in rec)
