from format import dumps_row, loads_row
from yamr_record import SimpleRecord, SubkeyedRecord

""" Methods for records conversion """
# Deprecated
def record_to_line(rec, format=None, eoln=True):
    return dumps_row(rec, format)

def line_to_record(line, format=None):
    return loads_row(line, format)

def extract_key(rec, fields):
    if isinstance(rec, SimpleRecord) or isinstance(rec, SubkeyedRecord):
        return rec.key
    else:
        return dict((key, rec[key]) for key in fields if key in rec)
