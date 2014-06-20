""" Methods for records conversion """

from format import dumps_row, loads_row
from yamr_record import SimpleRecord, SubkeyedRecord

def record_to_line(rec, format=None, eoln=True):
    """.. note:: Deprecated. Use :py:func:`yt.wrapper.format.dumps_row` instead."""
    return dumps_row(rec, format)

def line_to_record(line, format=None):
    """.. note:: Deprecated. Use :py:func:`yt.wrapper.format.loads_row` instead"""
    return loads_row(line, format)

def extract_key(rec, fields):
    if isinstance(rec, SimpleRecord) or isinstance(rec, SubkeyedRecord):
        return rec.key
    else:
        return dict((key, rec[key]) for key in fields if key in rec)
