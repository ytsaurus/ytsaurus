from yt.wrapper import format

import datetime
import gzip
import StringIO
import json
import struct
import logging

#==========================================

CHUNK_HEADER_FORMAT = "<QQQ"
CHUNK_HEADER_SIZE = struct.calcsize(CHUNK_HEADER_FORMAT)

#==========================================

def gzip_compress(text):
    out = StringIO.StringIO()
    with gzip.GzipFile(fileobj=out, mode="w") as f:
        f.write(text)
    return out.getvalue()


def gzip_decompress(text):
    infile = StringIO.StringIO()
    infile.write(text)
    with gzip.GzipFile(fileobj=infile, mode="r") as f:
        f.rewind()
        return f.read()

#==========================================

EVENT_LOG_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
LOGBROKER_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

def normalize_timestamp(ts):
    dt = datetime.datetime.strptime(ts, EVENT_LOG_TIMESTAMP_FORMAT)
    microseconds = dt.microsecond
    dt -= datetime.timedelta(microseconds=microseconds)
    return dt.isoformat(' '), microseconds

def revert_timestamp(normalized_ts, microseconds):
    dt = datetime.datetime.strptime(normalized_ts, LOGBROKER_TIMESTAMP_FORMAT)
    dt += datetime.timedelta(microseconds=microseconds)
    return dt.strftime(EVENT_LOG_TIMESTAMP_FORMAT)

#==========================================

def convert_to_tskved_json(row):
    result = {}
    for key, value in row.iteritems():
        if isinstance(value, basestring):
            pass
        else:
            value = json.dumps(value)
        result[key] = value
    return result

def convert_from_tskved_json(converted_row):
    result = dict()
    for key, value in converted_row.iteritems():
        new_value = value
        try:
            if isinstance(new_value, basestring):
                new_value = json.loads(new_value)
        except ValueError:
            pass

        result[key] = new_value
    return result

#==========================================

LOGBROKER_TSKV_PREFIX = "tskv\t"

def convert_to_logbroker_format(row):
    stream = StringIO.StringIO()
    stream.write(LOGBROKER_TSKV_PREFIX)
    row = convert_to_tskved_json(row)
    format.DsvFormat(enable_escaping=True).dump_row(row, stream)
    return stream.getvalue()

def convert_from_logbroker_format(converted_row):
    stream = StringIO.StringIO(converted_row)
    stream.seek(len(LOGBROKER_TSKV_PREFIX))
    return convert_from_tskved_json(format.DsvFormat(enable_escaping=True).load_row(stream))

#==========================================

def serialize_chunk(chunk_id, seqno, lines, data):
    serialized_data = struct.pack(CHUNK_HEADER_FORMAT, chunk_id, seqno, lines)
    serialized_data += gzip_compress("".join([convert_to_logbroker_format(row) for row in data]))
    return serialized_data


def parse_chunk(serialized_data):
    serialized_data = serialized_data.strip()

    index = serialized_data.find("\r\n")
    assert index != -1
    index += len("\r\n")

    chunk_id, seqno, lines = struct.unpack(CHUNK_HEADER_FORMAT, serialized_data[index:index + CHUNK_HEADER_SIZE])
    index += CHUNK_HEADER_SIZE

    decompressed_data = gzip_decompress(serialized_data[index:])

    data = []
    for line in decompressed_data.split("\n"):
        data.append(convert_from_logbroker_format(line))

    return data

#==========================================

def _preprocess(data, **args):
    return [_transform_record(record, **args) for record in data]

def _transform_record(record, cluster_name, log_name):
    try:
        normalized_ts, microseconds = normalize_timestamp(record["timestamp"])
        record.update({
            "timestamp": normalized_ts,
            "microseconds": microseconds,
            "cluster_name": cluster_name,
            "tskv_format": log_name,
            "timezone": "+0000"
        })
    except:
        logging.getLogger("Fennel").error("Unable to transform record: %r", record)
        raise
    return record

def _untransform_record(record):
    record.pop("cluster_name", None)
    record.pop("tskv_format", None)
    record.pop("timezone", None)
    microseconds = record.pop("microseconds", 0)
    timestamp = record["timestamp"]
    record["timestamp"] = revert_timestamp(timestamp, microseconds)
    return record

