from format import YamrFormat

import os
#import sys

PROXY = "proxy.yt.yandex.net"

# Turn off gzip encoding if you want to speed up reading and writing tables
ACCEPT_ENCODING = os.environ.get("ACCEPT_ENCODING", "identity, gzip")
REMOVE_TEMP_FILES = True

DEFAULT_FORMAT = None

ALWAYS_SET_EXECUTABLE_FLAG_TO_FILE = False
USE_MAPREDUCE_STYLE_DESTINATION_FDS = False
UNEXISTING_AS_EMPTY = False
DELETE_EMPTY_TABLES = False
USE_YAMR_SORT_REDUCE_COLUMNS = False
REPLACE_TABLES_WHILE_COPY_OR_MOVE = False
MKDIR_RECURSIVE = False

WAIT_TIMEOUT = 5.0
ERRORS_TO_PRINT_LIMIT = 100
WRITE_BUFFER_SIZE = 10 ** 9
READ_BUFFER_SIZE = 10 ** 7
HTTP_CHUNK_SIZE = 10 * 1024
MEMORY_LIMIT = 10 ** 9
FILE_STORAGE = "//tmp/yt_wrapper/file_storage"
TEMP_TABLES_STORAGE = "//tmp/yt_wrapper/table_storage"

KEYBOARD_ABORT = True

TRANSACTION = "0-0-0-0"
PING_ANSECTOR_TRANSACTIONS = False

MB = 2 ** 20
CLUSTER_SIZE = 235
MAX_SIZE_PER_JOB = 512 * MB
MIN_SIZE_PER_JOB = 16 * MB

USE_SHORT_OPERATION_INFO = False

WRITE_RETRIES_COUNT = 3

def set_mapreduce_mode():
    global MAPREDUCE_MODE, ALWAYS_SET_EXECUTABLE_FLAG_TO_FILE, USE_MAPREDUCE_STYLE_DESTINATION_FDS, UNEXISTING_AS_EMPTY, DEFAULT_FORMAT, DELETE_EMPTY_TABLES, USE_YAMR_SORT_REDUCE_COLUMNS, REPLACE_TABLES_WHILE_COPY_OR_MOVE, MKDIR_RECURSIVE
    ALWAYS_SET_EXECUTABLE_FLAG_TO_FILE = True
    USE_MAPREDUCE_STYLE_DESTINATION_FDS = True
    UNEXISTING_AS_EMPTY = True
    DELETE_EMPTY_TABLES = True
    USE_YAMR_SORT_REDUCE_COLUMNS = True
    REPLACE_TABLES_WHILE_COPY_OR_MOVE = True
    MKDIR_RECURSIVE = True
    DEFAULT_FORMAT = YamrFormat(has_subkey=True, lenval=False)

for key, value in os.environ.iteritems():
    if key.startswith("YT_"):
        key = key[3:]
        var_type = str
        if key not in globals():
            #print >>sys.stderr, "There is no variable %s in config, so it affect nothing" % key
            continue
        else:
            var_type = type(globals()[key])
        globals()[key] = var_type(value)
