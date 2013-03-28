import os

PROXY = ""
USE_HOSTS = False
USE_TOKEN = True

# Turn off gzip encoding if you want to speed up reading and writing tables
ACCEPT_ENCODING = os.environ.get("ACCEPT_ENCODING", "identity, gzip")
REMOVE_TEMP_FILES = True

DEFAULT_FORMAT = None
DEFAULT_TABULAR_FORMAT = None

ALWAYS_SET_EXECUTABLE_FLAG_TO_FILE = False
USE_MAPREDUCE_STYLE_DESTINATION_FDS = False
TREAT_UNEXISTING_AS_EMPTY = False
DELETE_EMPTY_TABLES = False
USE_YAMR_SORT_REDUCE_COLUMNS = False
REPLACE_TABLES_WHILE_COPY_OR_MOVE = False
CREATE_RECURSIVE = False
DO_NOT_UPLOAD_EMPTY_FILES = False
THROW_ON_EMPTY_DST_LIST = False
RUN_MAP_REDUCE_IF_SOURCE_IS_NOT_SORTED = False

MB = 2 ** 20

WAIT_TIMEOUT = 5.0
PRINT_STDERRS = False
ERRORS_TO_PRINT_LIMIT = 100
WRITE_BUFFER_SIZE = 2000 * MB
READ_BUFFER_SIZE = 10 ** 7
HTTP_CHUNK_SIZE = 10 * 1024
MEMORY_LIMIT = 256 * MB
FILE_STORAGE = "//tmp/yt_wrapper/file_storage"
TEMP_TABLES_STORAGE = "//tmp/yt_wrapper/table_storage"

KEYBOARD_ABORT = True
DETACHED = True

PREFIX = ""

TRANSACTION = "0-0-0-0"
PING_ANSECTOR_TRANSACTIONS = False
TRANSACTION_TIMEOUT = 60 * 1000
OPERATION_TRANSACTION_TIMEOUT = 5 * 60 * 1000

USE_RETRIES_DURING_WRITE = True
WRITE_TRANSACTION_TIMEOUT = 60 * 1000

CLUSTER_SIZE = 235
MAX_SIZE_PER_JOB = 512 * MB
MIN_SIZE_PER_JOB = 16 * MB

USE_SHORT_OPERATION_INFO = False

WRITE_RETRIES_COUNT = 3

CONNECTION_TIMEOUT = 120.0
WAIT_OPERATION_CONNECTION_TIMEOUT = 12 * 60 * 60 # twelve hours

MIN_CHUNK_COUNT_FOR_MERGE_WARNING = 1000
MAX_CHUNK_SIZE_FOR_MERGE_WARNING = 32 * MB

ERROR_FORMAT = "text"

PYTHON_FUNCTION_SEARCH_EXTENSIONS = None
PYTHON_FUNCTION_MODULE_FILTER = None
PYTHON_FUNCTION_CHECK_SENDING_ALL_MODULES = True

def set_mapreduce_mode():
    global MAPREDUCE_MODE, ALWAYS_SET_EXECUTABLE_FLAG_TO_FILE, USE_MAPREDUCE_STYLE_DESTINATION_FDS
    global TREAT_UNEXISTING_AS_EMPTY, DEFAULT_FORMAT, DELETE_EMPTY_TABLES, USE_YAMR_SORT_REDUCE_COLUMNS
    global REPLACE_TABLES_WHILE_COPY_OR_MOVE, CREATE_RECURSIVE, DO_NOT_UPLOAD_EMPTY_FILES
    global THROW_ON_EMPTY_DST_LIST, RUN_MAP_REDUCE_IF_SOURCE_IS_NOT_SORTED
    ALWAYS_SET_EXECUTABLE_FLAG_TO_FILE = True
    USE_MAPREDUCE_STYLE_DESTINATION_FDS = True
    TREAT_UNEXISTING_AS_EMPTY = True
    DELETE_EMPTY_TABLES = True
    USE_YAMR_SORT_REDUCE_COLUMNS = True
    REPLACE_TABLES_WHILE_COPY_OR_MOVE = True
    CREATE_RECURSIVE = True
    DO_NOT_UPLOAD_EMPTY_FILES = True
    THROW_ON_EMPTY_DST_LIST = True
    RUN_MAP_REDUCE_IF_SOURCE_IS_NOT_SORTED = True
    from format import YamrFormat
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
        if var_type == bool:
            try:
                value = int(value)
            except:
                pass
        if isinstance(None, var_type):
            var_type = str
        globals()[key] = var_type(value)
