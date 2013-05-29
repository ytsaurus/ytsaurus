from common import update_from_env

USE_HOSTS = True

# Turn off gzip encoding if you want to speed up reading and writing tables
REMOVE_TEMP_FILES = True

DEFAULT_FORMAT = None
TABULAR_DATA_FORMAT = None

ALWAYS_SET_EXECUTABLE_FLAG_TO_FILE = False
USE_MAPREDUCE_STYLE_DESTINATION_FDS = False
TREAT_UNEXISTING_AS_EMPTY = False
DELETE_EMPTY_TABLES = False
USE_YAMR_SORT_REDUCE_COLUMNS = False
REPLACE_TABLES_WHILE_COPY_OR_MOVE = False
CREATE_RECURSIVE = False
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

MIN_CHUNK_COUNT_FOR_MERGE_WARNING = 1000
MAX_CHUNK_SIZE_FOR_MERGE_WARNING = 32 * MB

WRITE_RETRIES_COUNT = 3

WAIT_OPERATION_RETRIES_COUNT = 1000

PYTHON_FUNCTION_CHECK_SENDING_ALL_MODULES = False
PYTHON_FUNCTION_SEARCH_EXTENSIONS = None
PYTHON_FUNCTION_MODULE_FILTER = None
PYTHON_DO_NOT_USE_PYC = False

# COMPAT(ignat): remove option when version 14 become stable
CREATE_FILE_BEFORE_UPLOAD = False

MUTATION_ID = None

def set_mapreduce_mode():
    global MAPREDUCE_MODE, ALWAYS_SET_EXECUTABLE_FLAG_TO_FILE, USE_MAPREDUCE_STYLE_DESTINATION_FDS
    global TREAT_UNEXISTING_AS_EMPTY, DEFAULT_FORMAT, DELETE_EMPTY_TABLES, USE_YAMR_SORT_REDUCE_COLUMNS
    global REPLACE_TABLES_WHILE_COPY_OR_MOVE, CREATE_RECURSIVE
    global THROW_ON_EMPTY_DST_LIST, RUN_MAP_REDUCE_IF_SOURCE_IS_NOT_SORTED
    ALWAYS_SET_EXECUTABLE_FLAG_TO_FILE = True
    USE_MAPREDUCE_STYLE_DESTINATION_FDS = True
    TREAT_UNEXISTING_AS_EMPTY = True
    DELETE_EMPTY_TABLES = True
    USE_YAMR_SORT_REDUCE_COLUMNS = True
    REPLACE_TABLES_WHILE_COPY_OR_MOVE = True
    CREATE_RECURSIVE = True
    THROW_ON_EMPTY_DST_LIST = True
    RUN_MAP_REDUCE_IF_SOURCE_IS_NOT_SORTED = True
    from format import YamrFormat
    DEFAULT_FORMAT = YamrFormat(has_subkey=True, lenval=False)

update_from_env(globals())

from errors_config import *
from http_config import *

import http

def _parse_api(self, description):
    commands = {}
    for elem in description:
        name = elem["name"]
        del elem["name"]

        for key in elem:
            if elem[key] == "null":
                elem[key] = None

        commands[name] = Command(**elem)
    return commands

_api = http.get_api(PROXY)
if "v2" in _api:
    COMMANDS = http.get_api(PROXY, version="v2")
    API_PATH = "api/v2"
    RETRY_VOLATILE_COMMANDS = True
    CREATE_FILE_BEFORE_UPLOAD = True
else:
    COMMANDS = _parse_api(_api)
    API_PATH = "api"
    RETRY_VOLATILE_COMMANDS = False
    CREATE_FILE_BEFORE_UPLOAD = False
