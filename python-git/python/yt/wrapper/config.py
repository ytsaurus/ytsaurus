from format import YamrFormat

PROXY = "proxy.yt.yandex.net"

# Turn off gzip encoding if you want to speed up reading and writing tables
ACCEPT_ENCODING = "identity, gzip"

MAPREDUCE_MODE = True
DEFAULT_FORMAT = YamrFormat(has_subkey=True, lenval=False)
USE_MAPREDUCE_STYLE_DESTINATION_FDS = False

WAIT_TIMEOUT = 5.0
WRITE_BUFFER_SIZE = 10 ** 7
READ_BUFFER_SIZE = 10 ** 7
HTTP_CHUNK_SIZE = 10 * 1024
MEMORY_LIMIT = 10 ** 9
FILE_STORAGE = "//tmp/yt_wrapper/file_storage"
TEMP_TABLES_STORAGE = "//tmp/yt_wrapper/table_storage"

CHECK_RESULT = True
KEYBOARD_ABORT = True
EXIT_WITHOUT_TRACEBACK = False
FORCE_SORT_IN_REDUCE = False

TRANSACTION = "0-0-0-0"
PING_ANSECTOR_TRANSACTIONS = False

