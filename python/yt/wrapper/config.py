from format import YamrFormat

PROXY = "n01-0400g.yt.yandex.net"
ACCEPT_ENCODING = "identity, gzip"

DEFAULT_FORMAT = YamrFormat(has_subkey=True, lenval=False)
WAIT_TIMEOUT = 5.0
WRITE_BUFFER_SIZE = 10 ** 7
READ_BUFFER_SIZE = 10 ** 7
FILE_STORAGE = "//tmp/yt_wrapper/file_storage"
TEMP_TABLES_STORAGE = "//tmp/yt_wrapper/table_storage"

MERGE_SRC_TABLES_BEFORE_OPERATION = False
USE_MAPREDUCE_STYLE_DST_TABLES = False
CHECK_RESULT = True
KEYBOARD_ABORT = True
FORCE_SORT_IN_REDUCE = False

TRANSACTION = "0-0-0-0"
PING_ANSECTOR_TRANSACTIONS = False

