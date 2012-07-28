from format import YamrFormat

PROXY = "proxy.yt.yandex.net"
DEFAULT_FORMAT = YamrFormat(has_subkey=True, lenval=False)
WAIT_TIMEOUT = 5.0
WRITE_BUFFER_SIZE = 10 ** 8
READ_BUFFER_SIZE = 10 ** 8
FILE_STORAGE = "//tmp/yt_wrapper/file_storage"
TEMP_TABLES_STORAGE = "//tmp/yt_wrapper/table_storage"

REPLACE_FILES_IN_OPERATION = True
MERGE_SRC_TABLES_BEFORE_OPERATION = False
USE_MAPREDUCE_STYLE_DST_TABLES = False
CHECK_RESULT = True
KEYBOARD_ABORT = True
FORCE_SORT_IN_REDUCE = False

