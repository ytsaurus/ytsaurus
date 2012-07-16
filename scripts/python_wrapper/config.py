from format import YamrFormat

WAIT_TIMEOUT = 2.0
WRITE_BUFFER_SIZE = 10 ** 8
FILE_STORAGE = "//home/files"
TEMP_TABLES_STORAGE = "//tmp"
DEFAULT_FORMAT = YamrFormat(has_subkey=True, lenval=False)
DEFAULT_PROXY = "proxy.yt.yandex.net"
#DEFAULT_PROXY = "n01-0400g.yt.yandex.net"
AUTO_REPLACE_FILES = True
CHECK_RESULT = True
KEYBOARD_ABORT = True
FORCE_SORT_IN_REDUCE = False

MERGE_SRC_TABLES_BEFORE_OPERATION = False
USE_MAPREDUCE_STYLE_DST_TABLES = False
