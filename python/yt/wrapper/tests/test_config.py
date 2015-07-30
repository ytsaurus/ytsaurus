import yt.wrapper as yt
from yt.common import update

import os
import pytest

@pytest.mark.usefixtures("yt_env")
class TestConfig(object):
    def test_old_config_options(self):
        yt.config.http.PROXY = yt.config.http.PROXY
        yt.config.http.PROXY_SUFFIX = yt.config.http.PROXY_SUFFIX
        yt.config.http.TOKEN = yt.config.http.TOKEN
        yt.config.http.TOKEN_PATH = yt.config.http.TOKEN_PATH
        yt.config.http.USE_TOKEN = yt.config.http.USE_TOKEN
        yt.config.http.ACCEPT_ENCODING = yt.config.http.ACCEPT_ENCODING
        yt.config.http.CONTENT_ENCODING = yt.config.http.CONTENT_ENCODING
        yt.config.http.REQUEST_RETRY_TIMEOUT = yt.config.http.REQUEST_RETRY_TIMEOUT
        yt.config.http.REQUEST_RETRY_COUNT = yt.config.http.REQUEST_RETRY_COUNT
        yt.config.http.REQUEST_BACKOFF = yt.config.http.REQUEST_BACKOFF
        yt.config.http.FORCE_IPV4 = yt.config.http.FORCE_IPV4
        yt.config.http.FORCE_IPV6 = yt.config.http.FORCE_IPV6
        yt.config.http.HEADER_FORMAT = yt.config.http.HEADER_FORMAT

        yt.config.VERSION = yt.config.VERSION
        yt.config.OPERATION_LINK_PATTERN = yt.config.OPERATION_LINK_PATTERN

        yt.config.DRIVER_CONFIG = yt.config.DRIVER_CONFIG
        yt.config.DRIVER_CONFIG_PATH = yt.config.DRIVER_CONFIG_PATH

        yt.config.USE_HOSTS = yt.config.USE_HOSTS
        yt.config.HOSTS = yt.config.HOSTS
        yt.config.HOST_BAN_PERIOD = yt.config.HOST_BAN_PERIOD

        yt.config.ALWAYS_SET_EXECUTABLE_FLAG_TO_FILE = yt.config.ALWAYS_SET_EXECUTABLE_FLAG_TO_FILE
        yt.config.USE_MAPREDUCE_STYLE_DESTINATION_FDS = yt.config.USE_MAPREDUCE_STYLE_DESTINATION_FDS
        yt.config.TREAT_UNEXISTING_AS_EMPTY = yt.config.TREAT_UNEXISTING_AS_EMPTY
        yt.config.DELETE_EMPTY_TABLES = yt.config.DELETE_EMPTY_TABLES
        yt.config.USE_YAMR_SORT_REDUCE_COLUMNS = yt.config.USE_YAMR_SORT_REDUCE_COLUMNS
        yt.config.REPLACE_TABLES_WHILE_COPY_OR_MOVE = yt.config.REPLACE_TABLES_WHILE_COPY_OR_MOVE
        yt.config.CREATE_RECURSIVE = yt.config.CREATE_RECURSIVE
        yt.config.THROW_ON_EMPTY_DST_LIST = yt.config.THROW_ON_EMPTY_DST_LIST
        yt.config.RUN_MAP_REDUCE_IF_SOURCE_IS_NOT_SORTED = yt.config.RUN_MAP_REDUCE_IF_SOURCE_IS_NOT_SORTED
        yt.config.USE_NON_STRICT_UPPER_KEY = yt.config.USE_NON_STRICT_UPPER_KEY
        yt.config.CHECK_INPUT_FULLY_CONSUMED = yt.config.CHECK_INPUT_FULLY_CONSUMED
        yt.config.FORCE_DROP_DST = yt.config.FORCE_DROP_DST

        yt.config.OPERATION_STATE_UPDATE_PERIOD = yt.config.OPERATION_STATE_UPDATE_PERIOD
        yt.config.STDERR_LOGGING_LEVEL = yt.config.STDERR_LOGGING_LEVEL
        yt.config.IGNORE_STDERR_IF_DOWNLOAD_FAILED = yt.config.IGNORE_STDERR_IF_DOWNLOAD_FAILED
        yt.config.READ_BUFFER_SIZE = yt.config.READ_BUFFER_SIZE
        yt.config.MEMORY_LIMIT = yt.config.MEMORY_LIMIT

        yt.config.FILE_STORAGE = yt.config.FILE_STORAGE
        yt.config.TEMP_TABLES_STORAGE = yt.config.TEMP_TABLES_STORAGE
        yt.config.LOCAL_TMP_DIR = yt.config.LOCAL_TMP_DIR
        yt.config.REMOVE_TEMP_FILES = yt.config.REMOVE_TEMP_FILES

        yt.config.KEYBOARD_ABORT = yt.config.KEYBOARD_ABORT

        yt.config.MERGE_INSTEAD_WARNING = yt.config.MERGE_INSTEAD_WARNING
        yt.config.MIN_CHUNK_COUNT_FOR_MERGE_WARNING = yt.config.MIN_CHUNK_COUNT_FOR_MERGE_WARNING
        yt.config.MAX_CHUNK_SIZE_FOR_MERGE_WARNING  = yt.config.MAX_CHUNK_SIZE_FOR_MERGE_WARNING

        yt.config.PREFIX = yt.config.PREFIX

        yt.config.TRANSACTION_TIMEOUT = yt.config.TRANSACTION_TIMEOUT
        yt.config.TRANSACTION_SLEEP_PERIOD = yt.config.TRANSACTION_SLEEP_PERIOD
        yt.config.OPERATION_GET_STATE_RETRY_COUNT = yt.config.OPERATION_GET_STATE_RETRY_COUNT

        yt.config.RETRY_READ = yt.config.RETRY_READ
        yt.config.USE_RETRIES_DURING_WRITE = yt.config.USE_RETRIES_DURING_WRITE
        yt.config.USE_RETRIES_DURING_UPLOAD = yt.config.USE_RETRIES_DURING_UPLOAD

        yt.config.CHUNK_SIZE = yt.config.CHUNK_SIZE

        yt.config.PYTHON_FUNCTION_SEARCH_EXTENSIONS = yt.config.PYTHON_FUNCTION_SEARCH_EXTENSIONS
        yt.config.PYTHON_FUNCTION_MODULE_FILTER = yt.config.PYTHON_FUNCTION_MODULE_FILTER
        yt.config.PYTHON_DO_NOT_USE_PYC = yt.config.PYTHON_DO_NOT_USE_PYC
        yt.config.PYTHON_CREATE_MODULES_ARCHIVE = yt.config.PYTHON_CREATE_MODULES_ARCHIVE

        yt.config.DETACHED = yt.config.DETACHED

        yt.config.format.TABULAR_DATA_FORMAT = yt.config.format.TABULAR_DATA_FORMAT

        yt.config.MEMORY_LIMIT = 1024 * 1024 * 1024
        yt.config.POOL = "pool"
        yt.config.INTERMEDIATE_DATA_ACCOUNT = "tmp"
        # Reset spec options
        yt.config["spec_defaults"] = {}

    @pytest.mark.usefixtures("config")
    def test_special_config_options(self, config):
        # Special shortcuts (manually backported)
        # MERGE_INSTEAD_WARNING
        yt.config.MERGE_INSTEAD_WARNING = True
        yt.config["auto_merge_output"]["action"] = "none"
        assert not yt.config.MERGE_INSTEAD_WARNING
        yt.config.MERGE_INSTEAD_WARNING = True
        assert yt.config["auto_merge_output"]["action"] == "merge"
        yt.config["auto_merge_output"]["action"] = "log"
        assert not yt.config.MERGE_INSTEAD_WARNING

        env_merge_option = os.environ.get("YT_MERGE_INSTEAD_WARNING", None)
        try:
            os.environ["YT_MERGE_INSTEAD_WARNING"] = "1"
            yt.config._update_from_env()
            assert yt.config["auto_merge_output"]["action"] == "merge"
            os.environ["YT_MERGE_INSTEAD_WARNING"] = "0"
            yt.config._update_from_env()
            assert yt.config["auto_merge_output"]["action"] == "log"
        finally:
            if env_merge_option is not None:
                os.environ["YT_MERGE_INSTEAD_WARNING"] = env_merge_option
            update(yt.config.config, config)

    def test_config_sanity(self):
        yt.write_table("//tmp/in", ["a=b\n"], format="dsv")

        old_format = yt.config["tabular_data_format"]
        yt.config.update_config({"tabular_data_format": yt.JsonFormat()})

        assert '{"a":"b"}\n' == yt.read_table("//tmp/in", raw=True).read()

        yt.config["tabular_data_format"] = old_format

