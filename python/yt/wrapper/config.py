import common
import default_config

import yt.yson as yson
import yt.packages.simplejson as json

import os
import sys
import types

# NB: Magic!
# To support backward compatibility we must translate uppercase fields as config values.
# To implement this translation we replace config module with special class Config!

class Config(types.ModuleType):
    DEFAULT_PICKLING_FRAMEWORK = "dill"

    def __init__(self):
        self.shortcuts = {
            "http.PROXY": "proxy/url",
            "http.PROXY_SUFFIX": "proxy/default_suffix",
            "http.TOKEN": "token",
            "http.TOKEN_PATH": "token_path",
            "http.USE_TOKEN": "enable_token",
            "http.ACCEPT_ENCODING": "proxy/accept_encoding",
            "http.CONTENT_ENCODING": "proxy/content_encoding",
            "http.REQUEST_RETRY_TIMEOUT": "proxy/request_retry_timeout",
            "http.REQUEST_RETRY_COUNT": "proxy/request_retry_count",
            "http.REQUEST_BACKOFF": "proxy/request_backoff_time",
            "http.FORCE_IPV4": "proxy/force_ipv4",
            "http.FORCE_IPV6": "proxy/force_ipv6",
            "http.HEADER_FORMAT": "proxy/header_format",

            "VERSION": "api_version",
            "OPERATION_LINK_PATTERN": "proxy/operation_link_pattern",

            "DRIVER_CONFIG": "driver_config",
            "DRIVER_CONFIG_PATH": "driver_config_path",

            "USE_HOSTS": "proxy/enable_proxy_discovery",
            "HOSTS": "proxy/proxy_discovery_url",
            "HOST_BAN_PERIOD": "proxy/proxy_ban_timeout",

            "ALWAYS_SET_EXECUTABLE_FLAG_TO_FILE": "yamr_mode/always_set_executable_flag_on_files",
            "USE_MAPREDUCE_STYLE_DESTINATION_FDS": "yamr_mode/use_yamr_style_destination_fds",
            "TREAT_UNEXISTING_AS_EMPTY": "yamr_mode/treat_unexisting_as_empty",
            "DELETE_EMPTY_TABLES": "yamr_mode/delete_empty_tables",
            "USE_YAMR_SORT_REDUCE_COLUMNS": "yamr_mode/use_yamr_sort_reduce_columns",
            "REPLACE_TABLES_WHILE_COPY_OR_MOVE": "yamr_mode/replace_tables_on_copy_and_move",
            "CREATE_RECURSIVE": "yamr_mode/create_recursive",
            "THROW_ON_EMPTY_DST_LIST": "yamr_mode/throw_on_missing_destination",
            "RUN_MAP_REDUCE_IF_SOURCE_IS_NOT_SORTED": "yamr_mode/run_map_reduce_if_source_is_not_sorted",
            "USE_NON_STRICT_UPPER_KEY": "yamr_mode/use_non_strict_upper_key",
            "CHECK_INPUT_FULLY_CONSUMED": "yamr_mode/check_input_fully_consumed",
            "FORCE_DROP_DST": "yamr_mode/abort_transactions_with_remove",
            "USE_YAMR_STYLE_PREFIX": "yamr_mode/use_yamr_style_prefix",

            "OPERATION_STATE_UPDATE_PERIOD": "operation_tracker/poll_period",
            "STDERR_LOGGING_LEVEL": "operation_tracker/stderr_logging_level",
            "IGNORE_STDERR_IF_DOWNLOAD_FAILED": "operation_tracker/ignore_stderr_if_download_failed",
            "KEYBOARD_ABORT": "operation_tracker/abort_on_sigint",

            "READ_BUFFER_SIZE": "read_buffer_size",

            "FILE_STORAGE": "remote_temp_files_directory",
            "TEMP_TABLES_STORAGE": "remote_temp_tables_directory",
            "LOCAL_TMP_DIR": "local_temp_directory",
            "REMOVE_TEMP_FILES": "clear_local_temp_files",

            "MIN_CHUNK_COUNT_FOR_MERGE_WARNING": "auto_merge_output/min_chunk_count",
            "MAX_CHUNK_SIZE_FOR_MERGE_WARNING": "auto_merge_output/max_chunk_size",

            "PREFIX": "prefix",

            "POOL": "spec_defaults/pool",
            "MEMORY_LIMIT": "memory_limit",
            "INTERMEDIATE_DATA_ACCOUNT": "spec_defaults/intermediate_data_account",

            "TRANSACTION_TIMEOUT": "transaction_timeout",
            "TRANSACTION_SLEEP_PERIOD": "transaction_sleep_period",
            "OPERATION_GET_STATE_RETRY_COUNT": "proxy/operation_state_discovery_retry_count",

            "RETRY_READ": "read_retries/enable",
            "USE_RETRIES_DURING_WRITE": "write_retries/enable",
            "USE_RETRIES_DURING_UPLOAD": "write_retries/enable",

            "CHUNK_SIZE": "write_retries/chunk_size",

            "PYTHON_FUNCTION_SEARCH_EXTENSIONS": "pickling/search_extensions",
            "PYTHON_FUNCTION_MODULE_FILTER": "pickling/module_filter",
            "PYTHON_DO_NOT_USE_PYC": "pickling/force_using_py_instead_of_pyc",
            "PYTHON_CREATE_MODULES_ARCHIVE": "pickling/create_modules_archive_function",

            "DETACHED": "detached",

            "format.TABULAR_DATA_FORMAT": "tabular_data_format",

            "CONFIG_PATH": "config_path",
            "CONFIG_FORMAT": "config_format"
        }

        # Some shortcuts can't be backported one-to-one so they are processed manually
        self.special_shortcuts = {"MERGE_INSTEAD_WARNING": int}

        super(Config, self).__init__(__name__)

        self.cls = Config
        self.__file__ = os.path.abspath(__file__)
        self.__path__ = [os.path.dirname(os.path.abspath(__file__))]
        self.__name__ = __name__
        if len(__name__.rsplit(".", 1)) > 1:
            self.__package__ = __name__.rsplit(".", 1)[0]
        else:
            self.__package__ = None

        for key in self.shortcuts:
            obj = self
            cls = Config
            parts = key.split(".")
            for part in parts[:-1]:
                if not hasattr(cls, part):
                    setattr(cls, part, type(part, (object,), {}))
                    setattr(obj, part, getattr(cls, part)())
                cls = getattr(cls, part)
                obj = getattr(obj, part)
            setattr(cls, parts[-1],
                property(lambda obj_self, key=key: self._get(self.shortcuts[key]))
                    .setter(lambda obj_self, value, key=key: self._set(self.shortcuts[key], value)))
        # MERGE_INSTEAD_WARNING shortcut is processed manually
        self._process_merge_instead_warning_shortcut()

        self.default_config_module = default_config
        self.common_module = common
        self.json_module = json
        self.yson_module = yson

        self._init()
        self._update_from_env()

    def _init(self):
        self.config = self.default_config_module.get_default_config()

        self.CLIENT = None
        self.RETRY = None
        self.MUTATION_ID = None
        self.TRACE = None
        self.SPEC = None
        self.TRANSACTION = "0-0-0-0"
        self.PING_ANCESTOR_TRANSACTIONS = False
        self._ENABLE_READ_TABLE_CHAOS_MONKEY = False
        self._ENABLE_HTTP_CHAOS_MONKEY = False
        self._ENABLE_HEAVY_REQUEST_CHAOS_MONKEY = False

        self._env_configurable_options = ["TRACE", "TRANSACTION", "PING_ANCESTOR_TRANSACTIONS", "SPEC"]

        self._transaction_stack = None
        self._banned_proxies = {}
        self._ip_configured = False

        # Cache for API version (to check it only once)
        self._api_version = None
        self._commands = None


    def _update_from_env(self):
        import os

        def get_var_type(value):
            var_type = type(value)
            # Using int we treat "0" as false, "1" as "true"
            if var_type == bool:
                try:
                    value = int(value)
                except:
                    pass
            # None type is treated as str
            if isinstance(None, var_type):
                var_type = str
            return var_type

        def apply_type(type, key, value):
            try:
                return type(value)
            except ValueError:
                raise self.common_module.YtError("Incorrect value of option '{0}': failed to apply type '{1}' to '{2}'".format(key, type, value))

        # These options should be processed before reading config fil
        for opt_name in ["YT_CONFIG_PATH", "YT_CONFIG_FORMAT"]:
            if opt_name in os.environ:
                self.config[self.shortcuts[opt_name]] = os.environ[opt_name]

        config_path = self.config["config_path"]
        if config_path is None:
            config_path = os.path.join(os.path.expanduser("~"), ".yt/config")
        if os.path.isfile(config_path):
            load_func = None
            format = self.config["config_format"]
            if format == "yson":
                load_func = self.yson_module.load
            elif format == "json":
                load_func = self.json_module.load
            else:
                raise self.common_module.YtError("Incorrect config_format '%s'" % format)
            try:
                self.update_config(load_func(open(config_path)))
            except Exception:
                print >>sys.stderr, "Failed to parse YT config from " + config_path
                raise

        old_options = sorted(list(self.shortcuts))
        old_options_short = [value.split(".")[-1] for value in old_options]

        for key, value in os.environ.iteritems():
            prefix = "YT_"
            if not key.startswith(prefix):
                continue

            key = key[len(prefix):]
            if key in old_options_short:
                name = self.shortcuts[old_options[old_options_short.index(key)]]
                var_type = get_var_type(self._get(name))
                #NB: it is necessary to set boolean vaiable as 0 or 1
                if var_type is bool:
                    value = int(value)
                self._set(name, apply_type(var_type, key, value))
            elif key in self._env_configurable_options:
                var_type = get_var_type(self.__dict__[key])
                self.__dict__[key] = apply_type(var_type, key, value)
            elif key in self.special_shortcuts:
                setattr(self, key, self.special_shortcuts[key](value))


    # NB: Method required for compatibility
    def set_proxy(self, value):
        self._set("proxy/url", value)


    # Helpers
    def get_backend_type(self, client):
        config = self.get_config(client)
        backend = config["backend"]
        if backend is None:
            if config["proxy"]["url"] is not None:
                backend = "http"
            elif config["driver_config"] is not None or config["driver_config_path"] is not None:
                backend = "native"
            else:
                raise self.common_module.YtError("Cannot determine backend type: either driver config or proxy url should be specified.")
        return backend

    def get_single_request_timeout(self, client):
        config = self.get_config(client)
        #backend = self.get_backend_type(client)
        # TODO(ignat): support native backend.
        return config["proxy"]["request_retry_timeout"]

    def get_request_retry_count(self, client):
        config = self.get_config(client)
        #backend = self.get_backend_type(client)
        # TODO(ignat): support native backend.
        return config["proxy"]["request_retry_count"]

    def get_total_request_timeout(self, client):
        return self.get_single_request_timeout(client) * self.get_request_retry_count(client)

    def __getitem__(self, key):
        return self.config[key]

    def __setitem__(self, key, value):
        self.config[key] = value

    def update_config(self, patch):
        self.common_module.update(self.config, patch)

    def get_config(self, client):
        if client is not None:
            return client.config
        elif self.CLIENT is not None:
            return self.CLIENT.config
        else:
            return self.config

    def get_option(self, option, client):
        if client is not None:
            return client.__dict__[option]
        else:
            return self.__dict__[option]

    def set_option(self, option, value, client):
        if client is not None:
            client.__dict__[option] = value
        else:
            self.__dict__[option] = value

    def _reload(self, ignore_env):
        self._init()
        if not ignore_env:
            self._update_from_env()

    def _get(self, key):
        d = self.config
        parts = key.split("/")
        for k in parts:
            d = d.get(k)
        return d

    def _set(self, key, value):
        d = self.config
        parts = key.split("/")
        for k in parts[:-1]:
            d = d[k]
        d[parts[-1]] = value

    def _process_merge_instead_warning_shortcut(self):
        modern_path = "auto_merge_output/action"
        getter = lambda obj_self: self._get(modern_path) == "merge"
        setter = lambda obj_self, value: self._set(modern_path, "merge" if value else "log")
        setattr(self.cls, "MERGE_INSTEAD_WARNING", property(getter, setter))

# Process reload correctly
special_module_name = "_yt_config_" + __name__
if special_module_name not in sys.modules:
    sys.modules[special_module_name] = Config()
else:
    sys.modules[special_module_name]._reload(ignore_env=False)

sys.modules[__name__] = sys.modules[special_module_name]


