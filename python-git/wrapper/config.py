from __future__ import print_function

# TODO(asaitgalin): Remove try/except when DEVTOOLS-3781 is done.
try:
    from . import common
    from . import default_config
    from . import client_state
except ImportError:
    import common
    import default_config
    import client_state

import yt.yson as yson
import yt.json as json

import yt.packages.six as six

import os
import sys
import types

# NB: Magic!
# To support backward compatibility we must translate uppercase fields as config values.
# To implement this translation we replace config module with special class Config!

class Config(types.ModuleType, client_state.ClientState):
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
            "http.FORCE_IPV4": "proxy/force_ipv4",
            "http.FORCE_IPV6": "proxy/force_ipv6",

            "VERSION": "api_version",

            "DRIVER_CONFIG": "driver_config",
            "DRIVER_CONFIG_PATH": "driver_config_path",

            "USE_HOSTS": "proxy/enable_proxy_discovery",
            "HOSTS": "proxy/proxy_discovery_url",

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

            "FILE_STORAGE": "remote_temp_files_directory",
            "LOCAL_TMP_DIR": "local_temp_directory",

            # Deprecated
            "TEMP_TABLES_STORAGE": "remote_temp_tables_directory",
            # Non-deprecated version of TEMP_TABLES_STORAGE
            "TEMP_DIR": "remote_temp_tables_directory",

            "PREFIX": "prefix",

            "POOL": "pool",
            "MEMORY_LIMIT": "memory_limit",
            "SPEC": "spec_defaults",
            "TABLE_WRITER": "table_writer",

            "RETRY_READ": "read_retries/enable",
            "USE_RETRIES_DURING_WRITE": "write_retries/enable",
            "USE_RETRIES_DURING_UPLOAD": "write_retries/enable",

            "CHUNK_SIZE": "write_retries/chunk_size",

            "DETACHED": "detached",

            "format.TABULAR_DATA_FORMAT": "tabular_data_format",

            "CONFIG_PATH": "config_path",
            "CONFIG_FORMAT": "config_format",

            "ARGCOMPLETE_VERBOSE": "argcomplete_verbose",

            "USE_YAMR_DEFAULTS": "yamr_mode/use_yamr_defaults",
            "IGNORE_EMPTY_TABLES_IN_MAPREDUCE_LIST": "yamr_mode/ignore_empty_tables_in_mapreduce_list"
        }

        super(Config, self).__init__(__name__)
        client_state.ClientState.__init__(self)

        self.cls = Config
        self.__file__ = os.path.abspath(__file__)
        self.__path__ = [os.path.dirname(os.path.abspath(__file__))]
        self.__name__ = __name__
        if len(__name__.rsplit(".", 1)) > 1:
            self.__package__ = __name__.rsplit(".", 1)[0]
        else:
            self.__package__ = None

        self.default_config_module = default_config
        self.common_module = common
        self.json_module = json
        self.yson_module = yson
        self.client_state_module = client_state
        self.six_module = six

        self._init()
        self._update_from_env()

    def _init(self):
        self.client_state_module.ClientState.__init__(self)
        self.config = self.default_config_module.get_default_config()

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
            if var_type == dict:
                var_type = lambda obj: self.yson_module.json_to_yson(self.json_module.loads(obj)) if obj else {}
            return var_type

        def apply_type(type, key, value):
            try:
                return type(value)
            except ValueError:
                raise self.common_module.YtError("Incorrect value of option '{0}': failed to apply type '{1}' to '{2}'".format(key, type, value))

        if "YT_CONFIG_PATCHES" in os.environ:
            try:
                patches = self.yson_module._loads_from_native_str(os.environ["YT_CONFIG_PATCHES"],
                                                                  yson_type="list_fragment",
                                                                  always_create_attributes=False)
            except self.yson_module.YsonError:
                print("Failed to parse YT config patches from 'YT_CONFIG_PATCHES' environment variable", file=sys.stderr)
                raise

            try:
                for patch in reversed(list(patches)):
                    self.update_config(patch)
            except:
                print("Failed to apply config from 'YT_CONFIG_PATCHES' environment variable", file=sys.stderr)
                raise

        # These options should be processed before reading config file
        for opt_name in ["YT_CONFIG_PATH", "YT_CONFIG_FORMAT"]:
            if opt_name in os.environ:
                self.config[self.shortcuts[opt_name[3:]]] = os.environ[opt_name]

        config_path = self.config["config_path"]
        if config_path is None:
            home = None
            try:
                home = os.path.expanduser("~")
            except KeyError:
                pass

            config_path = "/etc/ytclient.conf"
            if home:
                home_config_path = os.path.join(os.path.expanduser("~"), ".yt/config")
                if os.path.isfile(home_config_path):
                    config_path = home_config_path

            try:
                open(config_path, "r")
            except IOError:
                config_path = None
        if config_path and os.path.isfile(config_path):
            load_func = None
            format = self.config["config_format"]
            if format == "yson":
                load_func = self.yson_module.load
            elif format == "json":
                load_func = self.json_module.load
            else:
                raise self.common_module.YtError("Incorrect config_format '%s'" % format)
            try:
                self.update_config(load_func(open(config_path, "rb")))
            except Exception:
                print("Failed to parse YT config from " + config_path, file=sys.stderr)
                raise

        old_options = sorted(list(self.shortcuts))
        old_options_short = [value.split(".")[-1] for value in old_options]

        for key, value in self.six_module.iteritems(os.environ):
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
            elif key == "TRACE":
                self.COMMAND_PARAMS["trace"] = common.bool_to_string(bool(value))
            elif key == "TRANSACTION":
                self.COMMAND_PARAMS["transaction_id"] = value
            elif key == "PING_ANCESTOR_TRANSACTIONS":
                self.COMMAND_PARAMS["ping_ancestor_transactions"] = bool(value)
            # Some shortcuts can't be backported one-to-one so they are processed manually
            elif key == "MERGE_INSTEAD_WARNING":
                self._set("auto_merge_output/action", "merge" if int(value) else "log")
            elif key == "CREATE_TABLES_UNDER_TRANSACTION":
                print(value, bool(int(value)), file=sys.stderr)
                self._set("yamr_mode/create_tables_outside_of_transaction", not bool(int(value)))


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
        return self.common_module.get_value(config["proxy"]["request_retry_timeout"],
                                            config["proxy"]["request_timeout"])

    def get_request_retry_count(self, client):
        config = self.get_config(client)
        #backend = self.get_backend_type(client)
        # TODO(ignat): support native backend.
        enable = self.common_module.get_value(config["proxy"]["request_retry_enable"],
                                              config["proxy"]["retries"]["enable"])
        if enable:
            return self.common_module.get_value(config["proxy"]["request_retry_count"],
                                                config["proxy"]["retries"]["count"])
        else:
            return 1

    def get_total_request_timeout(self, client):
        return self.get_single_request_timeout(client) * self.get_request_retry_count(client)

    def __getitem__(self, key):
        return self.config[key]

    def __setitem__(self, key, value):
        self.config[key] = value

    def update_config(self, patch):
        self.common_module.update(self.config, patch)

    def _check_deprecations(self, config):
        declare_deprecated = self.common_module.declare_deprecated

        declare_deprecated('config option "proxy/request_retry_timeout"',
                           config["proxy"]["request_retry_timeout"] is not None)
        declare_deprecated('config option "proxy/heavy_request_retry_timeout"',
                           config["proxy"]["heavy_request_retry_timeout"] is not None)
        declare_deprecated('config option "proxy/request_retry_enable"',
                           config["proxy"]["request_retry_enable"] is not None)
        declare_deprecated('config option "proxy/request_retry_count"',
                           config["proxy"]["request_retry_count"] is not None)
        declare_deprecated('config option "retry_backoff"',
                           config["retry_backoff"] != self.default_config_module.retry_backoff_config())

        declare_deprecated('config option "start_operation_retries/retry_count"',
                           config["start_operation_retries"]["retry_count"] is not None)
        declare_deprecated('config option "start_operation_retries/retry_timeout"',
                           config["start_operation_retries"]["retry_timeout"] is not None)

        declare_deprecated('config option "read_retries/retry_count"',
                           config["read_retries"]["retry_count"] is not None)

    def get_config(self, client):
        if client is not None:
            config = client.config
        else:
            config = self.config
        self._check_deprecations(config)
        return config

    def has_option(self, option, client):
        if client is not None:
            return option in client.__dict__
        else:
            return option in self.__dict__

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

    def get_command_param(self, param_name, client):
        command_params = self.get_option("COMMAND_PARAMS", client)
        return command_params.get(param_name)

    def set_command_param(self, param_name, value, client):
        command_params = self.get_option("COMMAND_PARAMS", client)
        command_params[param_name] = value
        self.set_option("COMMAND_PARAMS", command_params, client)

    def get_client_state(self, client):
        object = client if client is not None else self
        return super(type(object), object)

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

# Process reload correctly
special_module_name = "_yt_config_" + __name__
if special_module_name not in sys.modules:
    sys.modules[special_module_name] = Config()
else:
    sys.modules[special_module_name]._reload(ignore_env=False)

sys.modules[__name__] = sys.modules[special_module_name]


