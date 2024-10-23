from __future__ import print_function

from . import common
from . import default_config
from . import client_state

import yt.json_wrapper as json
import yt.logger as logger
import yt.yson as yson

import os
import sys
import types

# NB: Magic!
# To support backward compatibility we must translate uppercase fields as config values.
# To implement this translation we replace config module with special class Config!


class Config(types.ModuleType, client_state.ClientState):
    DEFAULT_PICKLING_FRAMEWORK = "dill"

    def __init__(self):
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
        self.logger_module = logger
        self.json_module = json
        self.yson_module = yson
        self.client_state_module = client_state
        self.config = None

        self._init()

    def _init(self):
        self.client_state_module.ClientState.__init__(self)
        self._init_from_env()
        self._init_from_cluster()

    def _init_from_env(self):
        import os

        if self.config is not None:
            self.config = self.default_config_module.update_config_from_env(self.config)
        else:
            self.config = self.default_config_module.update_config_from_env(
                self.default_config_module.get_default_config())

        # Update params from env.
        for key, value in os.environ.items():
            if not key.startswith("YT_"):
                continue

            if key == "YT_TRACE":
                self.COMMAND_PARAMS["trace"] = bool(value)
            elif key == "YT_TRANSACTION":
                self.COMMAND_PARAMS["transaction_id"] = value
            elif key == "YT_PING_ANCESTOR_TRANSACTIONS":
                self.COMMAND_PARAMS["ping_ancestor_transactions"] = bool(value)

    def _init_from_cluster(self):
        if self.config["apply_remote_patch_at_start"] is not None:
            self.default_config_module.RemotePatchableValueBase.set_read_access_callback(self.config, self.default_config_module._get_settings_from_cluster_callback)

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
                raise self.common_module.YtError("Cannot determine backend type: either driver config or proxy url "
                                                 "should be specified.")
        return backend

    def __getitem__(self, key):
        return self.config[key]

    def __setitem__(self, key, value):
        self.config[key] = value

    def update_config(self, patch):
        self.common_module.update_inplace(self.config, patch)

    def get_config(self, client):
        if client is not None:
            config = client.config
        else:
            config = self.config
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

    def del_command_param(self, param_name, client):
        command_params = self.get_option("COMMAND_PARAMS", client)
        if param_name in command_params:
            del command_params[param_name]
        self.set_option("COMMAND_PARAMS", command_params, client)

    def get_client_state(self, client):
        object = client if client is not None else self
        return super(type(object), object)

    def _reload(self, ignore_env):
        self._init()
        if not ignore_env:
            self._init_from_env()

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
