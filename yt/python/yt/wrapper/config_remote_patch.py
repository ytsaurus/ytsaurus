from .errors import YtResolveError
from .mappings import VerifiedDict  # noqa

import yt.logger as logger
import yt.yson as yson

import os
import random
import typing


class RemotePatchableValueBase(object):

    _REMOTE_CACHE = {}

    def __init__(self, default, remote_path, transform=None):
        # type: (any, str, typing.Callable[[any], any] | None) -> None
        self.value = default
        self.remote_path = remote_path
        self.transform = transform  # type: typing.Callable[[Any, Any], Any] | None
        self._callback = None  # type: typing.Callable[[], None] | None

    def __int__(self):
        self._call_back()
        return int(self.value)

    def __str__(self):
        self._call_back()
        return str(self.value)

    def __bool__(self):
        self._call_back()
        return bool(self.value)

    def __nonzero__(self):
        self._call_back()
        return bool(self.value)

    def __float__(self):
        self._call_back()
        return float(self.value)

    def __index__(self):
        self._call_back()
        return int(self.value)

    def _call_back(self):
        if self._callback:
            try:
                self._callback()
            except Exception as ex:
                logger.error("Can not apply patch to config. Error in callback - %s", ex)

    def __repr__(self):
        return "{} ({}".format(self.value, type(self))

    def _patch_value(self, remote_settings):
        # type: (YsonMap) -> None
        if isinstance(remote_settings, typing.Mapping) and self.remote_path in remote_settings:
            value = remote_settings[self.remote_path]
            if any(map(lambda t: isinstance(value, t), self._allowed_remote_type())) or value is None:
                if self.transform:
                    try:
                        self.value = self.transform(self.value, value)
                    except Exception as ex:
                        logger.error("Can not apply patch to config value. Error in transform function - %s", ex)
                else:
                    self.value = value

    @classmethod
    def _get_all_leafs(cls, config):
        # type: (VerifiedDict) -> list[RemotePatchableValueBase]
        out = []
        if hasattr(config, 'config'):
            config = config.config
        for v in config.values():
            if isinstance(v, cls):
                out.append(v)
            elif isinstance(v, typing.Mapping):
                out.extend(cls._get_all_leafs(v))
        return out

    @classmethod
    def _materialize_all_leafs(cls, config):
        # type: (VerifiedDict) -> None
        if hasattr(config, 'config'):
            config = config.config
        for k, v in config.items():
            if isinstance(v, cls):
                config[k] = v.value
            elif isinstance(v, typing.Mapping):
                cls._materialize_all_leafs(v)

    @classmethod
    def set_read_access_callback(cls, config, callback):
        # type: (VerifiedDict, typing.Callable[[], None]) -> None
        for v in cls._get_all_leafs(config):
            v._callback = callback

    @classmethod
    def _get_remote_cluster_data(cls, client):
        # type: (yt.wrapper.YtClient | yt.wrapper) -> dict | None
        if client.config["apply_remote_patch_at_start"] is None or os.environ.get("YT_APPLY_REMOTE_PATCH_AT_START") == "none":
            return None

        path = client.config["config_remote_patch_path"]
        cache_key = str(client.config["proxy"]["url"]) + str(path) if path else None

        if cache_key and cache_key in cls._REMOTE_CACHE:
            cluster_data = cls._REMOTE_CACHE[cache_key]
        elif path and client:
            try:
                cluster_data = client.get(client.config["config_remote_patch_path"], read_from="cache", attributes=["value"])
                if not isinstance(cluster_data, yson.yson_types.YsonMap):
                    # cache bad config
                    cluster_data = {}
                    logger.error("Can not get config patch from cluster. Wrong format - %s %s", client.config["config_remote_patch_path"], type(cluster_data))
            except YtResolveError:
                # cache bad path
                cluster_data = {}
                logger.debug("Skip remote config patch (cluster does not support this)")
            except Exception as ex:
                # do not cache error
                cluster_data = None
                logger.error("Can not get config patch from cluster - %s", ex)
        else:
            cluster_data = None

        if cluster_data is not None:
            cls._REMOTE_CACHE[cache_key] = cluster_data

        return cluster_data

    @classmethod
    def _get_remote_patch(cls, client):
        # type: (yt.wrapper.YtClient | yt.wrapper) -> dict | None
        cluster_data_full = cls._get_remote_cluster_data(client)

        if cluster_data_full:
            def _get_section(cluster_data, name):
                # type: (any, str) -> dict
                if isinstance(cluster_data, yson.YsonMap) \
                        and name in cluster_data \
                        and isinstance(cluster_data[name], yson.YsonEntity):
                    data = cluster_data[name].attributes.get("value")
                    if isinstance(data, yson.YsonMap):
                        return data
                return {}
            try:
                cluster_data = _get_section(cluster_data_full, "default")
                cluster_data_experiment = _get_section(cluster_data_full, "experiment_20")
                if cluster_data_experiment and random.randrange(100) < 20:
                    cluster_data.update(cluster_data_experiment)
                return cluster_data
            except Exception:
                return None
        else:
            return None

    @classmethod
    def patch_config_with_remote_data(cls, client, config=None):
        # type: (yt.wrapper.YtClient | yt.wrapper, VerifiedDict | None) -> None
        remote_patch = cls._get_remote_patch(client)
        for v in cls._get_all_leafs(config or client.config):
            v._patch_value(remote_patch)


class RemotePatchableString(RemotePatchableValueBase):
    def __eq__(self, other):
        return str(self) == str(other)

    def __ne__(self, other):
        return str(self) != str(other)

    def __contains__(self, item):
        return item in str(self)

    def to_yson_type(self):
        return yson.YsonUnicode(str(self))

    @staticmethod
    def _allowed_remote_type():
        return [str, yson.YsonUnicode, yson.YsonString, yson.YsonUnicode]

    def format(self, *args, **kwargs):
        return str.format(str(self), *args, **kwargs)


class RemotePatchableBoolean(RemotePatchableValueBase):
    def __eq__(self, other):
        return bool(self) == bool(other)

    def __ne__(self, other):
        return bool(self) != bool(other)

    def to_yson_type(self):
        return yson.YsonBoolean(bool(self))

    @staticmethod
    def _allowed_remote_type():
        return [bool, yson.YsonBoolean]


class RemotePatchableInteger(RemotePatchableValueBase):
    def _get_nullable_value(self, value=None):
        if value is None:
            self._call_back()
            value = self
        if isinstance(value, RemotePatchableValueBase):
            return int(value.value) if value.value is not None else None
        elif value is None:
            return None
        else:
            return int(value)

    def __eq__(self, other):
        return self._get_nullable_value() == self._get_nullable_value(other)

    def __ne__(self, other):
        return self._get_nullable_value() != self._get_nullable_value(other)

    def to_yson_type(self):
        if self.value is None:
            return yson.YsonEntity()
        return yson.YsonInt64(int(self))

    @staticmethod
    def _allowed_remote_type():
        return [int, yson.YsonInt64, yson.YsonUint64]


def _validate_operation_link_pattern(local_value, remote_value):
    ret = remote_value.replace("{operation_id}", "{id}").replace("{cluster_ui_host}", "{proxy}")
    try:
        ret.format(
            proxy="PROXY",
            cluster_path="CLUSTER_PATH",
            id="OPERATION"
        )
    except KeyError:
        raise RuntimeError("Wrong placeholder")
    return ret


def _validate_query_link_pattern(local_value, remote_value):
    ret = remote_value.replace("{query_id}", "{id}").replace("{cluster_ui_host}", "{proxy}")
    try:
        ret.format(
            proxy="PROXY",
            cluster_path="CLUSTER_PATH",
            id="QUERY"
        )
    except KeyError:
        raise RuntimeError("Wrong placeholder")
    return ret
