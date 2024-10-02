from .orchid_client import OrmOrchidClient

from yt.wrapper.ypath import ypath_join
from yt.wrapper.errors import YtResolveError

from yt.common import update, wait

from contextlib import contextmanager
from enum import Enum


class WaitType(Enum):
    Nothing = 0
    Leader = 1
    All = 2


class OrmDynamicConfig(object):
    def __init__(self, yt_client, orm_path, service="master"):
        self._yt_client = yt_client
        self._config_path = ypath_join(orm_path, service, "config")
        self._orchid_client = OrmOrchidClient(yt_client, orm_path, service)

    def is_usable(self):
        instance = self._orchid_client.get_leader_instance(throw_if_no_leader=False)
        return instance is not None and self._orchid_client.exists_at_instance(instance, "config")

    def _wait_for_config(self, wait_for, expected_value):
        if wait_for == WaitType.All:
            instances = self._orchid_client.list_instances()
            assert instances
        elif wait_for == WaitType.Leader:
            instances = [self._orchid_client.get_leader_instance()]

        for instance in instances:
            wait(lambda: self.get_effective_config(instance=instance) == expected_value)

    def get_effective_config(self, inner_path=None, instance=None):
        if instance is None:
            instance = self._orchid_client.get_leader_instance()
        if inner_path is None:
            return self._orchid_client.get_at_instance(instance, "config")

        try:
            return self._orchid_client.get_at_instance(instance, "config/" + inner_path)
        except YtResolveError:
            # Will throw if there's no "config" at all.
            self._orchid_client.get_at_instance(instance, "config")
            return None

    def get_config_patches(self, inner_path=None):
        try:
            if inner_path is not None:
                return self._yt_client.get(ypath_join(self._config_path, inner_path))
            return self._yt_client.get(self._config_path)
        except YtResolveError:
            return None

    def set_config(self, inner_path, value, wait=WaitType.All, type="document"):
        if inner_path is not None and inner_path != "/":
            self._yt_client.set(ypath_join(self._config_path, inner_path), value, recursive=True)
            value = self.get_config_patches()
        else:
            self._yt_client.create(type, self._config_path, attributes=dict(value=value), force=True)

        if wait == WaitType.Nothing:
            return

        initial_config = self._orchid_client.get("initial_config")
        patched_config = update(initial_config, value)

        self._wait_for_config(wait, patched_config)

    def update_config(self, patch, wait=WaitType.All):
        try:
            current_config = self._yt_client.get(self._config_path)
        except YtResolveError:
            current_config = dict()
        self.set_config(None, update(current_config, patch), wait)

    def reset_config_patches(self, wait=WaitType.All):
        self._yt_client.remove(self._config_path, force=True, recursive=True)

        if wait == WaitType.Nothing:
            return

        initial_config = self._orchid_client.get("initial_config")
        self._wait_for_config(wait, initial_config)

    @contextmanager
    def with_config(self, value, wait=WaitType.All, type="document"):
        try:
            self.set_config(None, value, wait, type)
            yield
        finally:
            self.reset_config_patches(wait)
