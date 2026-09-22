from .orchid_client import OrmOrchidClient

from yt.wrapper.ypath import ypath_join
from yt.wrapper.errors import YtResolveError

import yt.common
from yt.common import update

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

    def get_config_patch(self, inner_path=None):
        try:
            if inner_path is not None:
                return self._yt_client.get(ypath_join(self._config_path, inner_path))
            return self._yt_client.get(self._config_path)
        except YtResolveError:
            return dict()

    def get_applied_config_patch(self, instance=None):
        if instance is None:
            instance = self._orchid_client.get_leader_instance()

        try:
            return self._orchid_client.get_at_instance(
                instance,
                "dynamic_config_manager/raw_config_patch",
            )
        except YtResolveError:
            return dict()

    def get_effective_config(self, inner_path=None, instance=None):
        if instance is None:
            instance = self._orchid_client.get_leader_instance()
        if inner_path is None:
            return self._orchid_client.get_at_instance(instance, "config")

        try:
            return self._orchid_client.get_at_instance(instance, ypath_join("config", inner_path))
        except YtResolveError:
            # Will throw if there's no "config" node at all.
            self._orchid_client.get_at_instance(instance, "config")
            return dict()

    def set_config_patch(self, value, inner_path=None, wait=WaitType.All, type="document"):
        effective_path = self._config_path
        if inner_path and inner_path != "/":
            effective_path = ypath_join(effective_path, inner_path)

        if value is None or value == dict():
            self._yt_client.remove(effective_path, recursive=True, force=True)
        elif effective_path == self._config_path:
            self._yt_client.create(type, effective_path, attributes=dict(value=value), force=True)
        else:
            self._yt_client.set(effective_path, value, recursive=True)

        self._wait_until_config_patch_is_applied(wait)

    def update_config_patch(self, patch, wait=WaitType.All, type="document"):
        current_patch = self.get_config_patch()
        self.set_config_patch(value=update(current_patch, patch), wait=wait, type=type)

    def reset_config_patch(self, wait=WaitType.All):
        self.set_config_patch(value=None, wait=wait)

    @contextmanager
    def with_config_patch(self, value, wait=WaitType.All, type="document"):
        old_patch = self.get_config_patch()
        try:
            self.set_config_patch(value=value, wait=wait, type=type)
            yield
        finally:
            self.set_config_patch(value=old_patch, wait=wait, type=type)

    @contextmanager
    def with_config_patch_updated(self, patch, wait=WaitType.All, type="document"):
        old_patch = self.get_config_patch()
        try:
            self.update_config_patch(patch=patch, wait=wait, type=type)
            yield
        finally:
            self.set_config_patch(value=old_patch, wait=wait, type=type)

    def _wait_until_config_patch_is_applied(self, wait):
        if wait == WaitType.Nothing:
            return

        target_patch = self.get_config_patch()
        if wait == WaitType.Leader:
            yt.common.wait(lambda: self.get_applied_config_patch() == target_patch)
            return

        assert wait == WaitType.All
        for instance in self._orchid_client.list_instances():
            yt.common.wait(lambda: self.get_applied_config_patch(instance) == target_patch)
