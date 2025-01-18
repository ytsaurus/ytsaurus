from enum import StrEnum
import logging

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable

from .base import ActionBase

LOG = logging.getLogger("yt_sync")


class ScheduledIteration(StrEnum):
    CLEAN_DIRS = "clean_dirs"
    ENSURE_DIRS_AND_FETCH_GUID = "ensure_dirs_and_fetch_guid"
    GET_DIRECTORY_ATTRS = "get_directory_attrs"
    ENSURE_DIRECTORY_ATTRS = "ensure_directory_attrs"
    END = "end"


class RegisterQueueExportAction(ActionBase):
    def __init__(self, desired_queue: YtTable, actual_queue: YtTable | None, clean: bool = False):
        super().__init__()

        self._desired_queue = desired_queue
        self._actual_queue = actual_queue

        if clean:
            self._schedule_iteration = ScheduledIteration.CLEAN_DIRS
        else:
            self._schedule_iteration = ScheduledIteration.ENSURE_DIRS_AND_FETCH_GUID
        self._simple_results = []
        self._queue_guid_result = None
        self._directory_results = {}

        self._desired_export_directories = None

        self._fetched_queue_guid = None
        self._fetched_directory_attrs = {}

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        desired_static_export_config = self._desired_queue.attributes.get("static_export_config")

        actual_static_export_config = {}
        if self._actual_queue is not None:
            actual_static_export_config = self._actual_queue.attributes.get("static_export_config")

        desired_export_directories = set()
        if desired_static_export_config is not None:
            desired_export_directories = set(v["export_directory"] for v in desired_static_export_config.values())
        self._desired_export_directories = desired_export_directories

        actual_export_directories = set()
        if actual_static_export_config is not None:
            actual_export_directories = set(v["export_directory"] for v in actual_static_export_config.values())

        if self._schedule_iteration == ScheduledIteration.CLEAN_DIRS:
            all_directories = actual_export_directories | desired_export_directories

            for d in all_directories:
                LOG.warning("Removing queue export directory %s:%s", self._desired_queue.cluster_name, d)
                self._simple_results.append(batch_client.remove(d, recursive=True, force=True))

            self._schedule_iteration = ScheduledIteration.ENSURE_DIRS_AND_FETCH_GUID
            return True
        if self._schedule_iteration == ScheduledIteration.ENSURE_DIRS_AND_FETCH_GUID:
            create_export_directories = desired_export_directories.difference(actual_export_directories)
            remove_export_directories = actual_export_directories.difference(desired_export_directories)

            LOG.debug("create_export_directories=%s", str(create_export_directories))
            LOG.debug("desired_export_directories=%s", str(desired_export_directories))
            LOG.debug("actual_export_directories=%s", str(actual_export_directories))

            for d in create_export_directories:
                LOG.warning("Create queue export directory %s:%s", self._desired_queue.cluster_name, d)
                self._simple_results.append(batch_client.create("map_node", d, recursive=True, ignore_existing=True))

            for d in remove_export_directories:
                LOG.warning("Remove queue export directory %s:%s", self._desired_queue.cluster_name, d)
                self._simple_results.append(batch_client.remove(d, recursive=True))

            self._queue_guid_result = batch_client.get(self._desired_queue.path + "/@id")

            self._schedule_iteration = ScheduledIteration.GET_DIRECTORY_ATTRS
            return True
        elif self._schedule_iteration == ScheduledIteration.GET_DIRECTORY_ATTRS:
            for d in self._desired_export_directories:
                attr_path = f"{d}/@"
                self._directory_results[d] = batch_client.get(attr_path, attributes=["queue_static_export_destination"])

            self._schedule_iteration = ScheduledIteration.ENSURE_DIRECTORY_ATTRS
            return True
        elif self._schedule_iteration == ScheduledIteration.ENSURE_DIRECTORY_ATTRS:
            desired_attrs = {"originating_queue_id": self._fetched_queue_guid}

            for d, attrs in self._fetched_directory_attrs.items():
                if attrs.get("queue_static_export_destination", {}) != desired_attrs:
                    attr_path = f"{d}/@queue_static_export_destination"
                    LOG.warning("Replace %s: %s -> %s", attr_path, str(attrs), str(desired_attrs))
                    self._simple_results.append(batch_client.set(attr_path, desired_attrs))

            self._schedule_iteration = ScheduledIteration.END
            return False
        else:
            raise ValueError("Can't call RegisterQueueExportAction.schedule_next() more than three times")

    def process(self):
        for result in self._simple_results:
            self.assert_response(result)
        self._simple_results = []

        if self._queue_guid_result is not None:
            self.assert_response(self._queue_guid_result)
            self._fetched_queue_guid = self._queue_guid_result.get_result()
        self._queue_guid_result = None

        for k, v in self._directory_results.items():
            if self.dry_run and not v.is_ok():
                # assuming this is because of uncreated directory, attributes should be empty
                self._fetched_directory_attrs[k] = {}
            else:
                self.assert_response(v)
                self._fetched_directory_attrs[k] = v.get_result()
        self._directory_results = {}
