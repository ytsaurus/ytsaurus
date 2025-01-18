import logging
from typing import Any
from typing import Generator

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model import YtTableAttributes
from yt.yt_sync.core.yt_commands import make_yt_commands

from .base import ActionBase

LOG = logging.getLogger("yt_sync")


class AlterTableAttributesAction(ActionBase):
    def __init__(self, desired_table: YtTable, actual_table: YtTable):
        assert desired_table.cluster_name == actual_table.cluster_name
        assert desired_table.path == actual_table.path
        assert actual_table.exists

        super().__init__()
        self._desired_table = desired_table
        self._actual_table = actual_table
        self._scheduled: bool = False
        self._responses: list[Any] = list()

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        assert not self._scheduled, "Can't call schedule_next() more than one time"
        self._scheduled = True

        attribute_setter = make_yt_commands(self._actual_table.is_chaos_replicated).get_attribute_setter(
            self._actual_table
        )

        for path, desired_value, actual_value in self._changed_attrs():
            full_path = attribute_setter.attribute_path(path)
            if desired_value is None:
                LOG.warning(
                    "Remove attribute %s:%s: actual=%s", self._actual_table.cluster_name, full_path, actual_value
                )
                attribute_setter.remove(path)
            else:
                filtered_desired_value = YtTableAttributes.filtered_value(desired_value)
                if filtered_desired_value is not None:
                    LOG.warning(
                        "Alter attribute %s:%s: actual=%s, desired=%s",
                        self._actual_table.cluster_name,
                        full_path,
                        actual_value,
                        filtered_desired_value,
                    )
                    attribute_setter.set(path, filtered_desired_value)
        self._responses.extend(attribute_setter.apply_changes(batch_client))
        return False

    def process(self):
        assert self._scheduled, "Can't call process before schedule"
        for response in self._responses:
            self.assert_response(response)
        for path, desired_value, _ in self._changed_attrs():
            if desired_value is None:
                self._actual_table.attributes.remove_value(path)
            else:
                self._actual_table.attributes.set_value(path, desired_value)

    def _changed_attrs(self) -> Generator[tuple[str, Any | None, Any | None], None, None]:
        desired_attrs = self._desired_table.attributes
        actual_attrs = self._actual_table.attributes

        yield from desired_attrs.changed_attributes(actual_attrs)
