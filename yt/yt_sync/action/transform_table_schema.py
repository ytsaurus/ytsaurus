from abc import abstractmethod
from copy import deepcopy
import logging
from typing import Any
from typing import Callable
from typing import ClassVar

from yt.wrapper import YPath
from yt.yt_sync.core import Settings
from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtSchema
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model.helpers import make_tmp_name

from .base import LongOperationActionBase

LOG = logging.getLogger("yt_sync")


# TODO: tests!
class TransformTableSchemaActionBase(LongOperationActionBase):
    _STATE_INIT: ClassVar[int] = 0
    _STATE_DONE: ClassVar[int] = 1
    _STATE_CREATE_TMP: ClassVar[int] = 2
    _STATE_TRANSFORM: ClassVar[int] = 3
    _STATE_AWAIT_MAP: ClassVar[int] = 4
    _STATE_SORT: ClassVar[int] = 5
    _STATE_AWAIT_SORT: ClassVar[int] = 6
    _STATE_ALTER_TMP: ClassVar[int] = 7
    _STATE_COPY_ATTRS: ClassVar[int] = 8

    def __init__(self, settings: Settings, desired_table: YtTable, actual_table: YtTable, tmp_path: str):
        assert desired_table.cluster_name == actual_table.cluster_name
        assert desired_table.path == actual_table.path
        assert YtTable.Type.TABLE == desired_table.table_type
        assert YtTable.Type.TABLE == actual_table.table_type

        super().__init__(settings)
        self._desired_table: YtTable = desired_table
        self._actual_table: YtTable = actual_table
        self._tmp_path: str = tmp_path

        self._state: int = self._STATE_INIT
        self._result: Any | None = None
        self._operation: Any | None = None
        self._responses: list[Any] = list()

    def schedule_next(self, batch_client: YtClientProxy) -> bool:
        if self._STATE_DONE == self._state:
            return False
        elif self._STATE_INIT == self._state:
            assert self._actual_table.exists, "Can't transform non-existing table"
            if self._actual_table.schema == self._desired_table.schema:
                LOG.info(
                    "Skip transform schema, %s %s already has desired schema=%s",
                    self._actual_table.table_type,
                    self._actual_table.rich_path,
                    self._actual_table.schema.yt_schema,
                )
                self._state = self._STATE_DONE
                return False

            LOG.warning(
                "Transform %s %s schema: actual=%s, desired=%s",
                self._actual_table.table_type,
                self._actual_table.rich_path,
                self._actual_table.schema.yt_schema,
                self._desired_table.schema.yt_schema,
            )
            yt_client = batch_client.underlying_client_proxy
            assert yt_client
            if yt_client.exists(self._tmp_path):
                LOG.warning("Remove existing temporary table %s:%s", self._actual_table.cluster_name, self._tmp_path)
                yt_client.remove(self._tmp_path)
            self._result = batch_client.create(
                self._desired_table.table_type,
                self._tmp_path,
                recursive=True,
                attributes=self._make_tmp_table_attributes(),
            )
            self._state = self._STATE_CREATE_TMP
            return True
        elif self._STATE_TRANSFORM == self._state:
            yt_client = batch_client.underlying_client_proxy
            assert yt_client
            ts = yt_client.generate_timestamp()
            self._operation = yt_client.run_map(
                self._create_mapper(),
                self._actual_table.path,
                YPath(self._tmp_path, attributes={"output_timestamp": ts}),
                sync=False,
                ordered=not self._is_sort_needed(),
                spec=self.settings.get_operation_spec(batch_client.cluster, "map"),
            )
            self._state = self._STATE_AWAIT_MAP
            self._result = None
            return True
        elif self._state in (self._STATE_AWAIT_MAP, self._STATE_AWAIT_SORT):
            assert self._operation
            self._result = batch_client.get_operation(self._operation.id)
            return True
        elif self._STATE_SORT == self._state:
            yt_client = batch_client.underlying_client_proxy
            assert yt_client
            self._operation = yt_client.run_sort(
                self._tmp_path,
                YPath(self._tmp_path, attributes={"schema": self._desired_table.schema.yt_schema}),
                sort_by=[c.name for c in self._desired_table.schema.columns if c.sort_order],
                sync=False,
                spec=self.settings.get_operation_spec(batch_client.cluster, "sort"),
            )
            self._state = self._STATE_AWAIT_SORT
            return True
        elif self._STATE_ALTER_TMP == self._state:
            self._result = batch_client.alter_table(self._tmp_path, dynamic=True)
            return True
        elif self._STATE_COPY_ATTRS == self._state:
            LOG.debug("Set desired attributes to %s:%s", self._actual_table.cluster_name, self._tmp_path)
            for key, value in self._tmp_table_attributes().items():
                if value is not None:
                    self._responses.append(batch_client.set(f"{self._tmp_path}/@{key}", value))
            LOG.debug("Set user attributes to %s:%s", self._actual_table.cluster_name, self._tmp_path)
            for key in self._get_missing_user_attrs():
                value = self._actual_table.attributes.get_filtered(key)
                if isinstance(value, bool) or value:
                    self._responses.append(batch_client.set(f"{self._tmp_path}/@{key}", value))
            return True
        else:
            return self.schedule_additional_actions(batch_client)

    @abstractmethod
    def schedule_additional_actions(self, batch_client: YtClientProxy) -> bool:
        raise NotImplementedError()

    def process(self):
        assert self._STATE_INIT != self._state
        if self._STATE_DONE == self._state:
            return
        elif self._STATE_CREATE_TMP == self._state:
            self.assert_response(self._result)
            self._result = None
            self._state = self._STATE_TRANSFORM
            return
        elif self._STATE_TRANSFORM == self._state:
            return
        elif self._STATE_AWAIT_MAP == self._state:
            if self._is_operation_completed("map"):
                self._operation = None
                self._result = None
                if self._is_sort_needed():
                    self._state = self._STATE_SORT
                else:
                    self._state = self._STATE_ALTER_TMP
            return
        elif self._STATE_AWAIT_SORT == self._state:
            if self._is_operation_completed("sort"):
                self._operation = None
                self._result = None
                self._state = self._STATE_ALTER_TMP
            return
        elif self._STATE_ALTER_TMP == self._state:
            self.assert_response(self._result)
            self._result = None
            self._state = self._STATE_COPY_ATTRS
            return
        elif self._STATE_COPY_ATTRS == self._state:
            for r in self._responses:
                self.assert_response(r)
            self._responses.clear()
            self._copy_tablet_info()
        self.process_additional()

    @abstractmethod
    def process_additional(self):
        raise NotImplementedError()

    def _make_tmp_table_attributes(self) -> Types.Attributes:
        new_schema: YtSchema = (
            self._desired_table.schema.to_unsorted() if self._is_sort_needed() else self._desired_table.schema
        )
        attributes: Types.Attributes = {"schema": new_schema.yt_schema}
        primary_medium = self._desired_table.attributes.get("primary_medium")
        if primary_medium:
            attributes["primary_medium"] = primary_medium
        return attributes

    def _tmp_table_attributes(self) -> Types.Attributes:
        attrs = deepcopy(self._desired_table.yt_attributes)
        attrs.pop("dynamic", None)
        attrs.pop("schema", None)
        attrs.pop("replicated_table_options", None)
        attrs.pop("replication_throttler", None)
        return attrs

    def _create_mapper(self) -> Callable:
        expected_fields = set([c.name for c in self._desired_table.schema.columns if c.expression is None])

        def _mapper(row):
            fields_to_remove = row.keys() - expected_fields
            for f in fields_to_remove:
                row.pop(f, None)
            yield row

        return _mapper

    def _is_operation_completed(self, op_name: str) -> bool:
        if self.dry_run:
            return True
        if self._result is None:
            return False
        self.assert_response(self._result)
        operation = self._result.get_result()
        operation_state = operation["state"]
        assert operation_state not in (
            "aborted",
            "failed",
        ), (
            f"Operation '{op_name}' failed with state '{operation_state}', "
            + f"details {operation.get('result', {}).get('error', {})}"
        )
        return "completed" == operation_state

    def _is_sort_needed(self) -> bool:
        return self._actual_table.schema.is_key_changed_with_data_modification(
            self._desired_table.schema
        ) or self._actual_table.schema.is_change_with_downtime(self._desired_table.schema)

    def _is_need_copy_attrs(self) -> bool:
        return len(self._get_missing_user_attrs()) > 0

    def _get_missing_user_attrs(self) -> frozenset[str]:
        return self._actual_table.attributes.user_attribute_keys - self._desired_table.attributes.user_attribute_keys

    def _copy_tablet_info(self):
        self._desired_table.tablet_info = deepcopy(self._actual_table.tablet_info)
        if self._is_sort_needed():
            self._desired_table.tablet_info.pivot_keys = None
            LOG.debug(
                "Use tablet count after schema transform: table %s, tablet_count: %s",
                self._desired_table.rich_path,
                self._desired_table.tablet_info.tablet_count,
            )
        else:
            LOG.debug(
                "Use pivot keys after schema transform: table %s, pivot_keys: %s",
                self._desired_table.rich_path,
                self._desired_table.tablet_info.pivot_keys,
            )


class TransformTableSchemaAction(TransformTableSchemaActionBase):
    _STATE_MOVE_TMP: ClassVar[int] = 9

    def __init__(self, desired_table: YtTable, actual_table: YtTable, settings: Settings):
        super().__init__(settings, desired_table, actual_table, make_tmp_name(actual_table.path))

    def schedule_additional_actions(self, batch_client: YtClientProxy) -> bool:
        if self._STATE_MOVE_TMP == self._state:
            self._result = batch_client.move(self._tmp_path, self._actual_table.path, force=True)
        return False

    def process_additional(self):
        if self._STATE_COPY_ATTRS == self._state:
            self._state = self._STATE_MOVE_TMP
            return
        elif self._STATE_MOVE_TMP == self._state:
            self.assert_response(self._result)
            self._actual_table.schema = deepcopy(self._desired_table.schema)
            self._state = self._STATE_DONE


class ChaosTransformTableSchemaAction(TransformTableSchemaActionBase):
    def __init__(self, settings: Settings, desired: YtTable, actual_from: YtTable, actual_to: YtTable):
        assert YtTable.Type.TABLE == actual_to.table_type
        assert actual_from.cluster_name == actual_to.cluster_name
        assert actual_from.path != actual_to.path

        super().__init__(settings, desired, actual_from, actual_to.path)
        self._actual_to: YtTable = actual_to

    def schedule_additional_actions(self, batch_client: YtClientProxy) -> bool:
        return False

    def process_additional(self):
        if self._STATE_COPY_ATTRS == self._state:
            self._actual_to.exists = True
            self._state = self._STATE_DONE
