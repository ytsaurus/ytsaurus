"""Helper class for generating action plans ommiting configuration details."""

from __future__ import annotations

import copy
import functools
from typing import Any, Callable, Sequence
from typing_extensions import override
import uuid

from yt.environment import migrationlib
import yt.sequoia_tools as yt_sequoia
import yt.wrapper as yt

from . import actions, app as sequoia_app, config as cfg, descriptors, helpers, pivots, utils

import logging
logger = logging.getLogger(__name__)


def _get_sequoia_table_descriptors(
    scope: cfg.Scope,
    version: int,
) -> dict[str, descriptors.TableDescriptor]:
    """Return table descriptors from the parsed versioned registry."""
    group_names = cfg.SCOPE_GROUPS[scope]
    tds = descriptors.get_table_descriptors(version).as_dict()
    return {k: v for k, v in tds.items() if v.group in group_names}


class ComponentContext:
    """Context providing component-scoped configuration and helpers."""

    def __init__(
        self,
        scope: cfg.Scope,
        component_config: cfg.SequoiaComponentConfig,
        ground_config: cfg.GroundClusterConfig,
    ):
        self.scope = scope
        self.config = component_config
        self.ground_config = ground_config

    def get_table_attributes(self, table_descriptor: yt_sequoia.TableDescriptor) -> dict[str, Any]:
        """Build standard table attributes for this component."""
        attributes = self.config.get_table_attributes(table_descriptor.name)
        attributes.update(
            account=self.ground_config.account,
            tablet_cell_bundle=self.config.tablet_cell_bundle)
        return attributes


class TableContext:
    """Context providing table-scoped information."""

    def __init__(
        self,
        parent_path: str,
        descriptor: yt_sequoia.TableDescriptor,
        attributes: dict[str, Any],
        component_context: ComponentContext,
        version: int,
        pivot_keys: list | None = None,
    ):
        self.name = descriptor.name
        self.parent_path = parent_path
        self.descriptor = descriptor
        self.attributes = attributes
        self.component_context = component_context
        self.version = version
        self.pivot_keys = pivot_keys

    @property
    def path(self) -> str:
        return yt.ypath_join(self.parent_path, self.name)

    @property
    def logical_name(self) -> str:
        return self.descriptor.name

    def __str__(self) -> str:
        return f"{self.name}[v{self.version}]"


class TableBuilder:
    """Builder for table-scoped operations."""

    def __init__(
        self,
        table_contexts: list[TableContext],
        parent_builder: ActionBuilder,
    ):
        self._table_contexts = table_contexts
        self._parent_builder = parent_builder

    def with_table_factory(
        self,
        factory: Callable[[TableContext], Sequence[actions.Action]]
    ) -> TableBuilder:
        """Add actions from a factory with table context.."""
        for table_context in self._table_contexts:
            action_list = factory(table_context)
            self._parent_builder._actions.extend(action_list)
        return self

    def then(self) -> ActionBuilder:
        """Return to root scope."""
        return self._parent_builder


class ActionBuilder:
    """Root builder for creating action plans."""

    def __init__(
        self,
        app: sequoia_app.SequoiaTool,
        name: str,
        version: int,
    ) -> None:
        self._app = app
        self._name = name
        self._version = version
        self._actions: list[actions.Action] = []
        self._ground_config = app.config_provider.get_ground_config()

    @functools.cached_property
    def _cell_tags(self) -> list[str]:
        return helpers.list_master_cell_tags(self._app.remote_client)

    def _maybe_expand_bundled_table(self, ctx: TableContext) -> list[TableContext] | None:
        """Expand bundled table into multiple tables."""
        def from_template(ctx: TableContext, tag: str):
            ctx = copy.copy(ctx)
            ctx.name = f"{ctx.name}_{tag}"
            return ctx

        if ctx.logical_name == "chunk_refresh_queue":
            return [from_template(ctx, tag) for tag in self._cell_tags]

        return None

    def _discover_tables(
        self,
        component_filter: Callable[[cfg.Scope], bool] | None = None,
    ) -> list[TableContext]:
        """Get all table contexts across filtered components."""
        components = self._ground_config.sequoia_components
        if component_filter is not None:
            components = [c for c in components if component_filter(c)]

        table_contexts = []
        for scope in components:
            component_config = self._app.config_provider.get_component_config(scope)
            component_context = ComponentContext(scope, component_config, self._ground_config)
            root_dir = self._ground_config.sequoia_root_cypress_path

            table_descriptors = _get_sequoia_table_descriptors(scope, self._version)

            for descriptor in table_descriptors.values():
                attributes = component_context.get_table_attributes(descriptor)

                pivot_fn = pivots.get_pivot_function(descriptor.name)
                shard_count = attributes.get("tablet_count")
                pivot_keys = (
                    pivot_fn(shard_count, self._cell_tags, self._version)
                    if pivot_fn is not None and shard_count is not None
                    else None)

                table_context = TableContext(
                    root_dir, descriptor, attributes, component_context, self._version, pivot_keys)
                expanded = self._maybe_expand_bundled_table(table_context)
                if expanded is not None:
                    table_contexts.extend(expanded)
                else:
                    table_contexts.append(table_context)

        return table_contexts

    def for_each_table(
        self,
        component_filter: Callable[[cfg.Scope], bool] | None = None,
        table_filter: Callable[[TableContext], bool] | None = None,
    ) -> TableBuilder:
        """Enter table scope for all matching tables in filtered components."""
        table_contexts = self._discover_tables(component_filter)
        if table_filter is not None:
            table_contexts = [tc for tc in table_contexts if table_filter(tc)]

        return TableBuilder(table_contexts, self)

    def for_table(
        self,
        scope: cfg.Scope,
        table_name: str,
    ) -> TableBuilder:
        """Enter table scope for a specific table by name and component."""
        return self.for_each_table(
            component_filter=lambda s: s == scope,
            table_filter=lambda ctx: ctx.logical_name == table_name)

    def with_component_factory(
        self,
        factory: Callable[[ComponentContext], Sequence[actions.Action]],
        component_filter: Callable[[cfg.Scope], bool] | None = None,
    ) -> ActionBuilder:
        """Add actions from a factory with component context."""
        components = self._ground_config.sequoia_components
        if component_filter:
            components = [c for c in components if component_filter(c)]

        for scope in components:
            component_config = self._app.config_provider.get_component_config(scope)
            context = ComponentContext(scope, component_config, self._ground_config)
            action_list = factory(context)
            self._actions.extend(action_list)

        return self

    def with_factory(
        self,
        factory: Callable[[sequoia_app.SequoiaTool], Sequence[actions.Action]],
    ) -> ActionBuilder:
        """Add actions from a factory."""
        action_list = factory(self._app)
        self._actions.extend(action_list)
        return self

    def with_action(self, action: actions.Action) -> ActionBuilder:
        self._actions.append(action)
        return self

    def build(self) -> actions.ActionPlan:
        """Build the final action plan."""
        return actions.ActionPlan(self._actions, self._name)


class ConversionAction(actions.Action):
    """Action wrapping migrationlib.Conversion for table transformations."""

    def __init__(
        self,
        table_context: TableContext,
        source: str | list[str],
        pool: str | None = None,
        **conversion_kwargs,
    ) -> None:
        self._table_context = table_context
        self._source = source
        self._pool = pool
        self._conversion_kwargs = conversion_kwargs

    @override
    def describe(self) -> str:
        return (f"Apply conversion from {self._source} "
                f"to {self._table_context} in {self._table_context.parent_path}")

    @override
    def execute(self, app: sequoia_app.SequoiaTool) -> None:
        # TODO(danilalexeev): Use dict in `migrationlib.TableInfo`.
        def convert_to_key_tuple(column: dict[str, Any]) -> tuple[Any, ...]:
            return (
                column["name"],
                column["type"],
                column.get("expression"),
                column)

        def convert_to_value_tuple(column: dict[str, Any]) -> tuple[Any, ...]:
            return (
                column["name"],
                column["type"],
                column)

        tablet_count = self._table_context.attributes.pop("tablet_count", None)

        table_info = migrationlib.TableInfo(
            key_columns=[
                convert_to_key_tuple(c)
                for c in self._table_context.descriptor.schema
                if c.get("sort_order") is not None
            ],
            value_columns=[
                convert_to_value_tuple(c)
                for c in self._table_context.descriptor.schema
                if c.get("sort_order") is None
            ],
            attributes=self._table_context.attributes)

        self._conversion_kwargs.update(
            make_result_available_after_conversion=True)

        conversion = migrationlib.Conversion(
            table=self._table_context.name,
            **self._conversion_kwargs)

        random_suffix = uuid.uuid4().hex[:8]
        tmp_table_path = f"{self._table_context.path}_tmp{random_suffix}"

        conversion(
            app.ground_client,
            table_info,
            target_table=tmp_table_path,
            source_table=self._source,
            tables_path=self._table_context.parent_path,
            shard_count=tablet_count,
            pool=self._pool,
            version=self._table_context.version)

        # TODO(danilalexeev): Support uniform pivot-keys generation.
        if tablet_count is not None:
            app.ground_client.reshard_table(
                self._table_context.path,
                tablet_count=tablet_count,
                sync=True)

    @override
    def dry_run(self, app: sequoia_app.SequoiaTool) -> None:
        # TODO(danilalexeev): Implement.
        utils.log_dry_run(
            f"Would apply conversion from {self._source} "
            f"to {self._table_context}",
            logger)
