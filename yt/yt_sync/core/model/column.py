from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from typing import ClassVar

from yt.yt_sync.core.spec import Column
from yt.yt_sync.core.spec import TypeV1
from yt.yt_sync.core.spec.details import TypeV1Spec
from yt.yt_sync.core.spec.details import TypeV3Spec

from .types import Types


@dataclass
class YtColumn:
    # Disables normalized_expression in schema comparison.
    # Should be tested with True for all tables, then removed completely with normalized expression.
    # See YTADMINREQ-43528 for details.
    DISABLE_NORMALIZED_EXPRESSION: ClassVar[bool] = False

    name: str
    column_type: str
    column_type_v3: str | dict[str, Any]
    sort_order: str | None = None
    expression: str | None = None
    required: bool = False
    lock: str | None = None
    group: str | None = None
    aggregate: str | None = None
    max_inline_hunk_size: int | None = None

    @staticmethod
    def _cmp_nullable(left: str | None, right: str | None) -> bool:
        if not left:
            return not right
        if not right:
            return not left
        return left == right

    def __eq__(self, other: YtColumn | None):
        if other is None:
            return False
        assert isinstance(other, type(self)), f"Can't compare with type {type(other)}"
        return (
            self.name == other.name
            and self.column_type == other.column_type
            and self.column_type_v3 == other.column_type_v3
            and self.sort_order == other.sort_order
            and self.normalized_expression == other.normalized_expression
            and self.required == other.required
            and self._cmp_nullable(self.lock, other.lock)
            and self._cmp_nullable(self.group, other.group)
            and self._cmp_nullable(self.aggregate, other.aggregate)
            and self.max_inline_hunk_size == other.max_inline_hunk_size
        )

    @property
    def normalized_expression(self) -> str | None:
        # TODO (ashishkin): Remove completely when tested everywhere with DISABLE_NORMALIZED_EXPRESSION setting.
        if self.DISABLE_NORMALIZED_EXPRESSION:
            return self.expression
        if self.expression is None:
            return None
        return self.expression.replace(" ", "")

    @property
    def yt_attributes(self) -> Types.Attributes:
        result = dict()
        result["name"] = self.name
        result["type"] = self.column_type
        result["type_v3"] = self.column_type_v3
        if self.sort_order:
            result["sort_order"] = self.sort_order
        if self.expression:
            result["expression"] = self.expression
        if self.required:
            result["required"] = True
        if self.lock:
            result["lock"] = self.lock
        if self.group:
            result["group"] = self.group
        if self.aggregate:
            result["aggregate"] = self.aggregate
        if self.max_inline_hunk_size is not None:
            result["max_inline_hunk_size"] = self.max_inline_hunk_size
        return result

    @property
    def is_key_column(self):
        return self.sort_order is not None

    @property
    def has_hunks(self):
        return self.max_inline_hunk_size is not None and self.max_inline_hunk_size > 0

    def is_compatible(self, other: YtColumn) -> bool:
        """Checks if column can be converted to other."""
        assert isinstance(other, type(self))
        if self.name != other.name:
            return False
        if self.column_type != other.column_type:
            return False
        if self.column_type == TypeV1.ANY and self.column_type_v3 != other.column_type_v3:
            return False
        if bool(self.expression) != bool(other.expression):
            return False
        if not self.required and other.required:
            return False
        if self.aggregate and self.aggregate != other.aggregate:
            return False
        if self.max_inline_hunk_size and not other.max_inline_hunk_size:
            return False
        return True

    def is_data_modification_required(self, other: YtColumn) -> bool:
        """Checks if map operation needed to convert field."""
        assert isinstance(other, type(self))
        result = False
        if self.sort_order or other.sort_order:
            result |= self.sort_order != other.sort_order
        if self.expression and other.expression:
            result |= self.normalized_expression != other.normalized_expression
        return result

    def is_downtime_required(self, other: YtColumn) -> bool:
        assert isinstance(other, type(self))
        if self.sort_order or other.sort_order:
            return self.sort_order != other.sort_order
        return False

    @classmethod
    def make(cls, spec: Column) -> YtColumn:
        type_v1_spec = TypeV1Spec(spec)
        type_v3_spec = TypeV3Spec(spec)
        assert type_v1_spec.is_active or type_v3_spec.is_active

        required: bool = type_v1_spec.is_required if type_v1_spec.is_active else type_v3_spec.is_required

        return cls(
            name=spec.name,
            column_type=str(type_v1_spec.type_v1) if type_v1_spec.is_active else str(type_v3_spec.type_v1),
            column_type_v3=type_v3_spec.dump() if type_v3_spec.is_active else type_v1_spec.dump_v3(),
            sort_order=str(spec.sort_order) if spec.sort_order else None,
            expression=spec.expression,
            required=required,
            lock=spec.lock,
            group=spec.group,
            aggregate=str(spec.aggregate) if spec.aggregate else None,
            max_inline_hunk_size=spec.max_inline_hunk_size,
        )
