from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any
from typing import Callable
from typing import Generator

from yt.yson.yson_types import YsonEntity

from .helpers import iter_attributes_recursively
from .types import Types


@dataclass
class YtNodeAttributes:
    attributes: Types.Attributes = dataclass_field(default_factory=dict)
    user_attribute_keys: frozenset[str] = dataclass_field(default_factory=frozenset)
    # attribute_name -> path
    propagated_attributes: dict[str, str] = dataclass_field(default_factory=dict)

    @property
    def yt_attributes(self) -> Types.Attributes:
        return self._filter_none(self.attributes)

    def get(self, key: str, default: Any | None = None) -> Any | None:
        return self.attributes.get(key, default)

    def get_filtered(self, key: str) -> Any | None:
        return self.filtered_value(self.get(key))

    def pop(self, key: str, default: Any | None) -> Any | None:
        return self.attributes.pop(key, default)

    def __iter__(self):
        return self.attributes.__iter__()

    def __getitem__(self, key: str) -> Any | None:
        return self.attributes[key]

    def __setitem__(self, key: str, value: Any):
        self.attributes[key] = value

    def __bool__(self) -> bool:
        return bool(self.attributes)

    def _visit_by_path(self, path: str, visitor: Callable[[dict[str, Any], str], Any | None], ensure_path: bool):
        root = self.attributes
        parts = path.split("/")
        last = parts[-1]
        for key in parts[:-1]:
            if ensure_path:
                root = root.setdefault(key, dict())
            else:
                root = root.get(key, dict())
        return visitor(root, last)

    def remove_value(self, path: str):
        self._visit_by_path(path, lambda d, k: d.pop(k, None), ensure_path=False)

    def set_value(self, path: str, value: Any):
        def _visitor(d: dict[str, Any], k: str):
            d[k] = value

        self._visit_by_path(path, _visitor, ensure_path=True)

    def get_value(self, path: str, default_value: Any | None = None) -> Any | None:
        def _visitor(d: dict[str, Any], k: str):
            return d.get(k, default_value)

        return self._visit_by_path(path, _visitor, ensure_path=False)

    def has_value(self, path: str) -> bool:
        def _visitor(d: dict[str, Any], k: str):
            return k in d

        return self._visit_by_path(path, _visitor, ensure_path=False)

    def has_diff_with(self, other: YtNodeAttributes) -> bool:
        if not isinstance(other, YtNodeAttributes):
            raise TypeError("Can compare only with other YtNodeAttributes instance")

        return bool(next(self.changed_attributes(other), None))

    @property
    def missing_user_attributes(self) -> frozenset[str]:
        return frozenset(self.user_attribute_keys - self.attributes.keys())

    def changed_attributes(self, other: YtNodeAttributes) -> Generator[tuple[str, Any | None, Any | None], None, None]:
        if not self.attributes:
            return
        if not other.attributes:
            # All attributes must be created
            for name, value in self.attributes.items():
                yield (name, value, None)
            return

        for path, desired, actual in iter_attributes_recursively(self.attributes, other.attributes):
            if self._are_changes_ignored(path):
                continue
            if desired != actual:
                yield (path, desired, actual)

    @classmethod
    def make(cls, attributes: Types.Attributes, filter_: YtNodeAttributes | None = None) -> YtNodeAttributes:
        def _key_filter(attrs: dict[str, Any], allowed_keys: set[str]):
            for key in attrs.keys() - allowed_keys:
                attrs.pop(key, None)

        cleaned_attributes = cls._clean_attributes(attributes)
        user_attribute_keys: list[str] = []
        for attribute in cleaned_attributes.pop("user_attribute_keys", []):
            if not cls._is_untracked(attribute):
                user_attribute_keys.append(attribute)
        user_attribute_keys = frozenset(user_attribute_keys)
        if filter_:
            _key_filter(cleaned_attributes, filter_.attributes.keys() | user_attribute_keys)

        return cls(attributes=cleaned_attributes, user_attribute_keys=user_attribute_keys)

    @classmethod
    def _is_untracked(cls, attr_name: str) -> bool:
        return False

    @classmethod
    def _are_changes_ignored(cls, attr_name: str) -> bool:
        return False

    @classmethod
    def _patch_attributes(
        cls,
        attributes: Types.Attributes,
        patcher: Callable[[Types.Attributes, str, Any | None], None],
        path: str | None = None,
    ) -> Types.Attributes:
        result = {}
        for k, v in attributes.items():
            new_path = k if not path else f"{path}/{k}"
            if cls._is_untracked(new_path):
                continue
            elif isinstance(v, dict):
                result[k] = cls._patch_attributes(v, patcher, new_path)
            else:
                patcher(result, k, v)
        return result

    @classmethod
    def _clean_attributes(cls, attributes: Types.Attributes) -> Types.Attributes:
        def _patcher(attrs: Types.Attributes, key: str, value: Any | None):
            if isinstance(value, YsonEntity):
                attrs[key] = None
            else:
                attrs[key] = deepcopy(value)

        return cls._patch_attributes(attributes, _patcher)

    @classmethod
    def _filter_none(cls, attributes: Types.Attributes) -> Types.Attributes:
        def _patcher(attrs: Types.Attributes, key: str, value: Any | None):
            if value is not None:
                attrs[key] = deepcopy(value)

        return cls._patch_attributes(attributes, _patcher)

    @classmethod
    def filtered_value(cls, value: Any | None) -> Any | None:
        if isinstance(value, dict):
            return cls._filter_none(cls._clean_attributes(value))
        if isinstance(value, YsonEntity):
            return None
        return value
