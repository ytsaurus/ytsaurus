import copy
import deepmerge

from functools import reduce
from typing import Any
from typing import Mapping
from typing import Optional
from typing import Sequence

from library.python.confmerge import merge_copy, get_section

__all__ = []


class PresetsMerger:
    def __init__(
        self,
        merge_key: str,
        presets: Mapping[str, Mapping],
    ):
        self._merge_key = merge_key
        self._presets = presets
        self._simplified_presets = {}
        for preset_key in list(self._presets.keys()):
            self._simplify_preset(preset_key)

    def apply_presets(self, spec: Any) -> Any:
        """Returns copy of spec with applied presets"""
        if isinstance(spec, Mapping):
            new_value = {}
            for k, v in spec.items():
                if k != self._merge_key:
                    new_value[k] = self.apply_presets(v)
            merged = reduce(
                merge_copy,
                [self._simplify_preset(preset_name) for preset_name in spec.get(self._merge_key, [])] + [new_value],
            )
            return merged
        elif isinstance(spec, list):
            return [self.apply_presets(item) for item in spec]
        return spec

    def _simplify_preset(self, preset_name: str) -> dict:
        if preset_name in self._simplified_presets:
            ready_result = self._simplified_presets[preset_name]
            assert ready_result is not None, "Detected cycle in presets merge rules"
            return self._simplified_presets[preset_name]
        self._simplified_presets[preset_name] = None  # Mark as "in process".
        self._simplified_presets[preset_name] = self.apply_presets(self._presets[preset_name])
        return self._simplified_presets[preset_name]


class StageMerger:
    """Merge stage specs with respect to merge keys (these lists will be concatenated)."""

    def __init__(self, merge_keys: Sequence[str], special_prefix: Optional[str] = None):
        self._merge_keys = set(merge_keys)
        self._special_prefix = special_prefix
        self._merger = deepmerge.Merger(
            [
                (dict, [self._merge_dict]),
                (list, [self._merge_list]),
            ],
            [self._override],
            [self._override],
        )

    def merge(self, base: Any, patch: Any):
        return merge_copy(base, patch, self._merger)

    def get_section(self, spec: Mapping[str, Any], name: str):
        return get_section(spec, name, self._merger)

    def _merge_dict(self, config, path, base, nxt):
        for k, v in nxt.items():
            assert self._special_prefix is None or not k.startswith(self._special_prefix) or k in self._merge_keys, (
                f"Got unknown key '{k}' that started with special prefix '{self._special_prefix}', "
                f"but it is not one of merge keys: {self._merge_keys}; current merge path: {path}"
            )
            if k not in base:
                base[k] = copy.deepcopy(v)
            else:
                base[k] = config.value_strategy(path + [k], base[k], v)
        return base

    def _merge_list(self, config, path, base, nxt):
        if len(path) > 0 and path[-1] in self._merge_keys:
            return copy.deepcopy(base) + copy.deepcopy(nxt)
        return copy.deepcopy(nxt)

    @staticmethod
    def _override(config, path, base, nxt):
        return copy.deepcopy(nxt)
