import copy
import deepmerge


def _merge(config, path, base, nxt):
    for k, v in nxt.items():
        if k not in base:
            base[k] = copy.deepcopy(v)
        else:
            base[k] = config.value_strategy(path + [k], base[k], v)
    return base


def _override(config, path, base, nxt):
    return copy.deepcopy(nxt)


# Default merger, merges dict, overrides other.
DICT_MERGER = deepmerge.Merger(
    [
        (dict, [_merge]),
    ],
    [_override],
    [_override],
)


def merge_copy(base, patch, merger=DICT_MERGER):
    """Apply patch to base, keep base and patch w/o modification.
    base: Dict[str, Any], patch: Dict[str, Any]
    """
    return merger.merge(copy.deepcopy(base), patch)


def merge_inplace(base, patch, merger=DICT_MERGER):
    """Apply patch to base inplace.
    base: Dict[str, Any], patch: Dict[str, Any]
    """
    return merger.merge(base, patch)


def apply_inherit_recursively(config):
    """Apply 'inherit' instruction recursively on all levels."""
    for k, v in config.items():
        if isinstance(v, dict):
            inherit = v.pop("inherit", None)
            if inherit:
                config[k] = v = merge_copy(config[inherit], v)
            apply_inherit_recursively(v)

    return config


def get_section(config, name, merger=DICT_MERGER):
    """Get section by name.

    Supports special section `default`, which contains default options for all other sections
    Supports section inheritance via keyword `inherit`.

    config: Dict[str, Any], name: str -> Dict[str, Any]
    """

    if not config:
        return config

    bases = []
    bases.append(config.get(name, {}))
    while "inherit" in bases[-1]:
        bases.append(config[bases[-1]["inherit"]])

    if "default" in config:
        bases.append(config["default"])

    bases.reverse()
    result = {}
    for base in bases:
        merge_inplace(result, copy.deepcopy(base), merger=merger)

    result.pop("inherit", None)
    return result
