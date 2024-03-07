from enum import Enum
import random
import copy
import itertools
import string

from pprint import pprint

import yt.yson as yson

##################################################################


class VariationPolicy(Enum):
    Variate = 0
    PickRandom = 1

    # Aliases.
    variate = 0
    pick_random = 1


class Variable():
    def __init__(self, modes, policy=VariationPolicy.Variate):
        self.modes = list(modes)
        if isinstance(policy, str):
            policy = VariationPolicy[policy]
        assert isinstance(policy, VariationPolicy)
        self.policy = policy

    def variant_count(self):
        if self.policy == VariationPolicy.Variate:
            return len(self.modes)
        else:
            return 1

    def get_variant(self, index):
        if self.policy == VariationPolicy.Variate:
            return self.modes[index]
        else:
            return random.choice(self.modes)

    def __repr__(self):
        modes = ", ".join(map(str, self.modes))
        if self.policy == VariationPolicy.Variate:
            return "Variate({})".format(modes)
        else:
            return "Any({})".format(modes)

    @classmethod
    def from_string(cls, value, type):
        if value.startswith("Variate(") and value.endswith(")"):
            return Variable(
                (type(x.strip()) for x in value[8:-1].split(",")),
                VariationPolicy.Variate)
        elif value.startswith("Any(") and value.endswith(")"):
            return Variable(
                (type(x.strip()) for x in value[4:-1].split(",")),
                VariationPolicy.PickRandom)
        else:
            # Sanity check: parameter starting with "Smth(" is likely a typo.
            paren_idx = value.find("(")
            if paren_idx != -1 and value[:paren_idx].isalpha():
                raise RuntimeError("Invalid variable {}".format(value))

            return type(value)


class BoolVariable(Variable):
    def __init__(self, policy=VariationPolicy.Variate):
        super(BoolVariable, self).__init__([False, True], policy)


class MapWithUnrecognizedChildren(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

##################################################################

def variate_modes(spec):
    variable_paths = []
    complexity = [1]

    def _dfs(vertex, path):
        if isinstance(vertex, Variable):
            variable_paths.append((tuple(path), vertex))
            complexity[0] *= vertex.variant_count()
            if complexity[0] > 1000:
                raise RuntimeError("Complexity is too large, cool your jets!")
        elif isinstance(vertex, dict):
            for key, value in vertex.items():
                path.append(key)
                _dfs(value, path)
                path.pop(-1)
        elif isinstance(vertex, list):
            for index, value in enumerate(vertex):
                path.append(index)
                _dfs(value, path)
                path.pop(-1)

    _dfs(spec, [])

    def _deep_set(vertex, path, value):
        assert path
        for key in path[:-1]:
            vertex = vertex[key]
        vertex[path[-1]] = value

    if not variable_paths:
        return [(spec, "")]

    paths, variables = zip(*variable_paths)
    assignments = itertools.product(*(range(v.variant_count()) for v in variables))
    resulting_specs = []
    for assignment in assignments:
        root = copy.deepcopy(spec)
        description = []
        for path, variable, index in zip(paths, variables, assignment):
            variant = variable.get_variant(index)
            _deep_set(root, path, variant)
            description.append(("/".join(map(str, path)), variant))
        resulting_specs.append((root, description))

    return resulting_specs


def merge_specs(full, extra, allow_unrecognized=False, path="", root=True):
    if root:
        full = copy.deepcopy(full)

    assert isinstance(extra, dict)

    if isinstance(full, MapWithUnrecognizedChildren):
        allow_unrecognized = True

    for k, v in extra.items():
        if k not in full:
            if allow_unrecognized:
                full[k] = v
            else:
                raise RuntimeError('Unrecognized option "{}/{}"'.format(path, k))
        if isinstance(full[k], Opaque):
            full[k] = copy.deepcopy(full[k].underlying)
        if isinstance(v, dict) and isinstance(full[k], dict):
            merge_specs(full[k], v, allow_unrecognized, path + "/" + k, root=False)
        elif isinstance(v, Opaque):
            full[k] = Opaque(full[k])
        else:
            full[k] = v

    if root:
        return full


################################################################################


class Opaque():
    def __init__(self, underlying=None):
        self.underlying = underlying

    def __repr__(self):
        return None.__repr__()

    def __getitem__(self, attr):
        return None.__getitem__(attr)


# Presets
##################################################################

spec_template = {
    "seed": None,
    "mode": "iterative",
    "table_type": "sorted", #Variable(["sorted", "ordered"], VariationPolicy.PickRandom),
    "replicas": [],
    "chunk_format": Variable(["table_versioned_simple", "table_versioned_columnar", "table_versioned_slim"], VariationPolicy.PickRandom),
    "in_memory_mode": Variable(["none", "compressed", "uncompressed"], VariationPolicy.PickRandom),
    "erasure_codec": Variable(["none"], VariationPolicy.PickRandom),
    "compression_codec": None,
    "enable_tablet_balancer": False,

    "size": {
        "key_count": 1000,
        "data_weight": None,
        "job_count": 2,
        "data_job_count": None,
        "data_weight_per_data_job": None,
        "tablet_count": 5,
        "iterations": 2,
        "write_user_slot_count": None,
        "read_user_slot_count": None,
        "bundle_node_count": None,
    },

    "prepare_table_via_alter": BoolVariable(VariationPolicy.PickRandom),
    "skip_flush": False,
    "reshard": True,
    "alter": True,
    "remote_copy_to_itself": False,

    "mapreduce": True,
    "mr_options": {
        "operation_types": ["all"],
    },

    "sorted": {
        "enable_lookup_hash_table": BoolVariable(VariationPolicy.PickRandom),
        "lookup_cache_rows_per_tablet": None,
        "enable_data_node_lookup": BoolVariable(VariationPolicy.PickRandom),
        "enable_hash_chunk_index_for_lookup": False,
        "write_policy": Variable(["insert_rows", "bulk_insert", "mixed"], VariationPolicy.PickRandom),
        "insertion_probability": 0.7,
        "deletion_probability": 0.1,
        "max_inline_hunk_size": None,
        "enable_value_dictionary_compression": False,
    },

    "ordered": {
        "rows_per_tablet": 10000,
        "insertion_batch_size": 100,
        "trim": True,
    },

    "replicated": {
        "min_sync_replicas": 1,
        "max_sync_replicas": None,
        "switch_replica_modes": False,
        "enable_replicated_table_tracker": False,
        "mount_unmount_replicas": BoolVariable(VariationPolicy.PickRandom),
    },

    "schema": {
        "fixed": None,
        "key_column_count": None,
        "value_column_count": None,
        "key_column_types": None,
        "value_column_types": None,
        "allow_aggregates": True,
    },

    "extra_attributes": MapWithUnrecognizedChildren(),

    "retries": {
        "interval": 15,
        "count": 4,
    },

    "testing": {
        "skip_generation": False,
        "skip_write": False,
        "skip_verify": False,
        "skip_lookup": False,
        "skip_select": False,
        "skip_group_by": True,
        "ignore_failed_mr": False,
    },

    "ipv4": False
}

simple_sorted_spec = merge_specs(spec_template, {
    "table_type": "sorted",
    "chunk_format": Variable(["table_versioned_simple", "table_versioned_columnar", "table_versioned_slim"], VariationPolicy.PickRandom),
    "in_memory_mode": "none",
    "erasure_codec": "none",

    "size": {
        "tablet_count": 1,
    },

    "prepare_table_via_alter": False,

    "sorted": {
        "enable_lookup_hash_table": False,
        "enable_data_node_lookup": False,
        "write_policy": "insert_rows",
    },

    "ordered": None,
    "replicated": Opaque(),
})

simple_ordered_spec = merge_specs(spec_template, {
    "table_type": "ordered",
    "chunk_format": Variable(["table_versioned_simple", "table_versioned_columnar", "table_versioned_slim"], VariationPolicy.PickRandom),
    "in_memory_mode": "none",
    "erasure_codec": "none",

    "size": {
        "tablet_count": 1,
    },

    "prepare_table_via_alter": False,

    "ordered": {
    },

    "sorted": None,
    "replicated": Opaque(),
})

indexed_sorted_spec = merge_specs(spec_template, {
    "table_type": "sorted",
    "chunk_format": "table_versioned_indexed",
    "in_memory_mode": "none",
    "erasure_codec": "none",
    "compression_codec": "none",

    "size": {
        "tablet_count": 1,
    },

    "prepare_table_via_alter": False,

    "sorted": {
        "enable_lookup_hash_table": False,
        "enable_data_node_lookup": BoolVariable(VariationPolicy.PickRandom),
        "enable_hash_chunk_index_for_lookup": BoolVariable(VariationPolicy.PickRandom),
        "write_policy": "insert_rows",
    },

    "ordered": None,
    "replicated": Opaque(),
})

presets = {
    "full": spec_template,
    "simple_sorted": simple_sorted_spec,
    "simple_ordered": simple_ordered_spec,
    "indexed_sorted": indexed_sorted_spec,
}

##################################################################

simple_schema_mixin = lambda: {
    "schema": {
        "key_column_count": 2,
        "value_column_count": 1,
        "key_column_types": ["int64"],
        "value_column_types": ["string"],
    }
}

tiny_size_mixin = lambda: {
    "size": {
        "key_count": 1000,
        "data_weight": None,
        "job_count": 1,
        "tablet_count": 1,
    }
}

small_size_mixin = lambda: {
    "size": {
        "key_count": None,
        "data_weight": 50 * 2**20,
        "job_count": 5,
        "tablet_count": 1,
    }
}

medium_size_mixin = lambda: {
    "size": {
        "key_count": None,
        "data_weight": 2**30,
        "data_weight_per_data_job": 20 * 2**20,
        "job_count": 40,
        "tablet_count": 2,
    }
}

large_size_mixin = lambda: {
    "size": {
        "key_count": None,
        "data_weight": 20 * 2**30,
        "data_weight_per_data_job": 100 * 2**20,
        "job_count": 400,
        "tablet_count": 4,
    }
}

huge_size_mixin = lambda: {
    "size": {
        "key_count": None,
        "data_weight": 80 * 2**30,
        "data_weight_per_data_job": 100 * 2**20,
        "job_count": 1000,
        "tablet_count": 10,
    },
    "prepare_table_via_alter": True,
}

no_retries_mixin = lambda: {
    "retries": {
        "interval": 5,
        "count": 1,
    },
}

full_retries_mixin = lambda: {
    "retries": {
        "interval": 15,
        "count": 40,
    },
}

ipv4_mixin = lambda: {
    "ipv4": True,
}

mixins = {
    "simple_schema": simple_schema_mixin,
    "tiny": tiny_size_mixin,
    "small": small_size_mixin,
    "medium": medium_size_mixin,
    "large": large_size_mixin,
    "huge": huge_size_mixin,
    "no_retries": no_retries_mixin,
    "full_retries": full_retries_mixin,
    "ipv4": ipv4_mixin,
}

# Check all mixins could be successfully merged.
for mixin in mixins.values():
    merge_specs(spec_template, mixin())

##################################################################

class Spec():
    def __init__(self, dct):
        self.dct = dct

    def __getattr__(self, key):
        if key in self.dct:
            value = self.dct[key]
            return Spec(value) if isinstance(value, dict) else value
        raise AttributeError(key)

    def to_dict(self):
        return copy.deepcopy(self.dct)

    # Helpers for accessing attributes that may be calculated implicitly.
    def get_write_user_slot_count(self):
        if self.size.write_user_slot_count is not None:
            return self.size.write_user_slot_count
        if self.size.bundle_node_count is not None:
            return self.size.bundle_node_count * self.write_user_slots_per_node
        return None

    def get_read_user_slot_count(self):
        if self.size.read_user_slot_count is not None:
            return self.size.read_user_slot_count
        if self.size.bundle_node_count is not None:
            return self.size.bundle_node_count * self.read_user_slots_per_node
        return None

##################################################################

def get_spec_preset_names():
    return presets.keys()

def get_spec_preset(name):
    return presets[name]

def get_mixin(name, *args, **kwargs):
    return mixins[name](*args, **kwargs)

##################################################################

def spec_from_yson(preset, mixins, yson_dict):
    spec = presets[preset]

    # TODO: Support custom args for mixins.
    for mixin_name in mixins:
        spec = merge_specs(spec, get_mixin(mixin_name))

    def _unroll_variables(obj):
        if isinstance(obj, yson.YsonList) and obj.attributes.get("any"):
            return Variable(obj, policy=VariationPolicy.PickRandom)
        elif isinstance(obj, dict):
            return {k: _unroll_variables(v) for k, v in obj.items()}
        else:
            if hasattr(obj, "attributes"):
                obj.attributes = {}
            return obj

    custom_spec = _unroll_variables(yson_dict)

    return merge_specs(spec, custom_spec)

##################################################################

if __name__ == "__main__":
    pprint(simple_sorted_spec)
