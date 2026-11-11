"""Describe profile states from protobuf messages declared by the pipeline."""

import logging

from google.protobuf.descriptor_pb2 import FileDescriptorSet
from google.protobuf.message import Message
from yt.wrapper import yson

log = logging.getLogger(__name__)

# Mirrors MaxDescriptorSetSize in yt/yt/flow/yandex/extensions/profile_state/cpp/manager/spec.cpp.
_MAX_DESCRIPTOR_SET_SIZE = 8 << 20
_PROFILE_STATE_KINDS = (
    ("external_state_managers", "external_state_manager_class_name", "NYT::NFlow::NProfileState::TProfileStateManager"),
    ("external_state_joiners", "external_state_joiner_class_name", "NYT::NFlow::NProfileState::TProfileStateJoiner"),
)


def patch_profile_states(spec, *, proto_external_states=None, joined_proto_external_states=None):
    """Fill missing descriptor sources of profile managers and joiners in |spec|.

    Declarations map computation ids to maps of state names to protobuf message classes.
    Explicit layouts and descriptor sets take precedence over declarations.
    """
    computations = _map(spec, "computations")
    for (section, class_key, profile_class), declarations in zip(
        _PROFILE_STATE_KINDS, (proto_external_states or {}, joined_proto_external_states or {})
    ):
        for computation_id in declarations:
            if computation_id not in computations:
                raise ValueError(f"Declared profile states refer to unknown computation {computation_id}")
        for computation_id, computation in computations.items():
            context = f"Computation {computation_id}"
            try:
                entries = _map(computation, section)
                declared = declarations.get(computation_id, {})
                for state_name in declared:
                    if state_name not in entries:
                        raise ValueError(f"Declared profile state {state_name} is absent from {section}")
                for state_name, entry in entries.items():
                    context = f"Profile state {state_name} of computation {computation_id} ({section})"
                    class_name = _string(entry, class_key) or _string(entry, "class_name")
                    if class_name != profile_class.encode():
                        if state_name in declared:
                            raise ValueError(f"Declared proto state requires {profile_class}")
                        continue
                    _describe(entry, declared.get(state_name), context)
            except (TypeError, ValueError) as error:
                raise ValueError(f"{context}: {error}") from error


def _describe(entry, proto_class, context):
    parameters = _map(entry, "parameters")
    layout = _map(parameters, "descriptor_layout")
    if _string(parameters, "layout") or _string(layout, "descriptor_set"):
        return
    message_name = _string(layout, "message_name")
    if proto_class is None:
        if message_name:
            log.info("%s: leaving message %s to the descriptor pool of flow_server", context, message_name)
            return
        raise ValueError("No descriptor source: declare the proto state in the pipeline or specify descriptor_set")
    if not isinstance(proto_class, type) or not issubclass(proto_class, Message) or proto_class.DESCRIPTOR is None:
        raise TypeError("Expected a protobuf message class")
    descriptor = proto_class.DESCRIPTOR
    if message_name and message_name != descriptor.full_name.encode():
        log.warning(
            "%s: message_name %s differs from declared %s; leaving it to the descriptor pool of flow_server",
            context,
            message_name,
            descriptor.full_name,
        )
        return
    serialized = _serialize_descriptor_set(descriptor.file)
    target = entry.setdefault("parameters", {}).setdefault("descriptor_layout", {})
    target["message_name"] = descriptor.full_name
    target["descriptor_set"] = serialized


def _serialize_descriptor_set(root):
    descriptor_set = FileDescriptorSet()
    visited = set()

    def add_file(descriptor):
        if descriptor.name in visited:
            return
        visited.add(descriptor.name)
        for dependency in descriptor.dependencies:
            add_file(dependency)
        descriptor.CopyToProto(descriptor_set.file.add())

    add_file(root)
    serialized = descriptor_set.SerializeToString()
    if len(serialized) > _MAX_DESCRIPTOR_SET_SIZE:
        raise ValueError(
            f"Descriptor set of {root.name} is {len(serialized)} bytes, "
            f"over the {_MAX_DESCRIPTOR_SET_SIZE}-byte limit of flow_server"
        )
    return serialized


def _map(parent, key):
    if not isinstance(parent, dict):
        raise ValueError("Expected a spec map")
    value = parent.get(key, {})
    if not isinstance(value, dict):
        raise ValueError(f"The {key!r} node must be a map")
    return value


def _string(parent, key):
    if not isinstance(parent, dict):
        raise ValueError("Expected a state spec map")
    if key not in parent:
        return b""
    try:
        return yson.get_bytes(parent[key])
    except TypeError as error:
        raise ValueError(f"The {key!r} node must be a string") from error
