from __future__ import annotations
from dataclasses import dataclass, field as dataclass_field
from library.python import resource
import google.protobuf.descriptor_pb2 as descriptor_pb2


@dataclass
class OrmDescription:
    leading_comments: str = dataclass_field(default_factory=str)
    leading_detached_comments: str = dataclass_field(default_factory=str)

    @classmethod
    def merge(cls, base: OrmDescription, mixin: OrmDescription) -> OrmDescription:
        if base is None and mixin is None:
            return None
        result = cls()
        if base is not None:
            result.leading_comments += base.leading_comments
            result.leading_detached_comments += base.leading_detached_comments
        if mixin is not None:
            result.leading_comments += mixin.leading_comments
            result.leading_detached_comments += mixin.leading_detached_comments

        return result


PATH_TO_DESCRIPTION: dict[str, OrmDescription] = {}


def _walk_path(location: descriptor_pb2.SourceCodeInfo.Location, root: descriptor_pb2.FileDescriptorProto):
    path = []
    if root.package is not None:
        path.append(root.package)

    next_is_repeated = False
    for i in location.path:
        if next_is_repeated:
            root = root[i]
            next_is_repeated = False
            path.append(root.name)
        else:
            next = root.DESCRIPTOR.fields_by_number[i]
            if hasattr(next, "label") and next.label == next.LABEL_REPEATED:
                next_is_repeated = True
            root = getattr(root, next.name)

    full_path = ".".join(path)
    PATH_TO_DESCRIPTION[full_path] = OrmDescription.merge(
        PATH_TO_DESCRIPTION.get(full_path, None),
        OrmDescription(
            leading_comments=location.leading_comments,
            leading_detached_comments="".join(location.leading_detached_comments),
        ),
    )


def _process_file_proto(file_desc_proto: descriptor_pb2.FileDescriptorProto):
    for location in file_desc_proto.source_code_info.location:
        try:
            root = file_desc_proto
            _walk_path(location, root)
        except Exception:
            pass


def load_proto_descriptions():
    resource_item = resource.find("/proto.desc")
    if resource_item is None:
        return
    file_set = descriptor_pb2.FileDescriptorSet.FromString(resource_item)
    for file_desc_proto in file_set.file:
        _process_file_proto(file_desc_proto)
