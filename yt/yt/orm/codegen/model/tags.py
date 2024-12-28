from .enum import OrmEnumValue
from .message import OrmMessage

from copy import deepcopy
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from operator import itemgetter


@dataclass
class TagsTreeNode:
    children: dict[str, "TagsTreeNode"] = dataclass_field(default_factory=dict)

    message_tags: dict[int, str] = dataclass_field(default_factory=dict)
    add_tags: dict[int, str] = dataclass_field(default_factory=dict)
    remove_tags: dict[int, str] = dataclass_field(default_factory=dict)

    etc_name: str | None = None
    etc: bool = False
    repeated: bool = False

    tags: dict[int, str] = dataclass_field(default_factory=dict)

    @property
    def formatted_tags(self):
        return ", ".join(f"/*{name}*/ {number}" for number, name in sorted(self.tags.items(), key=itemgetter(0)))


def _build_tags_tree(object: "OrmObject"):
    def build_tags(tags: list[OrmEnumValue]):
        return {tag.number: tag.enum_value_name for tag in tags}

    def build_message_nodes(message: OrmMessage):
        nodes = {}

        for field in message.fields:
            if field.is_view:
                continue

            field_node = TagsTreeNode(
                children=build_message_nodes(field.value_message) if field.value_message else {},
                message_tags=build_tags(field.value_message.message_tags) if field.value_message else {},
                add_tags=build_tags(field._add_tags),
                remove_tags=build_tags(field._remove_tags),
            )
            if field in message.direct_fields and not (field.system or field.computed):
                field_node.etc = True
                for etc in message.etcs:
                    if field in etc.fields and etc.name:
                        field_node.etc_name = etc.name
                        break
            field_node.repeated = field.is_repeated

            nodes[field.snake_case_name] = field_node

        return nodes

    object.tags_tree = TagsTreeNode(
        add_tags={10: "any"} | build_tags(object.root.message_tags),
    )
    for field in object.root.fields:
        if not field.value_message:
            continue

        object.tags_tree.children[field.snake_case_name] = TagsTreeNode(
            children=build_message_nodes(field.value_message),
            message_tags=build_tags(field.value_message.message_tags),
        )


def _build_tags(node: TagsTreeNode, parent_tags: dict[int, str] | None = None):
    def build_tags(node: TagsTreeNode, parent_tags: dict[int, str]):
        tags = deepcopy(parent_tags)

        tags |= node.message_tags
        tags = {number: name for number, name in tags.items() if number not in node.remove_tags}
        tags |= node.add_tags

        node.message_tags.clear()
        node.add_tags.clear()
        node.remove_tags.clear()

        return tags

    if parent_tags is None:
        parent_tags = {}

    node.tags = build_tags(node, parent_tags)

    for child in node.children.values():
        _build_tags(child, node.tags)


def _collapse_tags(node: TagsTreeNode):
    do_collapse = True

    for child in node.children.values():
        child_collapsed = _collapse_tags(child)
        if not child_collapsed or node.tags.keys() != child.tags.keys():
            do_collapse = False

    if do_collapse:
        node.children.clear()

    return do_collapse


def resolve_tags(object: "OrmObject"):
    if object.enable_tags:
        _build_tags_tree(object)
        _build_tags(object.tags_tree)
        _collapse_tags(object.tags_tree)
