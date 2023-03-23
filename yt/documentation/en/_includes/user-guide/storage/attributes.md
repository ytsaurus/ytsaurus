# Attributes

This section contains information about Cypress attributes.

## General information { #common }

Each object in {{product-name}} has a set of **attributes** associated with it. The set of attributes can be thought of as a key-value dictionary which is both readable and modifiable. Arbitrary non-empty strings can serve as attribute names. Arbitrary [YSON](../../../user-guide/storage/yson.md) structures can be values.

There are two kinds of attributes: **[system](#system_attr)** attributes and **[user](#user_attr)** attributes.

The special path modifier `/@` and the `get`, `list`, `set`, and `remove` commands are used to access object attributes. In addition to accessing an attribute as a whole, you can address a part of it, in case the attribute value is a composite type.

For more information about path modifiers, see [YPath](../../../user-guide/storage/ypath.md).

For the `get` and `list` commands, you can optionally specify a list of attributes that you need to return along with the objects found.

## System attributes { #system_attr }

Different types of objects have their own set of type-specific system attributes. For example, all objects have the `id` attribute, tables have the `compression_codec` and `compressed_data_size` attributes. For more information about the attributes inherent to all objects, see [Objects](../../../user-guide/storage/objects.md).

System attributes are either modifiable (can be changed by the user), such as `compression_codec`, or unmodifiable, such as `compressed_data_size`. If you try to set an unmodifiable attribute, the system will return an error. Attempting to remove a system attribute will also cause an error.

Some attributes are **inherited**: `compression_codec`, `erasure_codec`, `primary_medium`, `media`, `replication_factor`, `vital`, `tablet_cell_bundle`, `atomicity`, `commit_ordering`, `in_memory_mode`, and `optimize_for`. Besides objects for which the semantics of these attributes is obvious (tables, files, etc.), these attributes can be set on composite nodes (i.e. on map and list nodes). Inherited attributes are nullable with respect to composite nodes.  When no value for an inherited attribute is specified when creating a new object, that attribute gets the value of the closest ancestor node, or the default value if no ancestor up to the root has the attribute set.

System attributes can also be **opaque** (or **computed**). The value of an opaque attribute will be available only when explicitly requested with the `get` command, but will not be provided when the list of all attributes is requested. Specifically, when all attributes are requested, the value of an opaque attribute will be set to **entity**. Most often attributes with a large value, or the value that takes a long time to compute, are marked as opaque. If the value of an attribute is 100 KB or more, it is practical to mark this attribute as opaque. In this case, when all attributes are requested, it will not be returned, making it fast to read all attributes. If you need to get the value of an opaque attribute, you must explicitly request it. An example of an opaque attribute will be a list of all table chunks (the `chunk_ids` attribute) or the total usage of resources by a Cypress subtree (the `recursive_resource_usage` attribute).

Below is a list of attributes of {{product-name}} system objects:

- [Attributes of any object](../../../user-guide/storage/objects.md#attributes).
- [Attributes of Cypress nodes](../../../user-guide/storage/cypress.md#attributes).
- [Attributes of static tables](../../../user-guide/storage/static-tables.md#attributes).
- [Attributes of dynamic tables](../../../user-guide/dynamic-tables/overview.md#attributes).
- [Attributes of files](../../../user-guide/storage/files.md#attributes).
- The [compression_codec](../../../user-guide/storage/compression.md#get_compression) and [erasure_codec](../../../user-guide/storage/replication.md#erasure) attributes for tables and files.
- [Attributes of transactions](../../../user-guide/storage/transactions.md#attributes).
- [Attributes of links](../../../user-guide/storage/links.md#attributes).
- Attributes of [accounts](../../../user-guide/storage/accounts.md#account_attributes).
- [Attributes of media](../../../user-guide/storage/media.md#atributy).

## User attributes { #user_attr }

The user can set user-defined attributes.

{% note warning "Attention!" %}

The names (keys) of user attributes must not coincide with the names of system attributes. We recommend starting user attribute names with an underscore to avoid potential problems in the future if the set of system attributes is extended.

{% endnote %}

{% note warning "Attention!" %}

Attributes are metadata that is being stored in the Cypress master server memory. It is recommended that the total volume of all user attributes does not exceed hundreds of megabytes.

{% endnote %}

## How attributes are stored in the system { #how_stored }

How a system attribute is stored depends on type of the object. Some attributes are computed on the fly rather than being stored. All user attributes are stored as a single key-value dict. This dictionary is encoded as YSON, so accessing the value of a specific attribute requires YSON-decoding.

```bash
yt set //tmp/@my_attribute '{x=10; y=["hello"; "world"]}'
yt get //tmp/@my_attribute/x
10
yt get //tmp/@my_attribute/y/1
"world"

yt set //tmp/@my_attribute/y/@inner_attribute attr
yt get //tmp/@my_attribute/y/@inner_attribute
"attr"
```

## Examples { #examples }

```bash
# Getting all object attributes
yt get //tmp/@
{
  "creation_time" = "2013-12-06T19:00:48.256444Z";
  "locks" = [];
 ...
}

# Getting a specific object attribute
yt get //tmp/@type
"map_node"

# Setting a specific object attribute
yt set //tmp/@my_attribute "my_value"

# Removing an attribute
yt remove //tmp/@my_attribute

# creating a table
echo '{"a": 10, "b": "hello"}' | yt write //tmp/my_table --format json

# Getting the first table chunk
yt get //tmp/my_table/@chunk_ids/0
"3ab1-cd56f-3ec0064-c8998820"

# Removing a table
yt remove //tmp/my_table --force


# Requesting a list with attributes
yt list / --attribute type --format '<format=pretty>yson'
[
    <
        "type" = "map_node"
    > "home";
    <
        "type" = "map_node"
    > "kiwi";
   ....
]
```

## Attributes on paths { #path_attr }

A popular scenario for using attributes is annotating the path to tables and files when making requests. For more information, see Operation settings and [YPath](../../../user-guide/storage/ypath.md#known_attributes).

