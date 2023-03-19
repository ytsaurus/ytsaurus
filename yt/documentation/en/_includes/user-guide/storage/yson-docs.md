# YSON document

This section contains information about YSON documents.

## General information { #common }

A YSON document is a [Cypress](../../../user-guide/storage/cypress.md) node of the **document** type designed to store random YSON structures.

Besides a YSON document, you can create and store hierarchical data structures in the form of nodes of the `map_node`, `list_node`, `string_node`, and other types.
YSON documents differ from this method in the following ways:

- Storing data as a document is more compact in terms of memory consumption, since the document node is not a full-fledged Cypress node.
- The document behaves as a whole in terms of Cypress-specific features: locks, owners, the `revision`, `creation_time`, `modification_time`, `expiration_time`, and other attributes.

You can work with documents using standard commands: `get`, `list`, `exists`, `set`, and `remove`, just like with other objects in Cypress.

Requests and modifications within the document are supported. Addressing within the document is performed using the YPath language.

Learn more about the [YSON](../../../user-guide/storage/yson.md) format.

## Usage { #usage }

Create a YSON document:

CLI
```bash
yt create document //tmp/my_test_doc
3f08-5b920c-3fe01a5-e0c12642
```

By default, a YSON entity is stored in the document when it is created. Read a YSON document:

CLI
```bash
yt get //tmp/my_test_doc
```

Specify the initial value of the YSON document when it is created:

CLI
```bash
yt create document //tmp/my_test_doc --attributes '{value=hello}'
3f08-6c0ee0-3fe01a5-2c4f6104
yt get //tmp/my_test_doc
```
```
"hello"
```

Write a number to the YSON document and read it:

CLI
```bash
yt set //tmp/my_test_doc 123
#
yt get //tmp/my_test_doc
123
```

Write a complex structure to a YSON document and read it fully or partially:

CLI
```bash
yt set //tmp/my_test_doc '{key1=value1;key2={subkey=456}}'
#
yt get //tmp/my_test_doc
```
```
{
    "key1" = "value1";
    "key2" = {
        "subkey" = 456;
    };
}
yt get //tmp/my_test_doc/key2
{
    "subkey" = 456;
}
```

The node will have the `document` type:

CLI
```bash
yt get //tmp/my_test_doc/@type
```
```
"document"
```

Partially change a YSON document:

CLI
```bash
yt set //tmp/my_test_doc/key1 newvalue1
```

Remove a YSON document:

CLI
```bash
yt remove //tmp/my_test_doc
```

## Limits { #limits }

In Cypress, you can create and store hierarchical data structures in another way: in the form of nodes of the `map_node`, `list_node`, `string_node`, and other types.

YSON documents are read and written via the Cypress master server, so they cannot be used as a high-load object database. A reasonable limit is single-digit [RPS](https://en.wikipedia.org/wiki/Queries_per_second). Since this data is stored in the master server memory in the form of a tree, choose the amount of data with caution.

Kilobytes can be considered a reasonable limit for a single document. The total volume of all user documents must not exceed single-digit megabytes. Such nodes are usually used to store small pieces of structured metadata, configuration, etc.

## System attributes { #attributes }

Besides the attributes inherent to all Cypress nodes, documents have the following additional attributes:

| **Attribute** | **Type** | **Description** |
| ----------- | ------- | ------------------------------------------------------------ |
| `value` | `any` | Full contents of the document. This attribute enables you to specify the contents of the document when it is created. The attribute is `opaque`, which means that when you read all attributes of a node without a filter, it will be displayed as `entity`. |

