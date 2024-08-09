# YSON document

This section contains information about YSON documents.

## General information { #common }

A YSON document is a [Cypress](../../../user-guide/storage/cypress.md) node of the **document** type designed to store random YSON structures.

The document behaves as a whole in terms of Cypress-specific features: locks, owners, the `revision`, `creation_time`, `modification_time`, `expiration_time` attributes, and other features.

You can work with documents using standard [commands](../../../api/commands.md): `get`, `list`, `exists`, `set`, and `remove`, just like with other objects in Cypress.

Queries and modifications within the document are supported. This means that a document can be requested as a whole, or only some of its individual parts can be requested. Similarly, when modifying a document (the `set` command), you can change the entire document, or you can only change some of its fields. See [usage examples](#usage).

Addressing within the document is performed using the [YPath](../../../user-guide/storage/ypath.md) language.

## Usage { #usage }

Create a YSON document:

```bash
$ yt create document //tmp/my_test_doc
3f08-5b920c-3fe01a5-e0c12642
```

By default, a YSON entity is stored in the document when it is created.

Read a YSON document:

```bash
$ yt get //tmp/my_test_doc
```

Specify the initial value of the YSON document when it is created:

```bash
$ yt create document //tmp/my_test_doc --attributes '{value=hello}'
3f08-6c0ee0-3fe01a5-2c4f6104
$ yt get //tmp/my_test_doc
"hello"
```

Write a number to the YSON document and read it:

```bash
$ yt set //tmp/my_test_doc 123
#
$ yt get //tmp/my_test_doc
123
```

Write a complex structure to a YSON document and read it fully or partially:

```bash
$ yt set //tmp/my_test_doc '{key1=value1;key2={subkey=456}}'
#
$ yt get //tmp/my_test_doc
{
    "key1" = "value1";
    "key2" = {
        "subkey" = 456;
    };
}
$ yt get //tmp/my_test_doc/key2
{
    "subkey" = 456;
}
```

The node will have the `document` type:

```bash
$ yt get //tmp/my_test_doc/@type
"document"
```

Partially change a YSON document:

```bash
$ yt set //tmp/my_test_doc/key1 newvalue1
```

Remove a YSON document:

```bash
$ yt remove //tmp/my_test_doc
```

## Limits { #limits }

YSON documents are read and written via the Cypress master server, so they cannot be used as a high-load object database. A reasonable limit is single-digit [RPS](https://en.wikipedia.org/wiki/Queries_per_second). Note that since the master server memory stores this data as a tree, the amount of data that can be saved to a YSON document is very limited.

Kilobytes can be considered a reasonable limit for a single document. The total volume of all user documents must not exceed single-digit megabytes. Such nodes are usually used to store small pieces of structured metadata, configuration, etc.

## System attributes { #attributes }

Besides the attributes inherent to all Cypress nodes, documents have the following additional attributes:

| **Attribute** | **Type** | **Description** |
| ----------- | ------- | ------------------------------------------------------------ |
| `value` | `any` | Full contents of the document. This attribute enables you to specify the contents of the document when it is created. The attribute is [opaque](../../../user-guide/storage/attributes.md#system_attr), which means that when you read all attributes of a node without a filter, it will be displayed as [entity](../../../user-guide/storage/yson.md#entity). |

