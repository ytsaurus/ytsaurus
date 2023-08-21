# Objects

This section describes the main [object types](../../../user-guide/storage/objects.md#object_types) in the {{product-name}} system. A table with the key [attributes of each object](../../../user-guide/storage/objects.md#attributes) is given and the concept of an [object ID](../../../user-guide/storage/objects.md#object_ids) is described.

## Object types { #object_types }

The main object types in {{product-name}} are:

* [Files](../../../user-guide/storage/objects.md#files).
* [Tables](../../../user-guide/storage/objects.md#tables).
* [Links](../../../user-guide/storage/objects.md#links).
* [YSON documents](../../../user-guide/storage/objects.md#yson_doc).
* [Primitive types](../../../user-guide/storage/objects.md#primitive_types).
* [Internal objects](#internal_objects).

The description of objects takes up most of the meta-information which is stored in replicated form in the memory of the [Cypress](../../../user-guide/storage/cypress.md) master servers. Cypress contains various system information as well as indications of where user data is stored.

For any Cypress node, there may be several versions, because the state of the node may look different in the context of each [transaction](../../../user-guide/storage/transactions.md).

### Files { #files }

These are Cypress nodes of the **file** type designed to store large binary data in the system.
Files consist of multiple chunks and [are chunk owners](../../../user-guide/storage/chunks.md#attributes).
Chunks are organized as a special tree-like data structure in which leaves are chunks and intermediate nodes are lists of chunks.

For more information, see [Files](../../../user-guide/storage/files.md).

### Tables { #tables }

These are nodes of the **table** type designed to store large user data. There are two types of tables: [static](../../../user-guide/storage/static-tables.md) and [dynamic](../../../user-guide/dynamic-tables/overview.md). Logically, a table is a sequence of rows. Each row consists of columns.
Column values are typified and can contain random structured data supported by the [YSON](../../../user-guide/storage/yson.md) language.

### Links { #links }

Links are nodes of the **link** type that refer to other objects along the path specified when the object is created.
All accesses to the node links are automatically redirected to the target object.
For more information, see [Links](../../../user-guide/storage/links.md).

### YSON documents { #yson_doc }

A YSON document is a Cypress node of the **document** type designed to store random YSON structures.

Requests and modifications within the document are supported. Addressing within the document is performed using the [YPath](../../../user-guide/storage/ypath.md) language.

For more information, see [YSON documents](../../../user-guide/storage/yson-docs.md).

For more information about the YSON format, see [YSON.](../../../user-guide/storage/yson.md)

### Primitive types { #primitive_types }

A Cypress node can contain a value of the primitive type: string, number, dict, list. The table contains a description of the primitive types of Cypress nodes.

| Type name | Description |
| -------------- | --------------------------------|
| `string_node` | A Cypress node containing a string |
| `int64_node` | A Cypress node containing a signed integer |
| `uint64_node` | A Cypress node containing an unsigned integer |
| `double_node` | A Cypress node containing a real number |
| `map_node` | Dict in Cypress (keys are strings, values are other nodes). By analogy with a file system, this is a folder |
| `list_node` | An ordered list in Cypress (values are other nodes) |
| `boolean_node` | A Cypress node containing a Boolean value |

### Internal objects { #internal_objects }

In addition to the objects listed above, there are internal objects: transactions, chunks, accounts, users, and groups. Such objects are not subject to versioning.
The internal objects listed in the table:

| Object name | Description |
| -------------- | ------------------------------------------------------------ |
| `transaction` | [Transaction](../../../user-guide/storage/transactions.md) |
| `chunk` | Chunk |
| `chunk_list` | Chunk list |
| `erasure_chunk` | Erasure-chunk |
| `account` | Account |
| `user` | User |
| `group` | Group |

## Object IDs { #object_ids }

Each object has a unique **ID** that is a 128-bit number whose format coincides with the GUID. This number can be represented as four 32-bit numbers: `a-b-c-d`. Usually when an ID is typed, components `a`, `b`, `c`, and `d` are written in hexadecimal form. Each of these components has its own meaning and is described in the table.

| Component | Purpose |
| :---------: | ------------------------------------------------------------ |
| `a` | [The number of the epoch](https://en.wikipedia.org/wiki/Unix_time) in which the object was created |
| `b` | The mutation number within the epoch in which the object was created |
| `c[16..31]` | The master group ID (cell id) is a unique 16-bit number that unambiguously identifies the cluster. This ID can be found in the cluster web interface opposite the master clusters |
| `c[0..15]` | Object type |
| `d` | Hash is a pseudo-unique random number selected at the time of object creation. |

Thus, the object ID depends on the cluster on which the object was created, as well as on the point in time (in terms of mutation numbers) when the object was created. More than one object can be created within a single mutation. They will differ in hash value. This difference is guaranteed by the hash generation mechanism. ID example: `28bb5-75b04b-3fe012f-8b4eda94`.

## Object attributes { #attributes }

All system objects have the attributes listed in the table:

| **Attribute** | **Type** | **Value** |
| :---------------------- | --------------- | --------------------------------------------------------- |
| `id` | `string` | Object ID |
| `type` | `string` | Object type |
| `ref_counter` | `integer` | The number of [strong links](../../../user-guide/storage/objects.md#ref_counter) to the object |
| `supported_permissions` | `array<string>` | List of supported access permissions |
| `effective_acl` | `Acl` | Effective object ACL |

Some system objects have additional attributes: `inherit_acl`, `acl`, and `owner`. They affect access to this object. These attributes are called an **access control descriptor** (ACD). For more information about the ACD, see [Managing access](../../../user-guide/storage/access-control.md).

### Reference counters { #ref_counter }

**Reference counters** are used to track the lifetime of an object. An object lives in the system as long as there are **strong links** to it. The number of strong links can be found in the ref_counter attribute. As soon as this number reaches zero, the object turns into a **zombie** and is subject to removal. It is not removed instantly: there is a special **removal queue** (GC queue) from which zombies are taken in portions of a controlled size and destroyed. The size is set by the internal configuration parameters of the system. Once the object has become a zombie, you can no longer access it, even by explicitly building a path by ID.

