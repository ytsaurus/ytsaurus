# Meta information tree

This section describes Cypress, the meta information tree. Cypress contains various system information as well as indications of where user data is stored. This section includes three subsections describing a [general tree view](#description), [Cypress node attributes](#attributes), and [TTL for Cypress nodes](#TTL).

## General tree view { #description }

To a user, Cypress looks like a Linux [file system](https://en.wikipedia.org/wiki/File_system) tree but with a number of significant differences. First, every tree node has an associated [collection of attributes](../../../user-guide/storage/attributes.md), including some user-defined ones. Second, the tree is [transactional](../../../user-guide/storage/transactions.md). Third, files and directories, as well as [other objects](../../../user-guide/storage/objects.md), can serve as Cypress nodes. Like a file system, Cypress supports an [access control system](../../../user-guide/storage/access-control.md).

Cypress is rooted at `/` which has **map_node** type (that is, it's a directory). Cypress nodes are addressed using [YPath](../../../user-guide/storage/ypath.md).

Example paths: `//tmp` is the temporary directory, `//tmp/@` is a pointer to the directory attributes, `//tmp/table/@type` is the path to the `type` attribute of the `//tmp/table` node.

Using YPath, you can represent Cypress as follows:

```
/
  /home
    /user1
      /table
        /@id
        /@chunk_ids
        /@type
        ...
      ...
    /user2
    ...
  /tmp
  /sys
    /chunks
      ...
    ...
  ...
```

You can manipulate Cypress via a CLI.

## Cypress node attributes { #attributes }

In addition to attributes common to all objects, Cypress nodes have additional attributes listed in the table:

| **Attribute** | **Type** | **Value** |
| -------------------------- | ------------------ | ------------------------------------------------------------                                                                  |
| `parent_id` | `string` | Parent node ID (none for the root) |
| `locks` | `array<Lock>` | [List of locks](../../../user-guide/storage/transactions.md) taken out on a node |
| `lock_mode` | `LockMode` | Current node [lock mode](../../../user-guide/storage/transactions.md) (transaction-dependent) |
| `path` | `string` | Node absolute path |
| `key` | `string` | Key to access this node in its parent folder (if the node is so nested) |
| `creation_time` | `DateTime` | Node [create](#time_attributes) time |
| `modification_time` | `DateTime` | Node [most recent modification](#time_attributes) time |
| `access_time` | `DateTime` | Node [most recent access](#time_attributes) time |
| `expiration_time` | `DateTime` | Time [automatically to delete](#TTL) a node. Optional attribute |
| `expiration_timeout` | `DateTime` | A timeout for the [automatic deletion](#TTL) of a node if it has not been accessed. Optional attribute |
| `access_counter` | `integer` | Number of times a node has been accessed since being created |
| `revision` | `integer` | Node [revision](#time_attributes) |
| `resource_usage` | `ClusterResources` | Cluster resources appropriated by a node |
| `recursive_resource_usage` | `ClusterResources` | Cluster resources appropriated by a node and its entire subtree |
| `account` | `string` | Account used to keep track of the resources being used by a specific node |
| `annotation` | `string` | Human-readable [summary description](../../../user-guide/storage/annotations.md) of an object |

Each node has its own attribute responsible for access control. Therefore, its attributes include `inherit_acl`, `acl`, and `owner`. For more information, see the [Access control system](../../../user-guide/storage/access-control.md) section.

### Time attributes { #time_attributes }

The `creation_time` attribute stores the node create time. The `modification_time` attribute stores the time of the last update of the node and node attribute. `modification_time` does not track child node updates, that is, `modification_time` for `map_node` does not change if there are changes somewhere deep in the tree.

When a node is created and every time a node is modified, the system updates its `revision` attribute. It stores a non-negative integer. The revision number is guaranteed to increase in a strictly monotonous manner over time. You can use revisions to verify that a node has not updated. `revision` updates together with `modification_time`.

The `access_time` attribute stores the most recent node access time. Attribute access does not count. In addition, to improve performance, the system does not update this attribute for every access transaction but rather accumulates such transactions and updates `access_time` approximately once per second.

{% note warning "Attention!" %}

In rare cases, an attribute may have been accessed without an `access_time` update because of a master server fault.

{% endnote %}

Most commands used for reads and writes include the `suppress_access_tracking` and the `suppress_modification_tracking` options that disable `access_time`, `modification_time`, and `revision` updates, respectively. for reading and writing. In particular, the web interface uses `suppress_access_tracking`, such that content viewing via the web UI does not result in `access_time` updates.

{% note info "Note" %}

In the event that a transaction creates or modifies a node, the above attributes are set once during updates within the transaction. Thus, a node may become visible in parent transactions much later than its `creation_time`: only after a commit of the relevant transaction.

{% endnote %}

## Cypress node TTL { #TTL }

Cypress can delete nodes automatically at a specified moment in time or if nodes are not accessed for a certain length of time. This feature is controlled by the `expiration_time` and the `expiration_timeout` attributes. By default, these attributes are not there, so the system will not delete a node automatically. For TTL to function, you need:
- to set `expiration_time` to a moment in time when the node is to be deleted. If it is a composite node, this will also delete its entire subtree.
- to set `expiration_timeout` to a time interval during which there have to be no attempts to access the node (and its entire subtree if it is a composite node) for it to be deleted.

The moment in time has to be either an isoformat string or an integer denoting the number of milliseconds since the epoch. These two methods are equivalent:

```bash
yt set //home/project/path/table/@expiration_time '"2020-05-16 15:12:34.591+03:00"'
yt set //home/project/path/table/@expiration_time '1589631154591'
```

A time interval is specified in milliseconds:
```bash
# Delete a node if "left alone" for a week.
yt set //home/project/path/table/@expiration_timeout 604800000
```

{% note warning "Attention!" %}

You cannot restore data deleted using this mechanism. Use it with caution.

{% endnote %}

You can modify these attributes within transactions; however, only their committed values will take effect.

To be able to set these attributes for a node, you need to have the [right](../../../user-guide/storage/access-control.md) to `write` to the node itself same as for many other attributes as well as the `remove` privilege to the node and its entire subtree because a delete is being requested in effect, albeit a deferred one. The `write` privilege is sufficient to delete these attributes.

The system provides no guarantee that the delete will occur exactly at the time requested. In real life, the delete occurs within single seconds of the specified moment in time.

A node is not automatically deleted if at the specified moment in time it is subject to locks other than `snapshot`. The system will delete the node when all locks are released. You can use this property to extend a node's time-to-live artificially.

When you copy and move a node, `expiration_time` and `expiration_timeout` are reset by default, so the copy will not automatically delete. Commands include the `preserve-expiration-time` and the `preserve-expiration-timeout` options that enable you to change their behavior.

{% note warning "Attention!" %}

A number of API calls that create temporary tables set such tables' `expiration_time`/`expiration_timeout` to purge them automatically. You must keep that in mind and not store important data in such tables.

{% endnote %}
