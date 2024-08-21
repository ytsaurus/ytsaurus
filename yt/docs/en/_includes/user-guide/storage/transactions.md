# Transactions

This section describes transactions as applied to [Cypress](../../../user-guide/storage/cypress.md), and [static tables](../../../user-guide/storage/static-tables.md), [locks](#locks), and [versioning](#versioning) of Cypress objects.
The transactional model of dynamic tables is described in the [Multiversioning and transaction processing of dynamic tables](../../../user-guide/dynamic-tables/transactions.md) section.

## General information { #transactions }

The {{product-name}} system supports transactions, but differs in a number of ways from the classic [transaction processing model](https://en.wikipedia.org/wiki/Online_transaction_processing):

- Transactions can last minutes or hours.
- Isolation is configurable.
- There are no foreign table keys.
- Transactions can affect other Cypress nodes in addition to tables.

{{product-name}} ensures the following properties of the transaction processing system:

- **Atomicity**. {{product-name}} guarantees that the transaction will not be partially committed in the system. Either all or none of its sub-operations will be executed. Changing the data of a single Cypress node within a single command (for example, the set command) is atomic.
- **Consistency**. Changes introduced by committing a transaction maintain data consistency in Cypress nodes and static tables.
- **Isolation**. Unlike traditional transaction processing systems, {{product-name}} enables you to choose isolation on a per-transaction basis. {{product-name}} behavior corresponds to the [Read committed](https://en.wikipedia.org/wiki/Isolation_(database_systems)) isolation level when interacting with Cypress and [Serializable](https://en.wikipedia.org/wiki/Isolation_(database_systems)) when interacting with static tables within transactions.
- **Durability**. {{product-name}} guarantees the safety of changes after the transaction is committed. A single server hardware failure, power outage, or system shutdown cannot cause a loss of made changes. Transactions in the system can survive a system shutdown and continue running after the system is restored.

When you perform most actions in the system, you can specify which transaction those actions should be performed under. If a transaction is not explicitly specified, basic actions involving Cypress nodes, such as creating a new node or reading and writing a node's attributes, are performed atomically. However, more complex actions require the transaction to be initiated explicitly. One such example is reading a table: without a transaction and a snapshot lock on the table, there are no guarantees that data will not be deleted while you are working with it. However, if the data physically exists and can be read with a single query, that query returns a consistent state of the table.

Transactions are objects of the `transaction` type. A list of all system transactions can be found in Cypress at `//sys/transactions`.
A transaction can have another transaction as a parent. Transactions form a tree whose roots are transactions without parents, also called topmost transactions. A list of all topmost transactions is available at `//sys/topmost_transactions`.

Transactions are divided into **master transactions** and **tablet transactions**. Master transactions enable you to perform operations on the master meta-information. Tablet transactions enable you only to write data to dynamic tables.

## Master transactions { #master_transactions }

Transactional processing within master servers applies to versionable [objects](../../../user-guide/storage/objects.md). Examples of such objects are files, tables, and folders.

### Creating a transaction { #create }

To create a transaction, run the `start_tx` command.
You can specify the parent transaction in the `parent_id` option and define the transaction's time to live (TTL) in the `timeout` option.
By default, the upper TTL limit for a transaction within the system is one hour. If you specify a `timeout` of more than one hour, it will be equal to the limit.
Time to live starts from the moment of the `start_tx` call or from the last [`ping_tx`](#ping) call.

### Extending a transaction's TTL { #ping }

To extend a transaction's TTL, run the `ping_tx` command.
Each execution extends the transaction's TTL by a time interval equal to `timeout`. If the time since transaction creation or the last execution of the `ping_tx` command exceeds `timeout`, the transaction will be aborted.

### Completing a transaction { #commit }

You can abort a transaction using the `abort_tx` or successfully complete it using the `commit_tx` command.

Aborting a transaction also aborts all its nested transactions.
To successfully complete a transaction that includes nested transactions, you must first run the `commit_tx` command on all nested transactions. Attempting to run `commit_tx` on the parent transaction first will result in an error.
In all other cases, completing a transaction cannot cause an error.

{{product-name}} uses [pessimistic locks](#locks), so possible conflicts are detected as they occur — when locks are acquired and objects are created within transactions rather than when a transaction is completed.

### Transaction attributes { #attributes }

In addition to the attributes [inherent to all objects](../../../user-guide/storage/objects.md#attributes), transactions have the following attributes:

| **Attribute** | **Type** | **Description** | **Mandatory** |
| ------------------------ | ------------------ | ------------------------------------------------------------ |-----------------------|
| `timeout` | `integer` | Transaction timeout in ms. May be omitted for some system transactions. | No |
| `title` | `string` | Text description string. This attribute is filled in automatically for all system transactions and for user transactions only if the user specifies it themselves when creating a transaction. | No |
| `last_ping_time` | `DateTime` | Time when the transaction's TTL was last extended. May be missing for some system transactions. | No |
| `parent_id` | `Guid` | Parent transaction ID. | Yes |
| `start_time` | `DateTime` | Transaction creation time. | Yes |
| `nested_transaction_ids` | `array<Guid>` | A list of nested transaction IDs. | Yes |
| `staged_object_ids` | `array<Guid>` | A list of IDs of objects that the transaction temporarily owns. | Yes |
| `branched_node_ids` | `array<Guid>` | A list of branched Cypress node IDs. | Yes |
| `locked_node_ids` | `array<Guid>` | A list of locked Cypress node IDs. | Yes |
| `lock_ids` | `array<Guid>` | A list of IDs of locks created in the transaction. | Yes |
| `resource_usage` | `ResourceUsageMap` | An attribute that shows the use of resources in a given transaction for each affected account. | Yes |

{% note info "Note" %}

Transactions created by the system always have the filled in `title` attribute. It contains a description of the process that created the transaction.
Users are also encouraged to use this attribute to describe the purpose of the transaction.

{% endnote %}

## Locks { #locks }

Versioning of Cypress nodes is related to the concept of locks. By acquiring a lock on the node, the transaction expresses its intention to work with the node in [locking mode](#locking_mode). If the transaction manages to acquire a lock, it is guaranteed that:
- It is allowed to work with this node in the specified manner.
- The node for this transaction is branched.

{% note info "Note" %}

Note that acquiring a lock on a node creates a branched version of that node. Furthermore, the node may already have existing branches.

The presence of a lock on a node always means that the node has a branched version. However, the opposite is not always true: in certain scenarios, a node may be branched but not locked. An attempt to work with such a node will always result in acquiring a lock on it. Branching and acquiring a lock are related, but different things.

{% endnote %}

A lock is a full-fledged object that has its own ID. A list of all locks in the system is available at `//sys/locks`. We recommend using an address of the `#lock-id` form to access a specific lock.

A list of locks acquired by a transaction is displayed in its `lock_ids` attribute.

### Locking modes { #locking_mode }

The available locking modes are: `snapshot`, `exclusive`, and `shared`.
A locking mode defines the list of allowed transaction actions, as well as the ability to acquire other locks:

- `snapshot`: The transaction can read but not modify the node. The lock is used to obtain a read-only copy of a Cypress node state in the context of the transaction and freeze the state of that node.

   {% note info "Note" %}

   A snapshot lock is acquired only on the node itself, but not on the path to it in Cypress. If you continue accessing the node using its path, you can get a new node placed on the same path.
   To ensure access to the snapshot version of a node, use the node's `id`, which is returned by the `lock` [command](../../../api/commands.md#lock).

   {% endnote %}

- `exclusive`: The transaction can modify the node state. Other transactions cannot change the node.

- `shared`: The transaction can modify a certain part of the node state. Other transactions can still change other parts of this node.

   There are three standard scenarios for using this lock:
   - Concurrently appending data to a table or file from multiple transactions. In this case, you can only append data, but not overwrite it.
   - Concurrently creating several differently named subfolders within the same folder from multiple transactions. For example, transaction T1 can be started and create (or delete) the `//tmp/a` node and transaction T2 can be started and create (or delete) the `//tmp/b` node. Each of them will acquire a separate `shared` lock on `//tmp`. To detect conflicts, each lock has the `child_key` attribute indicating which key (subfolder) is locked by it.
   - Concurrently creating several differently named attributes within the same node from multiple transactions. For example, transaction T1 can be started and set (change, delete) the `//tmp/@a` attribute and transaction T2 can be started and set (change, delete) the `//tmp/@b` attribute. To detect conflicts, each lock has the `attribute_key` attribute indicating which key (attribute name) is locked by it.

{% note info "Note" %}

Transactions can be nested. "Other transactions" refers to transactions that are unrelated to this transaction, meaning that they do not share a common ancestor with it. In particular, nested transactions can result in more than one `exclusive` lock on a node.

{% endnote %}

### Implicit locks { #implicit_locks}

A transaction can acquire locks either explicitly using the `lock` command or implicitly. Implicit acquisition of locks can occur in case of certain interactions with Cypress nodes, for example:

- Creating a node is accompanied by an acquisition of an `exclusive` lock.
- Writing to a table or file results in acquiring a `shared` lock if you are adding data to a table and an `exclusive` lock if you are overwriting it.
- Creating a new entry in the folder, as well as changing or deleting an existing entry is accompanied by an acquisition of a `shared` lock with the corresponding `child_key`.
- Creating a new node attribute, as well as changing or deleting an existing attribute is accompanied by an acquisition of a `shared` lock with the corresponding `attribute_key`.

### Lock compatibility { #locks_compatibility}

Some lock combinations can be acquired concurrently. The formal rules are as follows:

- A `snapshot` lock can always be acquired. If this transaction has already acquired a `snapshot` lock, an attempt to acquire the lock again is completed without errors and has no effect.
- A `shared` or `exclusive` lock cannot be acquired if the transaction or any of its ancestors has already acquired a `snapshot` lock.
- A `shared` or `exclusive` lock cannot be acquired if another transaction that is not an ancestor of the given one has already acquired an `exclusive` lock.
- An `exclusive` lock cannot be acquired if another transaction that is not an ancestor of the given one has already acquired a `shared` lock.
- A `shared` lock with the specified `child_key` cannot be acquired if another transaction that is not an ancestor of the given one has already acquired a `shared` lock with the same `child_key`.
- A `shared` lock with the specified `attribute_key` cannot be acquired if another transaction that is not an ancestor of the given one has already acquired a `shared` lock with the same `attribute_key`.
- A `shared` lock without `child_key` and `attribute_key` can be acquired despite any other `shared` locks.

### Lock operations { #lock_operations}

There are two commands to work with locks: `lock` and `unlock`.

The `lock` command enables you to acquire a lock on a Cypress node in a specified transaction.

The `unlock` command does the opposite: it removes all explicit locks from the node for a given transaction, both those already acquired and those still in the [lock queue](#locking_queue).
The lock can only be removed if the locked branched version of the node contains no changes compared to the original. Consequently, an explicit `snapshot` lock can always be removed. Otherwise the command will end with an error.

Locks are automatically removed at the end of the transaction, whether successful or unsuccessful. Therefore, there is usually no need to remove them manually.
We recommend using the `unlock` command only when you need to acquire and release locks without completing the transaction. Such transactions are usually designed to manage the synchronization of third-party services.

### Lock queue { #locking_queue }

Each Cypress node can have its own lock queue.

By default, the `lock` command tries to acquire a lock and returns an error if the node is already locked. If you specify the `waitable` parameter equal to `true` in the `lock` command, the lock will be queued.
To find out whether a lock is in the queue, request the `state` attribute. If the lock is in the queue, it will be `pending`, if not, it will be `acquired`.

{% note warning "Attention" %}

For a lock to be actually acquired, for example exclusively, its state must become `acquired`.
To acquire a lock:

- Call the `lock` command.
- Monitor the `state` in the cycle until it takes on the `acquired` value.

{% endnote %}

### Lock attributes { #lock_attributes}

In addition to the attributes inherent to all objects, locks have the following attributes:

| **Attribute** | **Type** | **Description** |
| ---------------- | -------- | ------------------------------------------------------------ |
| `state` | `string` | Lock state: `pending` or `acquired`. |
| `transaction_id` | `Guid` | ID of the transaction that took the lock. |
| `mode` | `string` | Locking mode: `shared`, `exclusive`, or `snapshot`. |
| `child_key` | `string` | The key on which the lock is acquired. For the `shared` type only. |
| `attribute_key` | `string` | The name of the attribute on which the lock is acquired (for the `shared` type only) |

## Versioning { #versioning }

Changing the node state by a transaction is the following three-phase process:

1. Transaction `T` acquires the lock on node `N`. Version `N:T` appears for node `N` and it is formed as follows:

   - For a `snapshot` lock, it branches off from version `N:T'` where `T'` is the closest ancestor of `T` that branched off `N`.
   - For `shared` and `exclusive` locks, `N:T''` branches off from `N:T'` where `T''` is the child of `T'` ; `N:T'''` branches off from `N:T''` where `T'''` is the child of `T''` and so on up to `N:T`. In other words, a chain of branched node versions is created for each transaction from `T'` to `T`.

   If there is no such `T'`, the real version of `N` is used. It does not matter for this description whether the lock was acquired explicitly or implicitly.

2. Transaction `T` works with node `N`, with the actual changes accumulated in its version `N:T`.

3. Transaction `T` completes successfully and the changes it made to `N:T`, if any, get merged into the version from which version `N:T` was branched. Thus, these changes become visible to the parent transaction or to all if transaction `T` was a topmost transaction.

## Using transactions in operations

When starting operations, the scheduler creates a set of transactions to provide some atomicity of data processing in the operation. To learn more about how this works, see [Transactions in data processing](../../../user-guide/data-processing/operations/overview.md#transactions).
