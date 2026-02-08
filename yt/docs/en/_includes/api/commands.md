# Commands

This section lists all the commands available in the {{product-name}} system API and includes a full description of their options.

## Structure { #structure }

To put it simply, each command has the following structure:

- Command name (string).
- Description of the input and output formats (a [YSON string](../../user-guide/storage/data-types.md#yson) with attributes).
- Parameters of executed action (a YSON structure).
- Input and output data streams.

Each command also defines the type of its input and output data. Below all the possible options for the input and output data:

- No data (`null`).
- The data is a `binary` stream. For example, [file](../../user-guide/storage/files.md) contents.
- The data is [structured](../../user-guide/storage/formats.md#structured_data) (`structured`). For example, regular [Cypress](../../user-guide/storage/cypress.md) nodes.
- The data is a `tabular` stream. For example, [table](../../user-guide/storage/objects.md#tables) contents.

In addition to that, each command can be:

- Mutating or not (whether it changes anything in the metastate or not).
- Light or heavy (light commands only transmit command parameters within a query, but heavy commands write or read the data stream).

### Formats { #formats }

If your command works with structured or tabular data, you need to specify a [format](../../user-guide/storage/formats.md) for it. For the input stream, use the `input_format` parameter, and for the output stream, use the `output_format` parameter, respectively.

For structured data formats, `YSON` (default) and `JSON` formats are supported. For tabular data, there exist many [formats](../../user-guide/storage/formats.md).

### Retries { #retry }

A command retry is an option to repeat the query in the event of transient (intermittent) errors. It is expected that the response to a query retry would be indistinguishable from the response to the original query, as if there were no transient errors. It doesn't mean, however, that the response to a retry will be totally identical to the original query response (that is, only standard isolation is guaranteed).

Availability of retry options and the mechanism of retries depends on the command properties.

#### Light commands { #light }

For non-mutating light commands, you can repeat the original query.

The mutating light commands change the system status. That's why you need to hint the system that such a query has already been made. To do this, before executing the command, generate the `mutation_id` for the command. This is a standard GUID that consists of four 32-bit numbers in the HEX format separated by a dash (`-`).

Specify the `mutation_id` generated both in the parameters of both the original query and in the retries.  In addition, you need to add, to the original query, the `retry` parameter with the value of `false`. To the retried queries, add the `retry` parameter with the value of `true`. Some light mutating commands do not support retries (an example is `concatenate`). If you need to retry such commands, you can use transactions.

{% note info "Note" %}

`mutation_id` are usually kept for 5-10 minutes.

{% endnote %}


#### Heavy commands { #hard }

You can't retry heavy commands However, you can implement a retry mechanism by using transactions.

## Transactions { #transactions }

[Transactions](../../user-guide/storage/transactions.md) are an integral Cypress property. Many commands that interact with Cypress this way or another, are transactional. Each command or group of commands has a separate indication of whether it is transactional or not. Transactional commands support the following parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------------- | ------------- | --------------------- | ------------------------------------------------------------ |
| `transaction_id` | No | *null-transaction-id* | Current transaction ID. |
| `ping_ancestor_transactions` | No | `false` | Whether to ping all the parent transactions while running the operation. |

## Other query parameters

You can also specify prerequisites for your queries. Moreover, for light non-mutating queries, you can also specify where to read the data from. For more information, see [Query parameters](../../api/query-parameters.md).

## Working with transactions

For more information about transactions, see the [Transactions](../../user-guide/storage/transactions.md) article.

### start_tx

Command properties: **Mutating**, **Light**.

Semantics:

- Begin a new transaction in the context of the current transaction.
- The new transaction is a nested (internal) transaction for the given one.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `transaction_id` | No | *null-transaction-id* | ID of the current transaction (it will become a parent transaction for the transaction created by the command). |
| `ping_ancestor_transactions` | No | `false` | Whether to ping, when running the operation, all the parent transactions (to extend their TTL). |
| `timeout` | No | `15000` | Transaction TTL since the last extension (in ms). |
| `deadline` | No | `missing` | Transaction execution deadline (in UTC). |
| `attributes` | No | `missing` | Enables you to set attributes for the created transaction. |
| `type` | No | `master` | Enables you to set the transaction type: `master` or `tablet`. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: ID of the new transaction.

Example:

```bash
PARAMETERS { "transaction_id" = "0-54b-1-36223d20" }
OUTPUT "0-54c-1-bb49086d"
```

### ping_tx

Command properties: **Mutating**, **Light**.

Semantics:

- Update the transaction.

Detailed description.

- `ping_tx` pings the transaction on the server (including all the parent transactions if `ping_ancestor_transactions` is specified). This way you can extend the TTL for the transaction.
- If the transaction started at the time `s` with the timeout (TTL) of `t`, then the transaction will complete at `s+t` by default.
- If you ping the transaction at the time `r` (`s < r < s + t`), it will be extended until `r + t`.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `transaction_id` | Yes |                           | Parent transaction ID. |
| `ping_ancestor_transactions` | No | `false` | Whether to ping all the parent transactions while running the operation. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "transaction_id" = "0-54b-1-36223d20" }
```

### commit_tx

Command properties: **Mutating**, **Light**.

Semantics:

- Complete the transaction successfully.
- While there are some incompleted internal transactions, the outer transaction can't complete.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `transaction_id` | Yes |                           | Transaction ID. |
| `ping_ancestor_transactions` | No | `false` | Whether to ping all the parent transactions while running the operation. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "transaction_id" = "0-54b-1-36223d20" }
```

### abort_tx

Command properties: **Mutating**, **Light**.

Semantics:

- Abort the transaction.
- All the active internal transactions are aborted as well.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `transaction_id` | Yes |                           | Transaction ID. |
| `ping_ancestor_transactions` | No | `false` | Whether to ping all the parent transactions while running the operation. |
| `force` | No | `false` | Forcibly aborts the transaction even if this may result in consistency loss (in case of a tablet transaction and a [two-phase commit](https://en.wikipedia.org/wiki/Two-phase_commit_protocol)). |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "transaction_id" = "0-54b-1-36223d20" }
```

### start_transaction

{% note info %}

The HTTP API supports this command from version 4 onward. Earlier versions use the `start_tx` command.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Begin a new transaction in the context of the current transaction.
- The new transaction is a nested (internal) transaction for the given one.
- This is the API v4 equivalent of `start_tx` with structured output.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `transaction_id` | No | *null-transaction-id* | ID of the current transaction (it will become a parent transaction for the transaction created by the command). |
| `ping_ancestor_transactions` | No | `false` | Whether to ping, when running the operation, all the parent transactions (to extend their TTL). |
| `timeout` | No | `15000` | Transaction TTL since the last extension (in ms). |
| `deadline` | No | `missing` | Transaction execution deadline (in UTC). |
| `attributes` | No | `missing` | Enables you to set attributes for the created transaction. |
| `type` | No | `master` | Enables you to set the transaction type: `master` or `tablet`. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Transaction ID.

Example:

```bash
PARAMETERS { "transaction_id" = "0-54b-1-36223d20" }
OUTPUT "0-54c-1-bb49086d"
```

### ping_transaction

{% note info %}

The HTTP API supports this command from version 4 onward. Earlier versions use the `ping_tx` command.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Update the transaction by pinging it to extend its TTL.
- This is the API v4 equivalent of `ping_tx` with structured output.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `transaction_id` | Yes |                           | Transaction ID to ping. |
| `ping_ancestor_transactions` | No | `false` | Whether to ping all the parent transactions while running the operation. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

Example:

```bash
PARAMETERS { "transaction_id" = "0-54b-1-36223d20" }
OUTPUT { }
```

### commit_transaction

{% note info %}

The HTTP API supports this command from version 4 onward. Earlier versions use the `commit_tx` command.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Complete the transaction successfully.
- While there are some incomplete internal transactions, the outer transaction can't complete.
- This is the API v4 equivalent of `commit_tx` with structured output.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `transaction_id` | Yes |                           | Transaction ID to commit. |
| `ping_ancestor_transactions` | No | `false` | Whether to ping all the parent transactions while running the operation. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

Example:

```bash
PARAMETERS { "transaction_id" = "0-54b-1-36223d20" }
OUTPUT { }
```

### abort_transaction

{% note info %}

The HTTP API supports this command from version 4 onward. Earlier versions use the `abort_tx` command.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Abort the transaction.
- All the active internal transactions are aborted as well.
- This is the API v4 equivalent of `abort_tx` with structured output.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `transaction_id` | Yes |                           | Transaction ID to abort. |
| `ping_ancestor_transactions` | No | `false` | Whether to ping all the parent transactions while running the operation. |
| `force` | No | `false` | Forcibly aborts the transaction even if this may result in consistency loss (in case of a tablet transaction and a two-phase commit). |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

Example:

```bash
PARAMETERS { "transaction_id" = "0-54b-1-36223d20" }
OUTPUT { }
```



## Working with Cypress

For more information about the metainformation tree, see [Cypress](../../user-guide/storage/cypress.md).

{% note info "Note" %}

All the commands used to work with Cypress are transactional.

{% endnote %}

### create { #create }

Command properties: **Mutating**, **Light**.

Semantics:

- Create a node of a specified type in Cypress.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ----------------- | ----------        | ------------------------- | ------------------------------------------------------------                                                                                                                                                                                     |
| `path` | Yes |                           | Path to the node in Cypress. At default settings, the *path* should not exist. |
| `type` | Yes |                           | [Node type](../../user-guide/storage/objects.md). |
| `recursive` | No | `false` | Whether to create intermediate nodes recursively. |
| `attributes` | No | `{}` | Enables you to set attributes for the created node. |
| `ignore_existing` | No | `false` | If the created node exists already, it is not recreated. In particular, the transmitted attributes are ignored. Moreover, both the existing and created nodes must have the same type, otherwise the query will return an error. |
| `lock_existing` | No | `false` | Set an [exclusive lock](../../user-guide/storage/transactions.md#locks) on the specified node even if it already exists. This parameter is only used together with `ignore_existing`. If the lock couldn't be set, the command fails. |
| `force` | No | `false` | If the specified node already exists, it is deleted and replaced with a new one. In this situation, the existing node can be of any type. When the node is recreated, its ID changes. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: ID of the created node.

Examples:

```bash
PARAMETERS { "path" = "//tmp/table" ; "type" = "table" }
OUTPUT "0-4-191-6c07cd58"
```

```bash
PARAMETERS { "type" = "user" ; attributes = { name = 'kulichek-robot' } }
OUTPUT "7727-417b1-1f5-d4f116ca"
```

```bash
PARAMETERS { "path" = "//tmp/document" ; "type" = "document" ; attributes = { value = {} } }
OUTPUT "7727-417b1-1f5-d4f116ca"
```

### remove { #remove }

Command properties: **Mutating**, **Light**.

Semantics:

- Delete the Cypress node.
- Deletes the node successfully, even if a non-empty subtree is growing from it.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `path` | Yes |                           | Path to the node in Cypress. The path *must* exist. |
| `recursive` | No | `false` | Enables you to delete the entire subtree in the case when the deleted node is a composite type. |
| `force` | No | `false` | Enables you to treat the command as successfully executed if the deleted node is missing. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "path" = "//tmp/table" }
```

### set { #set }

Command properties: **Mutating**, **Light**.

Semantics:

- Write new content into the Cypress node.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `path` | Yes |                           | Path to the node in Cypress. If `recursive == false`, then the path *must* exist (except, probably, the last token). |
| `recursive` | No | `false` | Create all non-existent intermediate nodes on the path. |
| `force` | No | `false` | Enables you to modify any Cypress node instead of only attributes and documents. |

Input data:

- Type: `structured`.
- Value: Node content.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "path" = "//tmp/node" }
INPUT {
    "my_integer" = 4 ;
    "my_double" = 2.718281828 ;
    "map" = { "a" = 1 ; "b" = 2 } ;
    "list" = [ 1, 2, 3 ]
}
```

### multiset_attributes

Command properties: **Mutating**, **Light**.

Semantics:

- Set several attributes at the specified path (if the attributes exist already, they are overwritten).

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `path` | Yes |                           | Path to attributes of a node in Cypress. The path *must* exist. |

Input data:

- Type: `structured`.
- Value: A map node that contains new attribute values.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "path" = "//tmp/node/@" }
INPUT {
    "attribute1" = 4 ;
    "attribute2" = "attribute2 value";
}
```

### get { #get }

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get the content of the Cypress node.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `path` | Yes |                           | Path to the node in Cypress. The path *must* exist. |
| `attributes` | No | `[]` | A list of attributes that need to be obtained along with each node. |
| `max_size` | No | `missing` | Sets a limit on the number of children that will be issued in the case of virtual composite nodes (for regular map nodes, this option doesn't make sense). |
| `ignore_opaque` | No | `false` | Ignore the `opaque` attribute when executing a query (never use this option without explicit advice from {{product-name}} developers). |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Node content.

Example:

```bash
PARAMETERS { "path" = "//tmp/node" }
OUTPUT {
    "my_integer" = 4 ;
    "my_double" = 2.718281828 ;
    "map" = { "a" = 1 ; "b" = 2 } ;
    "list" = [ 1, 2, 3 ]
}
```

### list { #list }

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get a list of descendants for the Cypress node.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `path` | Yes |                           | Path to the node in Cypress. The path *must* exist. |
| `attributes` | No | `[]` | A list of attributes that need to be obtained along with each node. |
| `max_size` | No | `missing` | Limits the number of descendants output. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`
- Value: A list of the node's descendants.

Example:

```bash
PARAMETERS { "path" = "//tmp/node" }
OUTPUT [
    "home" ;
    "sys" ;
    "statbox" ;
    "tmp"
]
```

### lock

Command properties: **Mutating**, **Light**.

Semantics:

- Set a lock on the Cypress node.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------- | -------------     | ------------------------- | ------------------------------------------------------------                                                                                                                                                                                |
| `path` | Yes |                           | Path to the node in Cypress. The path *must* exist. |
| `mode` | No | `exclusive` | Lock type (snapshot, shared, exclusive). |
| `waitable` | No | `false` | In case of a conflict, it's doesn't fail but is enqueued. You can find out whether the lock was actually set using the `state` attribute of the lock object. See [Transactions](../../user-guide/storage/transactions.md#locking_queue). |
| `child_key` | No |                           | The key in the dictionary on which the lock is taken (for the `shared` type only). |
| `attribute_key` | No |                           | The name of the attribute on which the lock is taken (for the `shared` type only). |

{% note info "Note" %}

Be sure to specify the transaction ID for this command.

{% endnote %}

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: ID of the created lock and ID of the branched node.

Example:

```bash
PARAMETERS { "path" = "//tmp/node" }
OUTPUT {
OUTPUT   "lock_id" = "0-1-3fe00c8-353e9ba4";
OUTPUT   "node_id" = "0-1-3fe012f-9ad48d90";
OUTPUT }
```

### unlock

{% note warning "Attention" %}

Locks are automatically removed at the end of the transaction. Do not use the `unlock` command unless absolutely needed. See [Transactions](../../user-guide/storage/transactions.md#lock_operations).

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Release all the locks set for the selected transaction on the Cypress node.
- You can only release explicit locks.
- This command releases both already taken and enqueued locks.
- If the node is not locked, the command is considered successful.
- If the locked node version includes any changes compared to the original version, the unlock fails.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ---------------------------------------------------- |
| `path` | Yes |                           | Path to the node in Cypress. The path *must* exist. |



{% note info "Note" %}

Be sure to specify the transaction ID for this command.

{% endnote %}

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "path" = "//tmp/node" }
```

### copy { #copy }

Command properties: **Mutating**, **Light**.

Semantics:

- Copy the Cypress node to the new address.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------------------    | -------------     | ------------------------- | ------------------------------------------------------------                                                                                                                                                                                                      |
| `source_path` | Yes |                           | Path to the source node in Cypress. The path *must* exist. |
| `destination_path` | Yes |                           | Path where the copy will be created. The path *must not* exist. |
| `recursive` | No | `false` | Whether to create the skipped levels (map nodes) at the destination path. |
| `ignore_existing` | No | `false` | If the node already exists at `destination_path`, do nothing  (you can't use this parameter together with `force = %true`). |
| `lock_existing` | No | `false` | Set an [exclusive lock](../../user-guide/storage/transactions.md#locks) on the node at `destination_path`, even if it already exists. This parameter is only used together with `ignore_existing`. If the command fails to set a lock, it returns an error. |
| `force` | No | `false` | Allows you to specify an existing node to be replaced, as the destination path. |
| `preserve_account` | No | `false` | Whether to keep the accounts of the source nodes or use the account at the destination path. |
| `preserve_expiration_time` | No | `false` | Whether to copy the [`expiration_time`](../../user-guide/storage/cypress.md#TTL) attribute or leave it empty. |
| `preserve_expiration_timeout` | No | `false` | Whether to copy the [`expiration_timeout`](../../user-guide/storage/cypress.md#TTL) attribute or leave it empty. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: ID of the created node.

Example:

```bash
PARAMETERS {
    "source_path" = "//tmp/from" ;
    "destination_path" = "//tmp/to" ;
}
OUTPUT "0-4-191-6c07cd58"
```

### move { #move}

Command properties: **Mutating**, **Light**.

Semantics:

- Move the Cypress node to a new path.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------------------    | -------------     | ------------------------- | ------------------------------------------------------------                                             |
| `source_path` | Yes |                           | Path to the source node in Cypress. The path *must* exist. |
| `destination_path` | Yes |                           | A new path in Cypress. The path *must not* exist. |
| `recursive` | No | `false` | Whether to create the skipped levels (map nodes) at the destination path. |
| `force` | No | `false` | Allows you to specify an existing node to be replaced, as the destination path. |
| `preserve_account` | No | `false` | Whether to keep the accounts of the source nodes or use the account at the destination path. |
| `preserve_expiration_time` | No | `false` | Whether to copy the [`expiration_time`](../../user-guide/storage/cypress.md#TTL) attribute or leave it empty. |
| `preserve_expiration_timeout` | No | `false` | Whether to copy the [`expiration_timeout`](../../user-guide/storage/cypress.md#TTL) attribute or leave it empty. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS {
    "source_path" = "//tmp/from" ;
    "destination_path" = "//tmp/to" ;
}
```

### link { #link }

Command properties: **Mutating**, **Light**.

Semantics:

- Create a symbolic link to the object at the new path.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| --------------- | -------------     | ------------------------- | ------------------------------------------------------------                                                                                            |
| `link_path` | Yes |                           | Path where the link will be created. The path *must not* exist. |
| `target_path` | Yes |                           | Path to the source node in Cypress. The path *must* exist. |
| `attributes` | No | `missing` | Attributes of the node created as a result of the command. |
| `recursive` | No | `false` | Whether to create intermediate nodes recursively. |
| `ignore_existing` | No | `false` | Enables you to avoids operation failure if the node already exists and is a link. The call will return an error if the node exists but is not a link. |
| `lock_existing` | No | `false` | Set an exclusive lock on the specified node even if it already exists. It's only used together with the `ignore_existing` parameter. |
| `force` | No | `false` | Recreate the link if the path where the link is created already exists. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: ID of the created node.

Example:

```bash
PARAMETERS {
    "target_path" = "//tmp/from" ;
    "link_path" = "//tmp/to" ;
}
OUTPUT "0-4-191-6c07cd58"
```

### exists { #exists }

Command properties: **Non-mutating**, **Light**.

Semantics:

- Checks what are the node exists.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ---------------------------------- |
| `path` | Yes |                           | Path to the source node in Cypress. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: `true` or `false` string.

Example:

```bash
PARAMETERS {
    "path" = "//tmp/my_table/@_format" ;
}
```

### concatenate { #concatenate }

Semantics:

- Merge the set of files or tables (in the order in which their paths are listed).

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `transaction_id` | No | *null-transaction-id* | Current transaction ID. |
| `ping_ancestor_transactions` | No | `false` | Whether to ping all the parent transactions while running the operation. |
| `source_paths` | Yes |                           | The list of paths to input files or tables in Cypress. The paths *must* exist and all items at the paths should either be files or tables. |
| `destination_path` | Yes |                           | Path to a file or table in Cypress. The path *must* exist. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS {
    "source_paths" = ["//tmp/file1"; "//tmp/file2"];
    "destination_path" = "//tmp/file";
}
```

## Access control { #access-control }

For more information about access permissions, see [Access control](../../user-guide/storage/access-control.md).

{% note info "Note" %}

This set of commands is not transactional.

{% endnote %}

### add_member

Command properties: **Mutating**, **Light**.

Semantics:

- Add to a group a user or another group.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `member` | Yes |                           | The name of the user or group that needs to be added to the group. |
| `group` | Yes |                           | The name of the group into which you are adding a user or another group. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS {
    "group" = "admins";
    "member" = "devs";
}
```

### remove_member

Command properties: **Mutating**, **Light**.

Semantics:

- Add to a group a user or another group.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `member` | Yes |                           | The name of the user or group that needs to be deleted from the group. |
| `group` | Yes |                           | The name of the group that you are deleting a user or another group from. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS {
    "group" = "admins";
    "member" = "devs";
}
```

### check_permission

Command properties: **Non-mutating**, **Light**.

Semantics:

- Check if a user has a certain permission to access a certain Cypress node.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `path` | Yes |                           | Path to the node in Cypress. |
| `user` | Yes |                           | The name of the user that you are checking the permission for. |
| `permission` | Yes |                           | The name of the permission checked. |
| `columns` | No |                           | The list of columns the access to which should be checked. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS {
    "path" = "//sys/accounts/statbox";
    "permission" = "use";
    "user" = "bob";
}
```

## Working with files { #files }

For more information about files, see the [Files](../../user-guide/storage/files.md) article.

{% note info "Note" %}

All the commands used to work with files are transactional.

{% endnote %}

### write_file

Command properties: **Mutating**, **Heavy**.

Synonyms (supported, but not recommended).

- upload.

Semantics:

- Upload the content to the file.
- The file must exist.
- If the file `path` includes the `append=%true` attribute, the data is appended to the file; otherwise, the file is overwritten.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `transaction_id` | No | *null-transaction-id* | Current transaction ID. |
| `ping_ancestor_transactions` | No | `false` | Whether to ping all the parent transactions while running the operation. |
| `path` | Yes |                           | Path to a file in Cypress. The path *must* exist. |
| `compute_md5` | No | `false` | Whether to calculate the MD5 sum for the file written; if both `compute_md5=%true` and `append=%true`, but the file does not have the `md5` attribute (that is, previously it was written without `compute_md5=%true`), an error will occur. |

Input data:

- Type: `binary`.
- Value: File content.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS {"path" = "//tmp/file"}
INPUT this is sample file content
```

### read_file

Command properties: **Non-mutating**, **Heavy**.

Synonyms (supported, but not recommended).

- download.

Semantics:

- Get the content of the Cypress node.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `transaction_id` | No | *null-transaction-id* | Current transaction ID. |
| `ping_ancestor_transactions` | No | `false` | Whether to ping all the parent transactions while running the operation. |
| `path` | Yes |                           | Path to a file in Cypress. The path *must* exist. |
| `offset` | No | 0 | The position starting from which the data should be read. |
| `length` | No |                           | The length of the data to be read; by default, the file is read to the end. |

Input data:

- Type: `null`.

Output data:

- Type: `binary`.
- Value: File content.

Example:

```bash
PARAMETERS { "path" = "//tmp/file" }
OUTPUT this is sample file content
```

### start_distributed_write_file_session { #start_distributed_write_file_session }

Part of the distributed API. See [start_distributed_write_session](#start_distributed_write_session)

Command properties: **Mutating**, **Light**.

Semantics:

- Initialize a session of distributed writing to a file.
- Request cookies for distributed session participants.
- Not all requested cookies have to be used.
- The same cookie object can be reused to write a new fragment.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------------- | -------------    | ------------------------- | ------------------------------------------------------------             |
| `path` | Yes |                           | Path to a file in Cypress. |
| `cookie_count` | Yes |                           | Number of distributed session participants. Positive number. |
| `transaction_id` | No | *null-transaction-id* | Current transaction ID. |
| `ping_ancestor_transactions` | No | `false` | Whether to ping all the parent transactions while running the operation. |
| `timeout` | No |                           | Session TTL since the last ping (in ms). |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: A signed distributed session object along with a list of signed cookies of participants.

Example:

```bash
PARAMETERS {
    "path" = "//tmp/file";
    "cookie_count" = "2";
}

OUTPUT {
    "session" = "sample_signed_session";
    "cookies" = ["sample_signed_cookie_1"; "sample_signed_cookie_2"];
}
```

### ping_distributed_write_file_session { #ping_distributed_write_file_session }

Part of the distributed API.

Command properties: **Mutating**, **Light**.

Semantics:

- Pings the distributed session transaction on the server (including all parent transactions if `ping_ancestor_transactions` is specified), thereby extending the transaction's TTL.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| --------------------------     | -------------    | ------------------------- | ------------------------------------------------------------ |
| `session` | Yes |                           | Object of the signed distributed session. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

### finish_distributed_write_file_session { #finish_distributed_write_file_session }

Part of the distributed API.

Command properties: **Mutating**, **Light**.

Semantics:

- Merges write fragments into a file.
- The merging order corresponds to the order of transmitted write results.
- The merging order does not depend on the order of issued cookies during [start_distributed_write_file_session] (#start_distributed_write_file_session).

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| --------------------------     | -------------    | ------------------------- | ------------------------------------------------------------                |
| `session` | Yes |                           | Object of the signed distributed session. |
| `results` | Yes |                           | List of write fragments (see [write_file_fragment](#write_file_fragment)). |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

### write_file_fragment { #write_file_fragment }

Part of the distributed API.

Command properties: **Mutating**, **Heavy**.

Semantics:

- Write a data fragment to a file.
- The same cookie object can be reused to write a new fragment.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------------- | -------------    | ------------------------- | ------------------------------------------------------------             |
| `cookie` | Yes |                           | Signed cookie of a distributed session's participant. |

Input data:

- Type: `binary`.
- Value: Contents of the file write fragment.

Output data:

- Type: `structured`.
- Value: Signed result of writing the fragment to a file.

Example:

```bash
PARAMETERS {"cookie" = "sample_signed_cookie_1"}

INPUT this is sample file fragment content

OUTPUT "sample_signed_result"
```

## Working with file cache { #file_cache }

To learn more about the file cache, see the [File cache](../../user-guide/storage/file-cache.md) section.

### put_file_to_cache

Command properties: **Mutating**, **Light**.

Semantics:

- Add the file to the cache.
- Checks that, at the provided path, there's a file with the MD5 hash given in the command options.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------ |
| `path` | Yes |                           | Path to the file in Cypress. |
| `md5` | Yes |                           | Expected MD5 hash for the file. |
| `cache_path` | Yes |                           | Path to the file cache. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Path to the cached file.

Example:

```bash
PARAMETERS {
    "path" = "//tmp/file";
    "md5" = "a3dcb4d229de6fde0db5686dee47145d";
    "cache_path" = "//tmp/yt_wrapper/file_storage/new_cache";
}
OUTPUT "//tmp/yt_wrapper/file_storage/new_cache/5d/a3dcb4d229de6fde0db5686dee47145d"
```

### get_file_from_cache { #get_file_from_cache }

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get a path to the cached file.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ---------------------- |
| `md5` | Yes |                           | MD5 hash for the file. |
| `cache_path` | Yes |                           | Path to the file cache. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Path to the cached file.

Example:

```bash
PARAMETERS {
    "md5" = "a3dcb4d229de6fde0db5686dee47145d";
    "cache_path" = "//tmp/yt_wrapper/file_storage/new_cache";
}
OUTPUT "//tmp/yt_wrapper/file_storage/new_cache/5d/a3dcb4d229de6fde0db5686dee47145d"
```

## Working with tables { #tables }

To learn more about static tables, see the [Static tables](../../user-guide/storage/static-tables.md) section.

To learn more about dynamic tables, see the [Dynamic tables](../../user-guide/dynamic-tables/overview.md) section.

### write_table { #write_table }

Command properties: **Mutating**, **Heavy**.

Scope: [Static tables](../../user-guide/storage/static-tables.md).

Synonyms (supported, but not recommended).

- write.

Semantics:

- Add new entries to a static table.
- The table must exist.
- If the table `path` includes the `append=%true` attribute, the entries are appended to the table; otherwise, the table is overwritten.
- If the table `path` includes the `sorted_by` attribute, the system checks that the data is sorted by the specified set of keys and the resulting table is labeled as sorted. You can write sorted data (when the `sorted_by` attribute is specified) only if any new key is greater than or equal to any old key.
- You can write unsorted data (when the `sorted_by` attribute is not specified) to any table, but the table's sort flag will be removed.
- The command can be nested in a [transaction](#transactions).

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `path` | Yes |                           | Path to a table in Cypress. The path *must* exist. |
| `table_writer` | No | From the driver configuration | [Table write options](../../user-guide/storage/io-configuration.md). |

Input data:

- Type: `tabular`.
- Value: Table content.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS {
    "path" = "//tmp/node" ;
    "table_writer" = { "codec_id" = "gzip" };
}
INPUT { "id" = 1; "value" = 1.125; };
INPUT { "id" = 2; "value" = 2.000; };
INPUT { "id" = 3; "value" = 3.850; };
```

### read_table { #read_table }

Command properties: **Non-mutating**, **Heavy**.

Scope: [Static](../../user-guide/storage/static-tables.md) and [dynamic](../../user-guide/dynamic-tables/overview.md) tables.

Synonyms (supported, but not recommended).

- read.

Semantics:

- Retrieve entries from a Cypress table.
- The command can be nested in a [transaction](#transactions).

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `path` | Yes |                           | Path to a table in Cypress. The path *must* exist. |
| `table_reader` | No | From the driver configuration | [Table's read options](../../user-guide/storage/io-configuration.md#table_reader). |
| `control_attributes` | No | From the driver configuration | [Configuration of the control read attributes](../../user-guide/storage/io-configuration.md#control_attributes). |
| `unordered` | No | `false` | Whether to read data in parallel, regardless of the entry order. |

Input data:

- Type: `null`.

Output data:

- Type: `tabular`.
- Value: Table content.

Example:

```bash
PARAMETERS { "path" = "//tmp/node" }
OUTPUT { "id" = 1; "value" = 1.125; };
OUTPUT { "id" = 2; "value" = 2.000; };
OUTPUT { "id" = 3; "value" = 3.850; };
```

### read_blob_table

Command properties: **Non-mutating**, **Heavy**.

Scope: Static [tables with binary data](../../user-guide/storage/blobtables.md).

Semantics:

- Get a binary data stream from a specific column in a Cypress table.
- The command can be nested in a transaction.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ---------------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `path` | Yes |                           | Path to a table in Cypress. The path *must* exist. |
| `table_reader` | No | From the driver config file | [Table's read options](../../user-guide/storage/io-configuration.md). |
| `part_index_column_name` | No | `part_index` | Name of the column that stores BLOB indexes. |
| `data_column_name` | No | `data` | Name of the column that stores data. |

Input data:

- Type: `null`.

Output data:

- Type: `binary`.
- Value: Contents of a data column in the table.

Example:

```bash
PARAMETERS { "path" = "//tmp/node" }
OUTPUT "Hello world"
```

### partition_tables

Command properties: **Non-mutating**, **Heavy**.

Semantics:

- Split a set of tables into ranges of a specified size (similar to how tables are split in operations for distribution across jobs).
- The command can be executed within a [transaction](#transactions).

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ---------------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `paths` | Yes | | List of paths to tables in Cypress. |
| `data_weight_per_partition` | Yes | | Desired size of one partition. |
| `partition_mode` | No | `unordered` | Partitioning mode (`ordered`, `unordered`), corresponds to the mode of the map operation. |
| `max_partition_count` | No | | Maximum number of partitions; this option takes precedence over `data_weight_per_partition`. |
| `enable_cookies` | No | | The response will include a `cookie` field that can be used with the [read_table_partition](#read_table_partition) call. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`;
- Value: a list of table partitions.

Example:

```
PARAMETERS { "path" = "//tmp/node" ; "data_weight_per_partition"=200000000 ; }
OUTPUT {
    "partitions" = [
        {
            "table_ranges" = [
                <
                    "ranges" = [
                        {
                            "lower_limit" = {
                                "row_index" = 0;
                            };
                            "upper_limit" = {
                                "row_index" = 24569;
                            };
                        };
                    ];
                > "//home/dev/ermolovd/YT-11914";
            ];
            "aggregate_statistics" = {
                "chunk_count" = 24569;
                "data_weight" = 605020;
                "row_count" = 24569;
                "value_count" = 24569;
                "compressed_data_size" = 491277;
            };
        };
        {
            "table_ranges" = [
                <
                    "ranges" = [
                        {
                            "lower_limit" = {
                                "row_index" = 24569;
                            };
                            "upper_limit" = {
                                "row_index" = 24570;
                            };
                        };
                    ];
                > "//home/dev/ermolovd/YT-11914";
            ];
            "aggregate_statistics" = {
                "chunk_count" = 1;
                "data_weight" = 25;
                "row_count" = 1;
                "value_count" = 1;
                "compressed_data_size" = 20;
            };
        };
    ];
}
```

### read_table_partition

Command properties: **Non-mutating**, **Heavy**.

Semantics:

- Retrieve records within a table range.
- The range must be obtained in advance using the `[partition_tables](#partition_tables)` command with the `enable_cookies=%true` option.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ---------------- | ------------------------- | ------------ |
| `cookie` | Yes | | Cookie corresponding to the table range, obtained from the [partition_tables](#partition_tables) call with the `enable_cookies=%true` option. |

Input data:

- Type: `null`.

Output data:

- Type: `tabular`;
- Value: contents of the table range.

### select_rows

Command properties: **Non-mutating**, **Heavy**.

Scope: [Dynamic tables](../../user-guide/dynamic-tables/overview.md).

Semantics:

- Execute an SQL-like query for a dynamic table in accordance with the [supported features](../../user-guide/dynamic-tables/dyn-query-language.md).
- The transaction can be executed against a data snapshot with a specified timestamp.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `query` | Yes |                           | Query string. |
| `timestamp` | No | `sync last committed` | Which timestamp the query should run against. |

Input data:

- Type: `null`.

Output data:

- Type: `tabular`.
- Value: Set of rows with the result.

Example:

```bash
PARAMETERS { "query" = "key, value from [//tmp/sometable]" }
OUTPUT { "key" = 1; "value" = "hello"; };
OUTPUT { "key" = 2; "value" = "world; };
```

### insert_rows

Command properties: **Mutating**, **Heavy**.

Scope: [Dynamic tables](../../user-guide/dynamic-tables/overview.md).

Semantics:

- Write rows to a dynamic table.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `path` | Yes |                           | Path to the dynamic table. |
| `update` | No | `false` | If set to `false`, the columns missing in the input data will be written with the value of `Null` (overwriting the current value in the table). If set to `true`, such columns will preserve their previous value in the table. |
| `aggregate` | No | `false` | If set to `false`, the [aggregating columns](../../user-guide/dynamic-tables/sorted-dynamic-tables.md#aggr_columns) will be overwritten by the new value. If set to `true`, such columns will apply the delta from the source data. |
| `atomicity` | No | `full` | Supported values: `none` and `full`. If set to `none`, writing will occur on each tablet, independent of the others. If set to `full`, either all of the passed rows will be written, or nothing. [Read more](../../user-guide/dynamic-tables/sorted-dynamic-tables.md). |
| `require-sync-replica` | No | `true` | The option makes sense only in case of replication. If set to `true`, inserting will occur only if the table has a synchronous replica. If set to `false`, a synchronous replica is not required. |

Input data:

- Type: `tabular`.
- Value: Table content.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "path" = "//home/user/table" }
INPUT { "id" = 1; "value" = 1.125; };
INPUT { "id" = 2; "value" = 2.000; };
INPUT { "id" = 3; "value" = 3.850; };
```

### delete_rows

Command properties: **Mutating**, **Heavy**.

Scope: [Dynamic tables](../../user-guide/dynamic-tables/overview.md).

Semantics:

- Delete all rows with the specified keys from the dynamic table.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------------- |
| `path` | Yes |                           | Path to the dynamic table. |

Input data:

- Type: `tabular`.
- Value: Set of rows with the keys.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "path" = "//tmp/sometable" }
INPUT { "id" = 1; };
INPUT { "id" = 2; };
INPUT { "id" = 3; };
```

### lock_rows
Command properties: **Mutating**, **Heavy**.

Scope: [Dynamic tables](../../user-guide/dynamic-tables/overview.md).

Semantics:

- Lock writing to the rows in the dynamic table while the current transaction is running.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------------- |
| `path` | Yes |                           | Path to the dynamic table. |
| `locks` | Yes |                           | List of the involved `lock groups` from the table schema. |
| `lock_type` | No | `shared strong` | Lock types: `shared weak`, `shared strong`, `exclusive`. |

A shared lock can be taken from multiple different transactions at the same time. An exclusive lock can be taken from a single transaction that usually updates this row.

When using read locks (in contrast to write locks), one row can be locked by multiple transactions. That's why, when there is a continuous stream of transactions that take a shared lock on a certain row, the row becomes locked permanently, and you won't be able to update it. This effect is referred to as `write starvation`. However this is never the case for write-write conflicts because they have an `exclusive lock` that is released every time a write transaction is complete.

To alleviate the effect of `write starvation`, you can decrease isolation of shared locks. For this, you can specify the `weak` or `strong` lock mode. The distinction of the `weak` mode is that when the transaction is complete, the timestamps until which the rows were locked by shared locks, aren't saved. The practical implication is that a write operation isn't locked if nested in a transaction that overlaps with the current transaction in time but completes later. However, if the write transaction completes earlier, the transaction with a shared lock isn't applied. In the `weak` mode, the shared lock becomes asymmetric.

Input data:

- Type: `tabular`.
- Value: Set of rows with the keys.


### lookup_rows

Command properties: **Non-mutating**, **Heavy**.

Scope: [Dynamic tables](../../user-guide/dynamic-tables/overview.md).

Semantics:

- Select the rows with the specified keys from the table.
- The command can be executed against a data snapshot with a specified timestamp.
- It is guaranteed that the relative order of rows retrieved in the response will be the same as the order of keys in the query.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ----------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `path` | Yes |                           | Path to the dynamic table. The path should be simple (without columns, ranges, etc.) |
| `column_names` | No |                           | Which columns to include in the response. |
| `keep_missing_rows` | No | `false` | Whether to include the rows corresponding to the non-found keys. If set to `true`, the corresponding positions will include `#` (entity). |
| `timestamp` | No | `sync last committed` | Which timestamp the query should run against. |

Input data:

- Type: `tabular`.
- Value: Set of rows with the keys.

Output data:

- Type: `tabular`.
- Value: A set of rows with the specified keys and queried columns.

Example:

```bash
PARAMETERS { "path" = "//tmp/sometable" }
INPUT { "id" = 1; };
INPUT { "id" = 2; };
INPUT { "id" = 3; };
OUTPUT { "id" = 1; "value" = 1.125; };
OUTPUT { "id" = 2; "value" = 2.000; };
OUTPUT { "id" = 3; "value" = 3.850; };
```

### trim_rows

Command properties: **Mutating**, **Heavy**.

Scope: [Ordered](../../user-guide/dynamic-tables/ordered-dynamic-tables.md) dynamic tables.

Semantics:

- Remove rows from the beginning of the tablet of an ordered dynamic table . After that, the deleted data can no longer be read by the `select_rows` command. Numbering of other rows is unchanged in this case.
- The command is executed outside of transactions.

Parameters:

| **parameter** | **Required** | **Default value** | **Description** |
| ------------------- | ------------- | ------------------------- | ----------------------------- |
| `path` | Yes |                           | Path to the dynamic table. |
| `tablet_index` | Yes |                           | Index of the truncated tablet. |
| `trimmed_row_count` | Yes |                           | Number of deleted rows. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "path" = "//home/user/table"; tablet_index = 10; trimmed_row_count = 43 }
```

### mount_table

Command properties: **Mutating**, **Light**.

Scope: [Dynamic tables](../../user-guide/dynamic-tables/overview.md).

Semantics:

- Mounts the tablets of the dynamic table.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `path` | Yes |                           | Path to the dynamic table. |
| `first_tablet_index` | No | `0` | Index of the first mounted tablet. |
| `last_tablet_index` | No | `tablet_count - 1` | Index of the last mounted tablet. |
| `cell_id` | No |                           | If specified, the tablets are mounted to the specified cell. Otherwise, the system selects suitable cells (in most cases, you should delegate this choice to the system). |
| `freeze` | No | `false` | If set to `true`, the tablets are mounted to a frozen state. See also the description of the [freeze_table](#freeze_table) command. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "path" = "//home/user/table" }
```

### unmount_table

Command properties: **Mutating**, **Light**.

Scope: [Dynamic tables](../../user-guide/dynamic-tables/overview.md).

Semantics:

- Unmounts the tablets of the dynamic table.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------- | ---------- | ------------------------- | ---------------------------------------- |
| `path` | Yes |                           | Path to the dynamic table. |
| `first_tablet_index` | No | `0` | Index of the first unmounted tablet. |
| `last_tablet_index` | No | `tablet_count - 1` | Index of the last unmounted tablet. |
| `force` | No | `false` | Forcibly unmount the tablets. Using this flag poses a risk of data loss, that's why you need {{product-name}} admin rights to use it. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "path" = "//home/user/table"; "first_tablet_index" = 10; "last_tablet_index" = 20; }
```

### remount_table

Command properties: **Mutating**, **Light**.

Scope: [Dynamic tables](../../user-guide/dynamic-tables/overview.md).

Semantics:

- Enables you to update certain settings of dynamic tables without unmounting them.
- When remounted like that, the table remains accessible for reading and writing.
- The settings are taken from the table attributes and eventually reach the node where the tablets are processed.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------- | ------------- | ------------------------- | ------------------------------------------ |
| `path` | Yes |                           | Path to the dynamic table. |
| `first_tablet_index` | No | `0` | Index of the first remounted tablet. |
| `last_tablet_index` | No | `tablet_count - 1` | Index of the last remounted tablet. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "path" = "//home/user/table" }
```

### freeze_table

Command properties: **Mutating**, **Light**.

Scope: [Dynamic tables](../../user-guide/dynamic-tables/overview.md).

Semantics:

- Changes the tablets of the dynamic table to a frozen state, flushing all the data to the disk. You can read data in this state, but you can't write new entries. However, all the other written data exist in chunks and are available to map-reduce operations, for example (even without using the `enable_dynamic_store_read` option).

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------- | ------------- | ------------------------- | -------------------------------------- |
| `path` | Yes |                           | Path to the dynamic table. |
| `first_tablet_index` | No | `0` | Index of the first frozen tablet. |
| `last_tablet_index` | No | `tablet_count - 1` | Index of the last frozen tablet. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "path" = "//home/user/table" }
```

### unfreeze_table

Command properties: **Mutating**, **Light**.

Scope: [Dynamic tables](../../user-guide/dynamic-tables/overview.md).

Semantics:

- Unfreezes a table that was frozed (for example, by a `freeze_table` command), making the table write-accessible.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------- | ------------- | ------------------------- | -------------------------------------- |
| `path` | Yes |                           | Path to the dynamic table. |
| `first_tablet_index` | No | `0` | Index of the first unfrozen tablet. |
| `last_tablet_index` | No | `tablet_count - 1` | Index of the last unfrozen tablet. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "path" = "//home/user/table" }
```

### reshard_table

Command properties: **Mutating**, **Light**.

Scope: [Dynamic tables](../../user-guide/dynamic-tables/overview.md).

Semantics:

- Reshards the dynamic table (that is, changes the set of its tablets).
- The resharded tablets must be unmounted.
- Be sure to specify `tablet_count` for an ordered table. For a sorted table, you can specify both `tablet_count` and `pivot_keys`. The resharded tablets are replaced by a set of new tablets.
- In the case of a sorted table:
  - When passing `pivot_keys`, the first key in `pivot_keys` must match the first key of the first resharded tablet. The number of `pivot_keys` is equal to the number of new tablets that the resharded tablets are split into.
  - When passing `tablet_count`, the system will select boundary keys based on the data available in the table as evenly as possible. If the table isn't large enough, you might get less tablets then requested as a result. At default settings, your resulting tablets can't be smaller than about 200 MB each. For smaller slicing, use the option`enable_slicing`.
  - If the first key column of the table has an integer type, then along with `tablet_count`, you can use `uniform=True`. In this case, uniform values from the range of the appropriate type will be selected as boundary keys. `0, 2^64/n, 2^64\*2/n, ...` for an unsigned 64-bit type and `-2^63, -2^63 + 2^64/n, -2^63 + 2^64\*2/n, ...` for a signed 64-bit type.
- For an ordered table, `table_count` specifies the number of new tablets that the sharded tablets are split into. In this case, if the resulting tablets are higher in numbers than the old ones, new empty tablets are created. If the resulting tablets are smaller in numbers, the corresponding number of source trailing tablets are merged into a single tablet in their natural order.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `path` | Yes |                           | Table path. |
| `first_tablet_index` | No | `0` | Index of the first resharded tablet. |
| `last_tablet_index` | No | `tablet_count - 1` | Index of the last resharded tablet. |
| `pivot_keys` | No |                           | Boundary keys for the new tablets (for a sorted table). |
| `tablet_count` | No |                           | Number of new tablets. |
| `uniform` | No | `false` | Uniformly reshard tablets based on an integer key column. |
| `enable_slicing` | No | `false` | Use sampling to increase granularity (for a more precise splitting into tablets) when boundary keys are selected automatically. This might help if you have many entries on one key and few entries on another key. |
| `slicing_accuracy` | No | `0.05` | Tolerance acceptable for uniform regarding into a given number of tablets. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "path" = "//home/user/table"; "schema" = [{"name" = "key"; type = "int64"; "sort_order" = "ascending"}; {"name" = "value"; type = "string"}]; }
```

### reshard_table_automatic

Command properties: **Mutating**, **Light**.

Scope: [Sorted](../../user-guide/dynamic-tables/sorted-dynamic-tables.md) dynamic tables.

Semantics:

- Forces an iteration of a [background tablet balancer](../../user-guide/dynamic-tables/resharding.md), that is, reshards the tablets that go beyond the `min_tablet_size`–`max_tablet_size` interval.
- In contrast to `reshard_table`, it can work with mounted tables. In the process of its operation, it can amount tablets.
- It's started asynchronously by default, but if you specify `keep_actions`, it returns a list of IDs that you can use to track the progress. Most APIs do it automatically when passed the `sync=True` flag.
- The command does not support replicated tables (you can use it independently on different replicas, however).

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `path` | Yes |                           | Table path. |
| `keep_actions` | No | `false` | If set to `true`, respond with a list of IDs to track the progress. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: A list of `tablet_action_id` if `keep_actions=True`. Otherwise, it returns an empty list.

Example:

```bash
PARAMETERS { "path" = "//home/user/table"; }
OUTPUT [
    "11-22-33-44";
]
```

 ### alter_table

Command properties: **Mutating**, **Light**.

Scope: [Static](../../user-guide/storage/static-tables.md) and [dynamic tables](../../user-guide/dynamic-tables/overview.md).

Semantics:

- Changes the scheme and other table settings (both static and dynamic).
- You can usually use `alter_table` to change the settings that require complex joint validation. The attributes are used to change the recommended settings that only affect new data.
- Changing the schema involves [various validation checks](../../user-guide/storage/static-schema.md) because the system has to make sure that the existing data matches the schema.
- You can change a static table to a dynamic table, but not vice versa.
- You can change the schema of a dynamic table, as well as `upstream_replica_id`, only if the table is unmounted.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| --------------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `path` | Yes |                           | Table path. |
| `schema` | No |                           | If specified, it sets a new schema for the table. |
| `dynamic` | No |                           | If specified, it changes a static table to a dynamic table. This setting can only be changed outside a transaction. |
| `upstream_replica_id` | No |                           | If specified, it changes the ID of the replica object on the metacluster. For more information, see [Replicated dynamic tables](../../user-guide/dynamic-tables/replicated-dynamic-tables.md). |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "path" = "//home/user/table"; "schema" = [{"name" = "key"; type = "int64"; "sort_order" = "ascending"}; {"name" = "value"; type = "string"}]; }
```

### alter_table_replica

Command properties: **Mutating**, **Light**.

Scope: [Replicated](../../user-guide/dynamic-tables/replicated-dynamic-tables.md) dynamic tables.

Semantics:

- Changes the replica's properties: enables/disables it or changes its mode.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | -------------------------------- | ----------------------------------------------------- |
| `replica_id` | Yes |                                  | Replica ID. |
| `enabled` | No | Doesn't change the enabled/disabled status of the replica | If set to `true`, enables the table. If set to `false`, disables the table. |
| `mode` | No | Doesn't change the replica's mode | Changes the replica mode `sync`/`async`. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "replica_id" = "730e-8611b-3ff02c5-f647333f"; "enabled" = %true; }
```

### get_table_columnar_statistics

Command properties: **Non-mutating**, **Light**.

Scope: [Static](../../user-guide/storage/static-tables.md) and [dynamic](../../user-guide/dynamic-tables/overview.md) tables.

Semantics:

- Get statistics on the set of columns in the given set of tables (taken completely or partially, by ranges).
- The statistics includes:
  - The total `data_weight` for each of the requested columns.
  - The total `data_weight` for all the old chunks (that the metainformation about each column hasn't been saved to because the chunk has been generated before column-by-column statistics were supported).
  - The total weight of all the timestamps of rows in a dynamic table.
- The paths should always include the `column selectors`.
- The command can be nested in a transaction.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `paths` | Yes |                           | A list of paths to tables in Cypress. The tables *must* exist. The paths *must* include `column selectors`. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Values: Column-by-column statistics for the requested columns.

Example:

```bash
PARAMETERS {
    "paths" = ["//tmp/table1{a,b,c}"; "//tmp/table2{a,b,c}"];
    "transaction_id" = "1234-abcd-abcd-7890"
}
OUTPUT {
    "column_data_weights" = {
        "a" = 8124;
        "b" = 124241241;
        "c" = 3121414;
    };
    "legacy_chunks_data_weight" = 100242;
    "timestamp_total_weight" = 50056;
}
```

### start_distributed_write_session { #start_distributed_write_session }

YT provides a mechanism for distributed file and table writes.

In a scenario when multiple participants write to a single table or file, this mechanism is less taxing on the master server resources compared to using the [write_table](#write_table)/[write_file](#write_file) method (with append=true).

The workflow for using distributed table write methods is as follows (same applies to distributed file writes, see [start_distributed_write_file_session](#start_distributed_write_file_session)):

1. First, one participant (the distributed session host) initiates the distributed write mechanism using the [start_distributed_write_session](#start_distributed_write_session) method. The method returns a session and a cookie array for write participants.

2. The host distributes cookies among participants. Each session participant writes data via [write_fragment](#write_fragment) using their respective cookie. This method returns a write result that the participant passes to the distributed session host.

3. Finally, the host calls the [finish_distributed_write_session](#finish_distributed_write_session) method, passing the array of write results to it. All write fragments will be combined into the final table.

4. Between [start_distributed_write_session](#start_distributed_write_session) and [finish_distributed_write_session](#finish_distributed_write_session), the session must be pinged with the [ping_distributed_write_session](#ping_distributed_write_session) method to prevent timeout termination.

Command properties: **Mutating**, **Light**.

Semantics:

- Initialize a session of distributed writing to a table.
- Request cookies for distributed session participants.
- Not all requested cookies have to be used.
- The same cookie object can be reused to write a new fragment.
- If the table `path` includes the `append=%true` attribute, the entries are appended to the table; otherwise, the table is overwritten.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------------- | -------------    | ------------------------- | ------------------------------------------------------------             |
| `path` | Yes |                           | Path to a table in Cypress. |
| `cookie_count` | Yes |                           | Number of distributed session participants. Positive number. |
| `transaction_id` | No | *null-transaction-id* | Current transaction ID. |
| `ping_ancestor_transactions` | No | `false` | Whether to ping all the parent transactions while running the operation. |
| `timeout` | No |                           | Session TTL since the last ping (in ms). |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: A signed distributed session object along with a list of signed cookies of participants.

Example:

```bash
PARAMETERS {
    "path" = "//tmp/table";
    "cookie_count" = "2";
}

OUTPUT {
    "session" = "sample_signed_session";
    "cookies" = ["sample_signed_cookie_1"; "sample_signed_cookie_2"];
}
```

### ping_distributed_write_session { #ping_distributed_write_session }

Part of the distributed tabular API.

Command properties: **Mutating**, **Light**.

Semantics:

- Pings the distributed session transaction on the server (including all parent transactions if `ping_ancestor_transactions` is specified), thereby extending the transaction's TTL.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| --------------------------     | -------------    | ------------------------- | ------------------------------------------------------------ |
| `session` | Yes |                           | Object of the signed distributed session. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

### finish_distributed_write_session { #finish_distributed_write_session }

Part of the distributed tabular API.

Command properties: **Mutating**, **Light**.

Semantics:

- Merges write fragments into a table.
- For sorted tables, passed write results are sorted by boundary keys.
Key intervals must not overlap, and the keys themselves can't be duplicate. With all invariants met, the order of passed results does not matter.
- For unsorted tables, fragments are combined in the order in which the results were passed.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| --------------------------     | -------------    | ------------------------- | ------------------------------------------------------------                |
| `session` | Yes |                           | Object of the signed distributed session. |
| `results` | Yes |                           | List of write fragments (see [write_fragment](#write_fragment)). |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

### write_fragment { #write_fragment }

Part of the distributed tabular API.

Command properties: **Mutating**, **Heavy**.

Semantics:

- Write a data fragment to a table.
- The same cookie object can be reused to write a new fragment.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------------- | -------------    | ------------------------- | ------------------------------------------------------------             |
| `cookie` | Yes |                           | Signed cookie of a distributed session's participant. |
| `max_row_buffer_size` | No | 1_MB | Size of the string buffer for the internal writer |

Input data:

- Type: `tabular`.
- Value: Table fragment content.

Output data:

- Type: `structured`.
- Value: Signed result of writing the table fragment.

Example:

```bash
PARAMETERS {"cookie" = "sample_signed_cookie_1"}

INPUT { "id" = 1; "value" = 1.125; };
INPUT { "id" = 2; "value" = 2.000; };

OUTPUT "sample_signed_result"
```

### alter_table

Command properties: **Mutating**, **Light**.

Semantics:

- Modify table metadata and settings.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `path` | Yes |                           | Path to the table. |
| `schema` | No |                           | New table schema. |
| `dynamic` | No |                           | Whether the table should be dynamic. |
| `upstream_replica_id` | No |                           | Upstream replica ID for table replication. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

### enable_table_replica

Command properties: **Mutating**, **Light**.

Semantics:

- Enable a table replica.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `replica_id` | Yes |                           | Replica ID to enable. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

### disable_table_replica

Command properties: **Mutating**, **Light**.

Semantics:

- Disable a table replica.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `replica_id` | Yes |                           | Replica ID to disable. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

### get_in_sync_replicas

Command properties: **Non-mutating**, **Heavy**.

Semantics:

- Get the list of replicas that are in sync with a table at a given timestamp.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `path` | Yes |                           | Path to the table. |
| `timestamp` | Yes |                           | Timestamp to check synchronization at. |
| `all_keys` | No | `false` | Whether to check all keys. |

Input data:

- Type: `tabular`.

Output data:

- Type: `structured`.
- Value: List of in-sync replica IDs.

### pull_rows

Command properties: **Non-mutating**, **Heavy**.

Semantics:

- Pull rows from a table for replication purposes.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `path` | Yes |                           | Path to the table. |
| `upstream_replica_id` | Yes |                           | Upstream replica ID. |
| `replication_progress` | Yes |                           | Current replication progress. |
| `upper_timestamp` | No |                           | Upper timestamp limit for pulling. |
| `order_rows_by_timestamp` | No | `false` | Whether to order rows by timestamp. |

Input data:

- Type: `null`.

Output data:

- Type: `tabular`.
- Value: Rows pulled from the table.

### explain_query

Command properties: **Non-mutating**, **Heavy**.

Semantics:

- Get the execution plan for a query without actually running it.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `query` | Yes |                           | Query string to explain. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Query execution plan.

### get_table_pivot_keys

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get the pivot keys that define tablet boundaries for a table.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `path` | Yes |                           | Path to the table. |
| `represent_key_as_list` | No | `false` | Whether to represent keys as lists instead of tuples. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: List of pivot keys.

### get_tablet_infos

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get information about specific tablets of a table.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `path` | Yes |                           | Path to the table. |
| `tablet_indexes` | Yes |                           | List of tablet indexes to get information for. |
| `request_errors` | No | `false` | Whether to include error information. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Information about the requested tablets.

### get_tablet_errors

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get error information for tablets of a table.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `path` | Yes |                           | Path to the table. |
| `limit` | No |                           | Maximum number of errors to return. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: List of tablet errors.

### get_table_mount_info

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get mount information for a dynamic table.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `path` | Yes |                           | Path to the table. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Table mount information including cell IDs and tablet states.

### balance_tablet_cells

Command properties: **Mutating**, **Light**.

Semantics:

- Balance tablets across tablet cells for optimal resource distribution.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `bundle` | Yes |                           | Tablet cell bundle name. |
| `tables` | No |                           | Specific tables to balance (if omitted, balance all tables in bundle). |
| `keep_actions` | No | `false` | Whether to keep the actions for inspection. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Balancing actions taken or planned.

### cancel_tablet_transition

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Cancel an ongoing tablet state transition (e.g., mounting, unmounting).

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `tablet_id` | Yes |                           | ID of the tablet. |
| `cell_id` | Yes |                           | ID of the tablet cell. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

### create_table_backup

Command properties: **Mutating**, **Light**.

Semantics:

- Create a backup of one or more tables with consistent snapshot semantics.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `manifest` | Yes |                           | Backup manifest describing tables and their destinations. |
| `checkpoint_timestamp_delay` | No | `5s` | Delay for checkpoint timestamp selection. |
| `force` | No | `false` | Force backup even if destination tables exist. |
| `preserve_account` | No | `false` | Preserve the source account in the backup. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

### restore_table_backup

Command properties: **Mutating**, **Light**.

Semantics:

- Restore tables from a previously created backup.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `manifest` | Yes |                           | Restore manifest describing source backups and target locations. |
| `force` | No | `false` | Force restore even if target tables exist. |
| `mount` | No | `false` | Whether to mount tables after restoration. |
| `enable_replicas` | No | `false` | Whether to enable table replicas after restoration. |
| `preserve_account` | No | `false` | Preserve the backup account in restored tables. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

### locate_skynet_share

Command properties: **Non-mutating**, **Heavy**.

Semantics:

- Locate data for Skynet share integration.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `path` | Yes |                           | Path to locate. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.



## Running operations

For more information about running data processing operations, see [Data processing](../../user-guide/data-processing/operations/overview.md).

All the operations are run asynchronously, the specified commands only launch them. To find out whether the operation is complete or not, request the [operation status](../../user-guide/data-processing/operations/overview.md#status) using the `get_operation` command.
All the commands used to work with operations are also transactional. It means that everything you do with tables in an operation will be executed within the specified transaction when you run the operation.  The node responsible for the operation (`//sys/operations/<OP_ID>`) is updated by the [scheduler](../../user-guide/data-processing/scheduler/scheduler-and-pools.md)outside of any transactions.

### start_operation

{% note info %}

The HTTP API supports this operation from version v4 onward. Earlier versions of the API use commands with the same names to run operations, such as `map`, `reduce`, `sort`, and so on.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Run the operation of the specified type.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `operation_type` | Yes |                           | Operation type (can be one of these: `map`, `reduce`, `map_reduce`, `remote-copy`, `erase`, `sort`, `merge`, `vanilla`). |
| `spec` | Yes |                           | Operation specification. For more information, see [Setting up operations](../../user-guide/data-processing/operations/operations-options.md). |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: ID of the running operation.

Example:

```bash
PARAMETERS {  "operation_type" = "map_reduce";
    "spec" = {
        "input_table_paths" = [ "//tmp/table_in", "//tmp/table_in" ] ;
        "output_table_path" = "//tmp/table_out"
    }
}
OUTPUT "37878b-ba919c15-cdc97f3a-8a983ece"
```

### merge

Command properties: **Mutating**, **Light**.

Semantics:

- Start merging the source tables.
- Detailed description of all [specification parameters](../../user-guide/data-processing/operations/merge.md).

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ----------------------- | -------------- | ------------------------- | ------------------------------------------- |
| `spec` | Yes |                           | Operation specification. |
| *spec[input_table_paths]* | Yes |                           | List of input tables. |
| *spec[output_table_path]* | Yes |                           | Output table. |
| *spec[mode]* | No | `unordered` | Merging mode ( `unordered`, `ordered`, `sorted`). |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: ID of the running operation.

Example:

```bash
PARAMETERS {
    "spec" = {
        "input_table_paths" = [ "//tmp/table_in", "//tmp/table_in" ] ;
        "output_table_path" = "//tmp/table_out"
    }
}
OUTPUT "37878b-ba919c15-cdc97f3a-8a983ece"
```

### erase

Command properties: **Mutating**, **Light**.

Semantics:

- Start erasing data from the source table.
- Detailed description of all [specification parameters](../../user-guide/data-processing/operations/erase.md).

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ---------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `spec` | Yes |                           | Operation specification. |
| *spec[table_path]* | Yes |                           | An input table with the specified row selector. The same table will be used for output. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: ID of the running operation.

Example:

```bash
PARAMETERS {
    "spec" = {
        "table_path" = "//tmp/table[#0:#500]" ;
    }
}
OUTPUT "3f9e62-ce8d2965-6350842b-3e4628d2"
```

### map

Command properties: **Mutating**, **Light**.

Semantics:

- Run the map operation on the source tables, writing the output to the output tables.
- Detailed description of all [specification parameters](../../user-guide/data-processing/operations/map.md).

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------------------ | ------------- | ------------------------- | -------------------------------------------------- |
| `spec` | Yes |                           | Operation specification (see the relevant fields below). |
| *spec[input_table_paths]* | Yes |                           | List of input tables. |
| *spec[output_table_paths]* | Yes |                           | List of output tables. |
| *spec[mapper]\[command]* | Yes |                           | The command that runs the mapper. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: ID of the running operation.

Example:

```bash
PARAMETERS {
    "spec" = {
      "mapper" = {
          "command" = "cat"
      } ;
      "input_table_paths"  = [ "//tmp/table_in", "//tmp/table_in" ] ;
      "output_table_paths" = [ "//tmp/table_out" ]
    }
}
OUTPUT "33ab3f-bf1df917-b35fe9ed-c70a4bf4"
```

### reduce

Command properties: **Mutating**, **Light**.

Semantics:

- Run the reduce operation on the source tables, writing the output to the output tables.
- Detailed description of all [specification parameters](../../user-guide/data-processing/operations/reduce.md).

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------------------ | ------------- | ------------------------- | -------------------------------------------------- |
| `spec` | Yes |                           | Operation specification (see the relevant fields below). |
| *spec[input_table_paths]* | Yes |                           | List of input tables. |
| *spec[output_table_paths]* | Yes |                           | List of output tables. |
| *spec[reduce_by]* | Yes |                           | Columns that `reduce` runs against. |
| *spec[reducer]\[command]* | Yes |                           | Command that runs the reducer. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: ID of the running operation.

Example:

```bash
PARAMETERS {
    "spec" = {
      "reducer" = {
          "command" = "cat" ;
      } ;
      "input_table_paths"  = [ "//tmp/table_in", "//tmp/table_in" ] ;
      "output_table_paths" = [ "//tmp/table_out" ] ;
      "reduce_by"          = [ "my_key" ] ;
    }
}
OUTPUT "33ab3f-bf1df917-b35fe9ed-c70a4bf4"
```

### sort

Command properties: **Mutating**, **Light**.

Semantics:

- Start sorting the source tables.
- Detailed description of all [specification parameters](../../user-guide/data-processing/operations/sort.md).

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ----------------------- | ------------- | ------------------------- | -------------------------------------------------------- |
| `spec` | Yes |                           | Operation specification (see the relevant fields below). |
| *spec[input_table_paths]* | Yes |                           | List of input tables. |
| *spec[output_table_path]* | Yes |                           | Output table. |
| *spec[sort_by]* | Yes |                           | And non-empty set of column names that make up the sort key. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: ID of the running operation.

Example:

```bash
PARAMETERS {
    "spec" = {
      "input_table_paths" = [ "//tmp/table_in", "//tmp/table_in" ] ;
      "output_table_path" = "//tmp/table_out" ;
      "sort_by"           = [ "mykey" ];
    }
}
OUTPUT "37878b-ba919c15-cdc97f3a-8a983ece"
```

### map_reduce

Command properties: **Mutating**, **Light**.

Semantics:

- Run Map-Reduce on the source tables.
- Detailed description of all [specification parameters](../../user-guide/data-processing/operations/mapreduce.md).

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `spec` | Yes |                           | Operation specification (see the relevant fields below). |
| *spec[input_table_paths]* | Yes |                           | List of input tables. |
| *spec[output_table_paths]* | Yes |                           | List of output tables. |
| *spec[mapper]\[command]* | No |                           | The command that runs the mapper. |
| *spec[sort_by]* | No |                           | A non-empty set of column names by which the data for reduces will be sorted. |
| *spec[reduce_by]* | Yes |                           | Columns that reduce runs against. |
| *spec[reducer]\[command]* | Yes |                           | Command that runs the reducer. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: ID of the running operation.

Example:

```bash
PARAMETERS {
    "spec" = {
      "mapper" = {
          "command" = "cat"
      } ;
      "reducer" = {
          "command" = "cat"
      } ;
      "input_table_paths" = [ "//tmp/table_in", "//tmp/table_in" ] ;
      "output_table_path" = "//tmp/table_out" ;
      "reduce_by" = [ "my_key" ] ;
    }
}
OUTPUT "37878b-ba919c15-cdc97f3a-8a983ece"
```

### remote-copy

Command properties: **Mutating**, **Light**.

Semantics:

- Copy the source tables from the specified cluster.
- Detailed description of all [specification parameters](../../user-guide/data-processing/operations/remote-copy.md).

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------------------ | ------------- | ------------------------- | -------------------------------------------------- |
| `spec` | Yes |                           | Operation specification (see the relevant fields below). |
| *spec[input_table_paths]* | Yes |                           | List of input tables. |
| *spec[output_table_paths]* | Yes |                           | List of output tables. |
| *spec[cluster_name]* | Yes |                           | Cluster with the source tables. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: ID of the running operation.

Example:

```bash
PARAMETERS {
    "spec" = {
      "input_table_paths" = [ "//tmp/table_in", "//tmp/table_in" ] ;
      "output_table_path" = "//tmp/table_out" ;
      "cluster_name" = {{production-cluster}};
    }
}
OUTPUT "37878b-ba919c15-cdc97f3a-8a983ece"
```

## Working with operations

{% note info "Note" %}

All the commands used to work with the operations are non-[transactional](#transactions).

{% endnote %}

The table describes the `TOperation` type, which contains information about the operation returned by the `get_operation` and `list_operations` commands:

| **Field** | **Type** | **Description** |
|--------------------------|-----------------|---------------------------------------------------------------------------------------------------|
| `id` | `string` | String representation of the operation ID (for example, `d840bb39-3d893e5b-3fe03e8-f009b1fb`). |
| `type` | `string` | Operation type (for example, `map`, `reduce`, `map_reduce`). |
| `state` | `string` | Current operation state (for example, `completed`, `running`,`failed`). |
| `authenticated_user` | `string` | User that ran the operation. |
| `start_time` | `ISO 8601 string` | Start time. |
| `finish_time` | `ISO 8601 string` | End time (if the operation completed). |
| `brief_progress` | `map` | Summarized statistics on jobs. |
| `progress` | `map` | Full statistics on jobs. |
| `brief_spec` | `map` | Part of the specification with light fields. |
| `provided_spec` | `map` | Specification given by the user when starting the operation. |
| `full_spec` | `map` | Specification where all the fields omitted by the user are populated by defaults. |
| `unrecognized_spec` | `map` | Specification fields that were entered by the user but not recognized by the scheduler. |
| `experiment_assignment_names` | `list<string>` | Names of the experiments applied to the operation. |
| `experiment_assignments` | `list<string>` | Descriptions of the experiments applied to the operation. |
| `spec` | `map` | Specification given by the user when starting the operation, with the experiments applied. |
| `controller_agent_address` | `string` | Address of the controller agent (host:port) responsible for the operation. |
| `events` | `list<map>` | List of state change events that occurred during the operation. |
| `alerts` | `map<string, map>` | Alerts set up for the operation at the moment. |
| `alert_events` | `list<map>` | List of alert start and end events that occurred during the operation. |
| `result` | `map` | A map with an `error` field that can include an error if the operation failed. |
| `committed` | `bool` | Whether the operation results were committed. |
| `suspended` | `bool` | Whether the operation is currently suspended. |
| `scheduling_attributes_per_pool_tree` | `map<string, map>` | Map with information about the operation's execution across different trees. |
| `task_names` | `list<string>` | Names of the tasks comprising the operation. |
| `controller_features` | `map` | Set of quantitative characteristics that can be used to analyze experiments applied to operations. |

### list_operations { #list_operations }

{% note warning "Attention" %}

This command can create a significant load against the cluster. Do not use it in your workflow without a prior approval by the administrator.

{% endnote %}

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get a list of operations that match the filters.

Parameters:

| **Parameter** | **Type** | **Required** | **Default value** | **Description** |
| ---------------- | -------------- | ----------------- | ------------------------- | ------------------------------------------------------------ |
| `attributes` | `list<string>` | No | `Null` | List of operation attributes that need to be returned in the response. |
| `from_time` | `ISO 8601 string` | No | `Null` | Bottom limit for the time interval for operation selection (by the time when the operation began). |
| `to_time` | `ISO 8601 string` | No | `Null` | Top limit for the time interval for operation selection (by the time when the operation began). |
| `cursor_time` | `ISO 8601 string` | No | `Null` | Time beginning from which a list of operation should be returned. |
| `cursor_direction` | `{past, future}` | No | `past` | Time direction used to list the operations. |
| `user` | `string` | No | `Null` | Username to filter for. |
| `state` | `string` | No | `Null` | Operation state used to filter data. |
| `type` | `string` | No | `Null` | Type of filtering operation. |
| `filter` | `string` | No | `Null` | Substring that `filter_factors` should include. `filter_factors` is a concatenation of various operation attributes, such as `id`, `type`, `authenticated_user`, `state`, `input_table_paths`, `output_table_paths`, `experiments`, `annotations`, `runtime_parameters`, `pool`, and `title`. |
| `pool` | `string` | No | `Null` | Pool used for filtering. |
| `pool_tree` | `string` | No | `Null` | Pool tree used for filtering. |
| `with_failed_jobs` | `bool` | No | `Null` | Return only the operations that have jobs with the `failed` status. |
| `access` | `map` | No | `Null` | Dictionary with the mandatory fields `subject` (a string) and `permissions` (a list of strings) that set a filter by access rights. If specified, only the operations for which a `subject` has every right in the `permissions` list, are returned. |
| `include_archive` | `bool` | No | `false` | Whether to request operations from the archive. |
| `include_counters` | `bool` | No | `true` | Whether to return statistics for the requested operations. |
| `limit` | `int` | No | `100` | List of operations that need to be returned in the response. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

| **Parameter** | **Type** | **Description** |
| ------------ | ------- | ------------ |
| `operations` | `list<TOperation>` | List of operations. |
| `incomplete` | `bool` | Whether the list of operations is complete. That is, whether all the operations in the range `from_time` — `to_time` are listed. |
| `type_counts` | `map<string, int>` | Map indicating the number of operations of various types that match all specified filters (except the filter by type). |
| `state_counts` | `map<string, int>` | Map indicating the number of operations with various states that match all specified filters (except the filter by state). |
| `type_counts` | `map<string, int>` | Map indicating the number of operations of various types that match all specified filters (except the filter by type). |
| `pool_counts` | `map<string, int>` | Map indicating the number of operations in various pools that match all specified filters (except the filter by pool). |
| `pool_tree_counts` | `map<string, int>` | Map indicating the number of operations in various pool trees that match all specified filters (except the filter by pool tree). |
| `failed_job_count` | `int` | Number of unsuccessful jobs with the `failed` state. |

Example:

```bash
PARAMETERS { }
OUTPUT {
      "operations" = [
          {
              "id" = "7001208d-fef089b3-3fe03e8-453d99a1";
              "type" = "remote-copy";
              "state" = "initializing";
              "authenticated_user" = "user-name";
              "brief_progress" = {};
              "brief_spec" = {
                  ...
              };
              "start_time" = "2018-02-06T11:06:34.200591Z";
              "suspended" = %false;
              "weight" = 1.;
          };
      ];
      "incomplete" = %true;
      "pool_counts" = {
          "pool-counts-example" = 2;
          "user-name-1" = 2;
          ...
      };
      "user_counts" = {
          "yql" = 52;
          "user-name-1" = 2;
      };
      "state_counts" = {
          "materializing" = 10;
          "pending" = 763;
          "running" = 1848;
          "completed" = 6654;
          "aborted" = 37;
          "failed" = 98;
      };
      "type_counts" = {
          "map" = 4294;
          "merge" = 1337;
          "erase" = 97;
          "sort" = 1126;
          "reduce" = 886;
          "map_reduce" = 1609;
          "remote-copy" = 24;
      };
      "failed_jobs_count" = 109;
}
```

### get_operation { #get_operation }

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get information about an operation.

Parameters:

| **Parameter** | **Type** | **Required** | **Default value** | **Description** |
| ------------ | ------- | ----------------- | ------------------------- | ----------------------- |
| `operation_id (operation_alias)` | `GUID (string)`    | Yes |                           | Operation ID. |
| `attributes` | `list` | No | `[]` | Operation attributes. |

{% note info %}

An operation can be accessed either via `operation_id` or `operation_alias`. For more information about operation aliases, see the section [Operation options](../../user-guide/data-processing/operations/operations-options.md#common_options).

{% endnote %}

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Operation description (`TOperation`).


Example:

```bash
PARAMETERS {  "operation_id" = "33ab3f-bf1df917-b35fe9ed-c70a4bf4"; attributes = [ "state" ] }
OUTPUT {
    "state" = "running";
}
```

### list_operation_events { #list_operation_events }

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get operation events.

Parameters:

| **Parameter** | **Type** | **Required** | **Default value** | **Description** |
| -------------- | --------- | ---------------- | ------------------------- | -------------------------------------------------------------------- |
| `operation_id (operation_alias)` | `GUID (string)` | Yes |                           | Operation ID. |
| `event_type` | `string` | No | `Null` | Event type. If the value is empty, it returns events of all types. |

Input data:

- Type: `null`.

Output data:

- Type: `list`.
- The list of the `TOperationEvent` operation events.

`TOperationEvent` is a structure that contains information about an operation event. The following event types are supported:

- `incarnation_started` — start of a new incarnation of a gang operation.

All events, irrespective of their type, have the following fields:

| **Field** | **Type** | **Description** |
|--------------------------|-------------------|---------------------------------------------------------------------------------------------------|
| `timestamp` | `ISO 8601 string` | Event time. |
| `type` | `string` | Event type. |

Other fields depend on the event type.

- `incarnation_started`:

| **Parameter** | **Type** | **Required** | **Default value** | **Description** |
| --------------------------- | -------- | ----------------- |--------------------------- | --------------------------------------------------------------------------------------- |
| `incarnation` | `string` | Yes |                            | Incarnation identifier. |
| `incarnation_switch_reason` | `string` | No | `Null` | Incarnation switch reason. The empty value refers to an incarnation at the operation start. |
| `incarnation_switch_info` | `map` | No | `{}` | Additional information about the incarnation switch reason. |

Example:

```bash
PARAMETERS { "operation_id" = "33ab3f-bf1df917-b35fe9ed-c70a4bf4" }
OUTPUT {
    [
        {
            "timestamp" = "2025-07-03T08:01:41.497318Z";
            "event_type" = "incarnation_started";
            "incarnation" = "d31999cc-7ee0f616-ecb005cb-b0ec2d45";
            "incarnation_switch_info" = {};
        };
    ]
}
```

### abort_operation

Command properties: **Mutating**, **Light**.

Semantics:

- Abort the operation.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `operation_id (operation_alias)` | Yes |                           | Operation ID. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "operation_id" = "33ab3f-bf1df917-b35fe9ed-c70a4bf4" }
```

### complete_operation

Command properties: **Mutating**, **Light**.

Semantics:

- Instantly complete the operation, saving the currently calculated result.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `operation_id (operation_alias)` | Yes |                           | Operation ID. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "operation_id" = "33ab3f-bf1df917-b35fe9ed-c70a4bf4" }
```

### suspend_operation

Command properties: **Mutating**, **Light**.

Semantics:

- Suspend the operation.
- This command is executed instantly. After that, no more jobs are started for the operation, and the running jobs are optionally aborted.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------------ | ------------- | ------------------------- | ----------------------------------------------- |
| `operation_id (operation_alias)` | Yes |                           | Operation ID. |
| `abort_running_jobs` | No | `false` | Whether to abort the running operation jobs. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "operation_id" = "33ab3f-bf1df917-b35fe9ed-c70a4bf4" }
```

### resume_operation

Command properties: **Mutating**, **Light**.

Semantics:

- Resume the suspended operation.
- This command is executed instantly.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `operation_id (operation_alias)` | Yes |                           | Operation ID. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "operation_id" = "33ab3f-bf1df917-b35fe9ed-c70a4bf4" }
```

### update_operation_parameters { #update_operation_parameters }

Command properties: **Mutating**, **Light**.

Semantics:

- Update the parameters of a running operation.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| -------------------------------------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `operation_id (operation_alias)` | Yes |                           | Operation ID. |
| `parameters` | Yes |                           | Dictionary with the operation parameters. |
| *parameters[owners]* | No |                           | (deprecated, will be removed) List of new owners of the operation. |
| *parameters[acl]* | No |                           | New ACL of the operation (it overlaps the base ACL). |
| *parameters[pool]* | No |                           | Name of the pool to which the operation has to be switched over in all its trees. |
| *parameters[weight]* | No |                           | New weight of the operation in all the trees. |
| *parameters[scheduling_options_per_pool_tree]* | No |                           | Dictionary `{tree name: scheduler settings for this tree}`. The settings are described below. To learn more about the scheduler, see [Scheduler and pools](../../user-guide/data-processing/scheduler/scheduler-and-pools.md). |
| *parameters[options_per_job_shell]* | No |                           | Dictionary `{Job Shell name: settings for this Job Shell}`. The settings are described below. |

Scheduler settings for the tree:

| **Parameter** | **Required** | **Default value** | **Description** |
| --------------- | ------------- | ------------------------- | ------------------------------------------------------- |
| `weight` | No |                           | Weight of the operation in the tree. |
| `pool` | No |                           | Pool of the operation in the tree. |
| `resource_limits` | No |                           | Dictionary `{ resource name: limit}`. Resource limits in the tree. |

Job Shell settings:

| **Parameter** | **Required** | **Default value** | **Description** |
| --------------- | ------------- | ------------------------- | ------------------------------------------------------- |
| `owners` | No |                           | Subjects (users or groups) who will have access to the Job Shell. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS {"operation_id" = "33ab3f-bf1df917-b35fe9ed-c70a4bf4"; "parameters" = {"pool" = "username"; "scheduling_options_per_pool_tree" = {"{{pool-tree}}" = {"weight" = 2; "resource_limits" = { "user_slots" = 1; "cpu" = 0.5; "network" = 10; "memory" = 1000000000}}}}}
```

### patch_operation_spec { #patch_operation_spec }

Command properties: **Mutating**, **Lightweight**.

Semantics:

- Update the specification of a running operation.

Parameters:

| **Parameter**                    | **Required** | **Description**                                              |
| -------------------------------- | ------------ | ------------------------------------------------------------ |
| `operation_id (operation_alias)` | Yes          | Operation ID.                                                |
| `patches`                        | Yes          | List of patches to apply to the operation specification.     |

Patch format:

| **Parameter** | **Required** | **Description**                                              |
| ------------- | ------------ | ------------------------------------------------------------ |
| `path`        | Yes          | Path to the parameter in the operation specification in [YPath](../../user-guide/storage/ypath.md) format. |
| `value`       | Yes          | New value for the parameter.                                 |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Modifiable parameters:

| **Parameter**                    | **Description**                                              | **Comment** |
| -------------------------------- | ------------------------------------------------------------ | ----------- |
| `/max_failed_job_count`          | Number of failed jobs after which the operation is considered failed. | |
| `/tasks/<task_name>/job_count`   | Number of jobs for task `<task_name>` in Vanilla operations. | Cannot be modified for gang operations or operations with `fail_on_job_restart`. |

Notes:

- This command allows you to modify individual parameters of an operation specification on the fly, without needing to restart the operation.
- Patches are applied atomically: either all patches from the list are applied, or none.
- Changing specification parameters may cause running jobs to be aborted if the desired job count is less than the current count.

Example:

```bash
PARAMETERS {
    "operation_id" = "33ab3f-bf1df917-b35fe9ed-c70a4bf4";
    "patches" = [
        {
            "path" = "/max_failed_job_count";
            "value" = 15;
        };
        {
            "path" = "/tasks/worker/job_count";
            "value" = 10;
        };
        {
            "path" = "/tasks/coordinator/job_count";
            "value" = 5;
        };
    ]
}
```

### check_operation_permission { #check_operation_permission }

Command properties: **Idempotent**, **Lightweight**.

Semantics:

- Check if a user has a specific permission for an operation.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `operation_id` | Yes | | Operation identifier. |
| `user` | Yes | | Username for which permission needs to be checked. |
| `permission` | Yes | | Permission to check. Possible values: `read`, `manage`, `administer`. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

| **Parameter** | **Type** | **Description** |
| ------------ | -------- | ------------ |
| `action` | `string` | Permission check result. |

{% note info "Note" %}

Having `manage` permission for a pool allows you to perform some [actions](../../user-guide/data-processing/scheduler/manage-pools.md#allowed_pool_actions) on operations running in the pool without formal permissions for the operation. In such cases, the command will indicate that the user does not have permission, as it only checks the permissions for the operation itself.

{% endnote %}

Example:

```bash
PARAMETERS {
    "operation_id" = "33ab3f-bf1df917-b35fe9ed-c70a4bf4";
    "user" = "vasya";
    "permission" = "manage";
}
OUTPUT {
    "action" = "allow"
}
```

## Working with jobs

{% note info "Note" %}

All the commands used to work with jobs are non-[transactional](#transactions).

{% endnote %}

The table describes the `TJob` type, which contains information about the job returned by the `get_job` and `list_jobs` commands:

| Field | Type | Description |
|------------------------------|-------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `job_id`/`id` | `string` | String representation of the job ID (for example, `d6bf44e17-8c200aed-41103e8-4958aff3`). The field name depends on the command: `job_id` for `get_job` and `id` for `list_jobs`. |
| `operation_id` | `string` | String representation of the operation ID (for example, `2cb654e4-9ab04944-4110384-100d7ed`). |
| `allocation_id` | `string` | String representation of the ID of the allocation used to execute the job (for example, `2cb654e4-9ab04944-4110384-d7ed`). |
| `type` | `string` | Job type (for example, `vanilla`, `map`, `partition_reduce`). |
| `state` | `string` | Current job state (for example, `running`, `failed`, `completed`). |
| `controller_state` | `string` | Job state according to the controller. |
| `archive_state` | `string` | Job state according to the node. |
| `address` | `string` | Address of the exec node (host:port) that ran the job. If the node has multiple addresses, the value matches the default address. |
| `addresses` | `map` | Map containing the addresses of the exec node that ran the job. Used when the exec node can have multiple addresses. |
| `task_name` | `string` | Name of the task the job belonged to. |
| `start_time` | `ISO 8601 string` | Job start time. |
| `finish_time` | `ISO 8601 string` | Job end time. |
| `has_spec` | `bool` | Flag indicating whether the job specification was archived. |
| `has_competitors` | `bool` | Flag indicating whether the job is speculative or has speculative competitors. |
| `has_probing_competitors` | `bool` | Flag indicating whether the job is a probe or has probe competitors. |
| `job_competition_id` | `string` | For speculative jobs, matches the ID of the competitor job. |
| `probing_job_competition_id` | `string` | For probes, matches the ID of the competitor job. |
| `pool` | `string` | Name of the [pool](../../user-guide/data-processing/scheduler/scheduler-and-pools.md) where the job was run. |
| `pool_tree` | `string` | Name of the [pool tree](../../user-guide/data-processing/scheduler/scheduler-and-pools.md) where the job was run. |
| `progress` | `float in [0,1]` | Evaluation of the share of work executed by the job by the current moment. |
| `stderr_size` | `int` | Size of the saved stderr of the job (you can get stderr using the [`get_job_stderr`](#get_job_stderr) command). |
| `fail_context_size` | `int` | Size of the saved fail_context of the job (you can get fail_context using the [`get_fail_context`](#get_fail_context) command). |
| `error` | `map` | Error that interrupted the job. |
| `statistics` | `map` | Job statistics. |
| `brief_statistics` | `map` | Summarized job statistics. |
| `exec_attributes` | `map` | Metainformation about the job's initiation on the exec node. |
| `monitoring_descriptor` | `string` | ID used by the monitoring system to store information about the job. |
| `job_cookie` | `int` | Unique job index within the operation task that serves as an implementation detail of the operation controller. In vanilla operations, it can be used to group workers: if a job is interrupted, the new job launched to replace it will have the same `job_cookie`. |
| `operation_incarnation` | `string` | ID of the gang operation incarnation. |
| `interruption_info` | `map` | Information about the reasons for the job's interruption. |
| `archive_features` | `map` | Information about the job's archived attributes. |
| `core_infos` | `map` | List of metainformation about the job's saved core dumps. |
| `events` | `map` | List of job state transition events. |
| `is_stale` | `bool` | Whether the information about the job is outdated (if `%true`, some fields might need update). Information about the job is considered outdated if it hasn't been updated for a long time. The information in the job archive is updated by the node running the job and the operation controller. The update process is asynchronous. If the node and the controller restart at the same time for some reason (for example, as a result of an update), the information about the final job state (`completed`, `failed`, or `aborted`) might not end up in the archive, resulting in this job always returning as stale. Despite the `running` status, such jobs likely haven't been running for a long time and should be ignored. |

### get_job { #get_job }

Command properties: **Non-mutating**, **Light**.

Semantics:

- Getting information about a job. The command can run both for running and completed jobs (if information about the jobs was saved).

Parameters:

| **Parameter** | **Type** | **Required** | **Default value** | **Description** |
| -------------- | ------------- | ---------------- | ------------------------- | ------------ |
| `operation_id (operation_alias)` | `GUID (string)` | Yes |                           | Operation ID. |
| `job_id` | `GUID` | Yes |                           | Job ID. |
| `attributes` | `list<string>` | No | `Null` | List of job attributes that need to be returned in the response. |

The `attributes` parameter is a subset of `TJob` objects. If it's `Null`, all job attributes are returned; otherwise, the user receives only the attributes specified in `attributes`.

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Job description (`TJob`).

Example:

```bash
PARAMETERS { "operation_id" = "e13c5406-e5dd6f5d-3fe03e8-fe05f0d3"; "job_id" = "f11ae559-a0375703-3fe0384-8f1"}
OUTPUT {
      "operation_id" = "e13c5406-e5dd6f5d-3fe03e8-fe05f0d3";
      "job_id" = "f11ae559-a0375703-3fe0384-8f1";
      "state" = "completed";
      "start_time" = "2018-02-06T09:37:02.858492Z";
      "finish_time" = "2018-02-06T09:42:19.185525Z";
      "address" = "hostname.net:9012";
      "statistics" = {
          "data" = {
               ...
          };
      };
      "events" = [
          ...
      ];
}
```


### list_jobs { #list_jobs }

{% note warning "Attention" %}

This command can create significant load on the cluster. Do not use it in your workflow without a prior approval by the administrator.

{% endnote %}

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get all the jobs that match the filters for a given operation.

Parameters:

| **Parameter** | **Type** | **Required** | **Default value** | **Description** |
| -------------------- | ------------------------------------------------------------ | ---------------- | ------------------------- | ------------------------------------------------------------ |
| `operation_id (operation_alias)` | `GUID (string)` | Yes |                           | Operation ID. |
| `type (job_type)` | `EJobType` | No | `Null` | When you specify the parameter, the response will only include the jobs with the specified `job_type`. |
| `state (job_state)` | `EJobState` | No | `Null` | When you specify the parameter, the response will only include the jobs with the specified `job_state`. |
| `address` | `string` | No | `Null` | If this parameter is specified, the response will only include the jobs with an address that starts with `address`. |
| `with_stderr` | `bool` | No | `Null` | At `Null`, all the jobs are returned. At `True`, only the jobs with a non-null `stderr` are returned. At `False`, only the jobs with a null `stderr` are returned. |
| `with_fail_context` | `bool` | No | `Null` | At `Null`, all the jobs are returned. At `True`, only the jobs that have a saved `fail_context` are returned. At `False`, only the jobs that do not have `fail_context` are returned. |
| `with_spec` | `bool` | No | `Null` | At `Null`, all the jobs are returned. At `True`, only the jobs that have a saved specification are returned. At `False`, only the jobs that do not have a specification are returned. |
| `with_competitors` | `bool` | No | `Null` | At `Null`, all the jobs are returned. At `True`, only the jobs for which speculative copies were run along with those copies are returned. At `False`, only the jobs that do not have speculative copies are returned. |
| `job_competition_id` | `GUID` | No | `Null` | When you specify the parameter, the response will include the job with the `job_competition_id` and all of its speculative copies (if any). |
| `with_monitoring_descriptor` | `bool` | No | `Null` | At `Null`, all the jobs are returned. At `True`, only the jobs that have `monitoring_descriptor` are returned. At `False`, only the jobs that do not have `monitoring_descriptor` are returned. |
| `with_interruption_info` | `bool` | No | `Null` | At `Null`, all the jobs are returned. At `True`, only the jobs that have `interruption_info` are returned. At `False`, only the jobs that do not have `interruption_info` are returned. |
| `task_name` | `string` | No | `Null` | When you specify the parameter, the response will only include the jobs with the specified `task_name`. |
| `operation_incarnation` | `string` | No | `Null` | When you specify the parameter, the response will only include the jobs belonging to the requested incarnation. |
| `from_time` | `ISO 8601 string` | No | `Null` | Bottom limit for the time interval for operation selection (based on the time when the operation began). |
| `to_time` | `ISO 8601 string` | No | `Null` | Upper limit for the time interval for operation selection (based on the time when the operation began for running jobs and based on the time the operation ended for completed jobs). |
| `continuation_token` | `string` | No | `Null` | String used for pagination (see below). |
| `sort_field` | `{none,type,state,start_time,finish_time,address,duration,progress,id}` | No | `none` | Sort fields. |
| `sort_order` | `{ascending,descending}` | No | `ascending` | Sorting order. |
| `limit` | `int` | No | `1000` | Limit on the number of returned jobs. |
| `offset` | `int` | No | `0` | Offset by the given number of jobs. |
| `attributes` | `list<string>` | No | `Null` | List of job attributes that need to be returned in the response. |

The `job_type`, `job_state`, `address`, `with_stderr`, `with_fail_context`, `with_competitors`, `with_spec`, `with_monitoring_descriptor`, and `with_interruption_info` parameters define the job *filter*. The response will only include the jobs that meet the filtering criteria.

The `sort_field` and`sort_order` define the order of jobs in the response. In this case, the `limit` and `offset` parameters define the slice (subset) of jobs in the response: the first `offset` jobs are skipped, and then `limit` of the remaining jobs is selected.

The `attributes` parameter is a list of `TJob` objects. If it's `Null`, default attributes are returned; otherwise, the user receives only the attributes specified in `attributes`.

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

| **Parameter** | **Type** | **Description** |
| ------------ | ------- | ------------ |
| `jobs` | `list<TJob>` | List of jobs. |
| `controller_agent_job_count` | `int` | Number of jobs in the response that the operation controller stores information about. |
| `archive_job_count` | `int` | Number of jobs in the response that the archive stores information about. |
| `type_counts` | `map<string, int>` | Map indicating the number of jobs of various types that match all specified filters (except the filter by type). |
| `state_counts` | `map<string, int>` | Map indicating the number of jobs with various states that match all specified filters (except the filter by state). |
| `errors` | `list<map>` | List of errors. |
| `continuation_token` | `string` | String used for pagination. It encodes the request parameters along with the index from which the job list should continue. You can pass it in the `continuation_token` parameter. |

Example:

```bash
PARAMETERS { "operation_id" = "4505e8eb-28fa88e2-3fe03e8-c6fcd8fa"; }
OUTPUT {
      "jobs" = [
          {
              "id" = "55aff293-7ef14284-3fe0384-3e07";
              "type" = "map";
              "state" = "failed";
              "address" = "hostname.net:9012";
              "start_time" = "2018-05-05T00:41:27.433832Z";
              "finish_time" = "2018-05-05T00:49:04.288196Z";
              "fail_context_size" = 973230u;
              "error" = {
                  "code" = 1205;
                  "message" = "User job failed";
                  ...
              };
              ...
          };
          ...
          {
              "id" = "69ae20a7-887b25ab-3fe0384-3cff";
              "type" = "map";
              "state" = "running";
              "address" = "hostname.net:9012";
              "start_time" = "2018-05-07T13:04:03.339873Z";
              "progress" = 0.;
              "brief_statistics" = <
                  "timestamp" = "2018-05-07T13:04:08.431740Z";
              > {
                  "processed_input_compressed_data_size" = 0;
                  "processed_input_data_weight" = 0;
                  "processed_output_uncompressed_data_size" = 0;
                  "processed_output_compressed_data_size" = 0;
                  "processed_input_uncompressed_data_size" = 0;
                  "processed_input_row_count" = 0;
              };
          };
      ];
      "cypress_job_count" = 200;
      "scheduler_job_count" = 208;
      "archive_job_count" = #;
      "type_counts" = {
          "map" = 408;
      };
      "state_counts" = {
          "running" = 208;
          "failed" = 200;
      };
}
```

### abandon_job

Command properties: **Mutating**, **Light**.

Semantics:

- Abort the job and consider that its input data was processed successfully.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | -------------------- |
| `job_id` | Yes |                           | Job ID. |

Input data:

- Type: `null`

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "job_id" = "1225d-1f2fb8c4-f1075d39-5fb7cdff" }
```

### abort_job

Command properties: **Mutating**, **Light**.

Semantics:

- Abort the job, and let it later be restarted by the scheduler (as any aborted operation job).

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ----------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `job_id` | Yes |                           | Job ID. |
| `interrupt_timeout` | No |                           | A timeout for successful completion of the job after it stops receiving input data. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "job_id" = "1225d-1f2fb8c4-f1075d39-5fb7cdff" }
```

### dump_job_context

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get the `input context` of the job.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `job_id` | Yes |                           | Job ID. |
| `path` | Yes |                           | A path where to save the set of input parameters received by the job. The path's components should exist (except for the file itself). |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "job_id" = "1225d-1f2fb8c4-f1075d39-5fb7cdff"; "path" = "//tmp/input_context" }
```

### get_job_input { #get_job_input }

Command properties: **Non-mutating**, **Heavy**.

Semantics:

- Get the full input for the job. The command is used both with running and failed jobs with a saved specification and all the input data available.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | -------------------- |
| `job_id` | Yes |                           | Job ID. |

Input data:

- Type: `null`.

Output data:

- Type: `binary`.
- Value: Job input.

Example:

```bash
PARAMETERS { "job_id" = "1225d-1f2fb8c4-f1075d39-5fb7cdff"}
```

### get_job_fail_context { #get_job_fail_context }

Command properties: **Non-mutating**, **Heavy**.

Semantics:

- Get the `fail context` of the job. The command is used with failed jobs.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `operation_id (operation_alias)` | Yes |                           | Operation ID. |
| `job_id` | Yes |                           | Job ID. |

Input data:

- Type: `null`.

Output data:

- Type: `binary`.
- Value: `Fail context` of the job.

Example:

```bash
PARAMETERS { "operation_id" = "33ab3f-bf1df917-b35fe9ed-c70a4bf4"; "job_id" = "1225d-1f2fb8c4-f1075d39-5fb7cdff"}
```

### get_job_stderr { #get_job_stderr }

Command properties: **Non-mutating**, **Heavy**.

Semantics:

- Get the stderr of the job. The command is used with failed jobs.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `operation_id (operation_alias)` | Yes |                           | Operation ID. |
| `job_id` | Yes |                           | Job ID. |
| `offset` | No |                            | Offset from the beginning in bytes. |
| `limit` | No |                            | Maximum size in bytes. |

Input data:

- Type: `null`.

Output data:

- Type: `binary`.
- Value: The stderr of the job.

Example:

```bash
PARAMETERS { "operation_id" = "33ab3f-bf1df917-b35fe9ed-c70a4bf4"; "job_id" = "1225d-1f2fb8c4-f1075d39-5fb7cdff"; "offset" = 500; "limit" = 100 }
OUTPUT {
OUTPUT   "total_size" = 1000;
OUTPUT   "end_offset" = 600;
OUTPUT }
```


### get_job_spec

Command properties: **Non-mutating**, **Heavy**.

Semantics:

- Get the specification of a job.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `job_id` | Yes |                           | Job ID. |
| `omit_node_directory` | No | `true` | Whether to omit node directory information. |
| `omit_input_table_specs` | No | `false` | Whether to omit input table specifications. |
| `omit_output_table_specs` | No | `false` | Whether to omit output table specifications. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Job specification.

### get_job_input_paths

Command properties: **Non-mutating**, **Heavy**.

Semantics:

- Get the input paths for a job.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `job_id` | Yes |                           | Job ID. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: List of input paths.

### dump_job_proxy_log

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Dump the job proxy log for a specific job.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `job_id` | Yes |                           | Job ID. |
| `operation_id` | Yes |                           | Operation ID containing the job. |
| `path` | Yes |                           | Path where to dump the log. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

### get_job_trace

Command properties: **Non-mutating**, **Heavy**.

Semantics:

- Get trace information for a job.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `job_id` | Yes |                           | Job ID. |
| `trace_id` | No |                           | Specific trace ID to retrieve. |
| `from_time` | No |                           | Start time for trace filtering. |
| `to_time` | No |                           | End time for trace filtering. |

Input data:

- Type: `null`.

Output data:

- Type: `binary`.
- Value: Job trace data.

### list_job_traces

Command properties: **Non-mutating**, **Light**.

Semantics:

- List available traces for a job.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `job_id` | Yes |                           | Job ID. |
| `limit` | No |                           | Maximum number of traces to return. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: List of job traces.

### poll_job_shell

Command properties: **Mutating**, **Light**.

Semantics:

- Poll for output from a job shell command.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `job_id` | Yes |                           | Job ID. |
| `parameters` | Yes |                           | Poll parameters including shell descriptor. |
| `shell_name` | No |                           | Name of the shell. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Shell output.

### run_job_shell_command

Command properties: **Mutating**, **Heavy**.

Semantics:

- Run a shell command in a job's environment.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `job_id` | Yes |                           | Job ID. |
| `command` | Yes |                           | Shell command to execute. |
| `shell_name` | No |                           | Name of the shell. |

Input data:

- Type: `null`.

Output data:

- Type: `binary`.
- Value: Command output.



## Working with queries { #queries }

The Query Tracker system enables executing SQL-like queries across YTsaurus data.

### start_query

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Start a new query in the Query Tracker system.
- The query will be executed asynchronously by the specified query engine.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `engine` | Yes |                           | Query engine to use (e.g., `yql`, `chyt`, `spyt`). |
| `query` | Yes |                           | Query text to execute. |
| `settings` | No |                           | Engine-specific query settings. |
| `files` | No |                           | List of files to attach to the query. |
| `stage` | No |                           | Query Tracker stage to use (e.g., `production`, `testing`). |
| `draft` | No | `false` | If `true`, the query is created as a draft and not executed immediately. |
| `annotations` | No |                           | User-defined annotations for the query. |
| `access_control_object` | No |                           | Access control object name. |
| `access_control_objects` | No |                           | List of access control object names. |
| `secrets` | No |                           | Map of secret values for the query. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Query ID.

Example:

```bash
PARAMETERS { "engine" = "yql"; "query" = "SELECT * FROM `//tmp/table`" }
OUTPUT "1a2b3c4d-5e6f7g8h-9i0j1k2l-3m4n5o6p"
```

### abort_query

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Abort a running query.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `query_id` | Yes |                           | ID of the query to abort. |
| `stage` | No |                           | Query Tracker stage to use (e.g., `production`, `testing`). |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

Example:

```bash
PARAMETERS { "query_id" = "1a2b3c4d-5e6f7g8h-9i0j1k2l-3m4n5o6p" }
OUTPUT { }
```

### get_query

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get information about a query.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `query_id` | Yes |                           | Query ID. |
| `stage` | No |                           | Query Tracker stage to use (e.g., `production`, `testing`). |
| `attributes` | No |                           | List of attributes to fetch. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Query information including state, progress, and results.

Example:

```bash
PARAMETERS { "query_id" = "1a2b3c4d-5e6f7g8h-9i0j1k2l-3m4n5o6p" }
OUTPUT {
    "id" = "1a2b3c4d-5e6f7g8h-9i0j1k2l-3m4n5o6p";
    "state" = "completed";
    "engine" = "yql";
    "query" = "SELECT * FROM `//tmp/table`";
    "result_count" = 1;
}
```

### list_queries

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Non-mutating**, **Light**.

Semantics:

- List queries matching the specified filters.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `stage` | No | `production` | Query Tracker stage to use (e.g., `production`, `testing`). |
| `from_time` | No |                           | Start time for query filtering. |
| `to_time` | No |                           | End time for query filtering. |
| `cursor_time` | No |                           | Cursor time for pagination. |
| `cursor_direction` | No | `past` | Cursor direction: `past` or `future`. |
| `user` | No |                           | Filter by user. |
| `state` | No |                           | Filter by query state. |
| `engine` | No |                           | Filter by query engine. |
| `filter` | No |                           | Additional filter string. |
| `limit` | No | `100` | Maximum number of queries to return. |
| `attributes` | No |                           | List of attributes to fetch for each query. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: List of queries with pagination information.

Example:

```bash
PARAMETERS { "user" = "root"; "limit" = 10 }
OUTPUT {
    "queries" = [ ... ];
    "incomplete" = %false;
    "timestamp" = 1234567890u;
}
```

### get_query_result

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get the result schema and metadata for a completed query.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `query_id` | Yes |                           | Query ID. |
| `result_index` | No | `0` | Index of the result (queries can produce multiple results). |
| `stage` | No |                           | Query Tracker stage to use (e.g., `production`, `testing`). |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Result metadata including schema and row count.

Example:

```bash
PARAMETERS { "query_id" = "1a2b3c4d-5e6f7g8h-9i0j1k2l-3m4n5o6p"; "result_index" = 0 }
OUTPUT {
    "schema" = [ ... ];
    "data_statistics" = { "row_count" = 1000; };
}
```

### read_query_result

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Non-mutating**, **Heavy**.

Semantics:

- Read the actual data from a query result.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `query_id` | Yes |                           | Query ID. |
| `result_index` | No | `0` | Index of the result to read. |
| `stage` | No |                           | Query Tracker stage to use (e.g., `production`, `testing`). |
| `columns` | No |                           | List of columns to read. |
| `lower_row_index` | No | `0` | Lower row index for range reading. |
| `upper_row_index` | No |                           | Upper row index for range reading. |

Input data:

- Type: `null`.

Output data:

- Type: `tabular`.
- Value: Rows from the query result.

Example:

```bash
PARAMETERS { "query_id" = "1a2b3c4d-5e6f7g8h-9i0j1k2l-3m4n5o6p"; "lower_row_index" = 0; "upper_row_index" = 100 }
```

### alter_query

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Modify query metadata such as annotations or access control settings.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `query_id` | Yes |                           | Query ID. |
| `stage` | No | `production` | Query Tracker stage to use (e.g., `production`, `testing`). |
| `annotations` | No |                           | New annotations for the query. |
| `access_control_object` | No |                           | New access control object name. |
| `access_control_objects` | No |                           | New list of access control object names. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

Example:

```bash
PARAMETERS { "query_id" = "1a2b3c4d-5e6f7g8h-9i0j1k2l-3m4n5o6p"; "annotations" = { "project" = "analytics" } }
OUTPUT { }
```

### get_query_tracker_info

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get information about the Query Tracker system itself.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `stage` | No | `production` | Query Tracker stage to use (e.g., `production`, `testing`). |
| `attributes` | No |                           | List of attributes to fetch. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Query Tracker system information.

Example:

```bash
PARAMETERS { }
OUTPUT {
    "supported_engines" = ["yql"; "chyt"; "spyt"];
    "cluster_name" = "primary";
}
```

### get_query_declared_parameters_info

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get information about parameters declared in a query text.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `stage` | No | `production` | Query Tracker stage to use (e.g., `production`, `testing`). |
| `query` | Yes |                           | Query text to analyze. |
| `engine` | Yes |                           | Query engine. |
| `settings` | No |                           | Engine-specific settings. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Information about declared parameters.

Example:

```bash
PARAMETERS { "engine" = "yql"; "query" = "SELECT * FROM table WHERE id = $user_id" }
OUTPUT {
    "parameters" = [
        { "name" = "user_id"; "type" = "int64" };
    ];
}
```


## Working with queues { #queues }

The Queue system provides durable message queue functionality in YTsaurus.

### register_queue_consumer

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Register a consumer for a queue to track consumption progress.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `queue_path` | Yes |                           | Path to the queue. |
| `consumer_path` | Yes |                           | Path to the consumer. |
| `vital` | Yes |                           | Whether the consumer is vital for the queue. |
| `partitions` | No |                           | List of partition indexes to register for. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

### unregister_queue_consumer

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Unregister a previously registered queue consumer.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `queue_path` | Yes |                           | Path to the queue. |
| `consumer_path` | Yes |                           | Path to the consumer. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

### list_queue_consumer_registrations

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Non-mutating**, **Light**.

Semantics:

- List all consumer registrations for a queue.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `queue_path` | No |                           | Path to the queue (if omitted, list for all queues). |
| `consumer_path` | No |                           | Path to the consumer (if omitted, list for all consumers). |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: List of consumer registrations.

### pull_queue

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Non-mutating**, **Heavy**.

Semantics:

- Pull data from a queue partition starting from a specified offset.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `queue_path` | Yes |                           | Path to the queue. |
| `partition_index` | Yes |                           | Partition index to pull from. |
| `offset` | No | `0` | Offset to start pulling from. |
| `max_row_count` | No |                           | Maximum number of rows to pull. |
| `max_data_weight` | No |                           | Maximum data weight to pull. |
| `data_weight_per_row_hint` | No |                           | Hint for average data weight per row. |

Input data:

- Type: `null`.

Output data:

- Type: `tabular`.
- Value: Rows from the queue.

### pull_queue_consumer

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Non-mutating**, **Heavy**.

Semantics:

- Pull data for a registered consumer from a queue partition.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `consumer_path` | Yes |                           | Path to the consumer. |
| `queue_path` | Yes |                           | Path to the queue. |
| `partition_index` | Yes |                           | Partition index to pull from. |
| `offset` | Yes |                           | Offset to start pulling from. |
| `max_row_count` | No |                           | Maximum number of rows to pull. |
| `max_data_weight` | No |                           | Maximum data weight to pull. |

Input data:

- Type: `null`.

Output data:

- Type: `tabular`.
- Value: Rows from the queue.

### advance_queue_consumer

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Advance a consumer's read position in a queue partition.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `consumer_path` | Yes |                           | Path to the consumer. |
| `queue_path` | Yes |                           | Path to the queue. |
| `partition_index` | Yes |                           | Partition index. |
| `new_offset` | Yes |                           | New offset for the consumer. |
| `old_offset` | No |                           | Expected old offset (for optimistic locking). |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

### create_queue_producer_session

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Create a producer session for writing to a queue.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `producer_path` | Yes |                           | Path to the producer. |
| `queue_path` | Yes |                           | Path to the queue. |
| `session_id` | Yes |                           | Unique session identifier. |
| `user_meta` | No |                           | User-defined metadata for the session. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Session information including epoch and sequence number.

### remove_queue_producer_session

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Remove a producer session.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `producer_path` | Yes |                           | Path to the producer. |
| `queue_path` | Yes |                           | Path to the queue. |
| `session_id` | Yes |                           | Session identifier to remove. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

### push_queue_producer

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Push rows to a queue through a producer session.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `producer_path` | Yes |                           | Path to the producer. |
| `queue_path` | Yes |                           | Path to the queue. |
| `session_id` | Yes |                           | Producer session identifier. |
| `epoch` | Yes |                           | Session epoch number. |
| `sequence_number` | No |                           | Starting sequence number for the rows. |

Input data:

- Type: `tabular`.
- Value: Rows to push to the queue.

Output data:

- Type: `structured`.
- Value: Last sequence number and skipped row count.

## Working with pipelines and flows { #flows }

The Flow system provides pipeline management and execution capabilities.

### get_pipeline_spec

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get the specification of a pipeline.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `pipeline_path` | Yes |                           | Path to the pipeline. |
| `spec_path` | No |                           | Path within the spec to retrieve. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Pipeline specification and version.

### set_pipeline_spec

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Set or update the specification of a pipeline.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `pipeline_path` | Yes |                           | Path to the pipeline. |
| `spec_path` | No |                           | Path within the spec to set. |
| `force` | No | `false` | Force update even if validation fails. |
| `expected_version` | No |                           | Expected current version (for optimistic locking). |

Input data:

- Type: `structured`.
- Value: Pipeline specification.

Output data:

- Type: `structured`.
- Value: New version.

### remove_pipeline_spec

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Remove a pipeline specification or a part of it.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `pipeline_path` | Yes |                           | Path to the pipeline. |
| `spec_path` | Yes |                           | Path within the spec to remove. |
| `force` | No | `false` | Force removal even if validation fails. |
| `expected_version` | No |                           | Expected current version. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: New version.

### get_pipeline_dynamic_spec

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get the dynamic specification of a pipeline.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `pipeline_path` | Yes |                           | Path to the pipeline. |
| `spec_path` | No |                           | Path within the dynamic spec to retrieve. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Dynamic pipeline specification and version.

### set_pipeline_dynamic_spec

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Set or update the dynamic specification of a pipeline.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `pipeline_path` | Yes |                           | Path to the pipeline. |
| `spec_path` | No |                           | Path within the dynamic spec to set. |
| `expected_version` | No |                           | Expected current version. |

Input data:

- Type: `structured`.
- Value: Dynamic pipeline specification.

Output data:

- Type: `structured`.
- Value: New version.

### remove_pipeline_dynamic_spec

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Remove a pipeline dynamic specification or a part of it.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `pipeline_path` | Yes |                           | Path to the pipeline. |
| `spec_path` | Yes |                           | Path within the dynamic spec to remove. |
| `expected_version` | No |                           | Expected current version. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: New version.

### start_pipeline

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Start execution of a pipeline.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `pipeline_path` | Yes |                           | Path to the pipeline. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

### stop_pipeline

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Stop execution of a pipeline.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `pipeline_path` | Yes |                           | Path to the pipeline. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

### pause_pipeline

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Pause execution of a pipeline.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `pipeline_path` | Yes |                           | Path to the pipeline. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

### get_pipeline_state

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get the current state of a pipeline.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `pipeline_path` | Yes |                           | Path to the pipeline. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Pipeline state.

### get_flow_view

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get a view of the flow for a pipeline.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `pipeline_path` | Yes |                           | Path to the pipeline. |
| `view_path` | No |                           | Path within the flow view. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Flow view data.

### flow_execute

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Heavy**.

Semantics:

- Execute a flow command on a pipeline.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `pipeline_path` | Yes |                           | Path to the pipeline. |

Input data:

- Type: `structured`.
- Value: Flow command to execute.

Output data:

- Type: `structured`.
- Value: Execution result.

### flow_execute_plaintext

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Heavy**.

Semantics:

- Execute a plaintext flow command on a pipeline.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `pipeline_path` | Yes |                           | Path to the pipeline. |

Input data:

- Type: `null`.

Output data:

- Type: `binary`.
- Value: Plaintext execution result.



## Authentication { #authentication }

### set_user_password

Command properties: **Mutating**, **Light**.

Semantics:

- Set or update a user's password.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `user` | Yes |                           | Username. |
| `current_password_sha256` | No |                           | SHA256 hash of the current password (required for password updates). |
| `new_password_sha256` | Yes |                           | SHA256 hash of the new password. |
| `password_is_temporary` | No | `false` | Whether the password is temporary and must be changed on first use. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

### issue_token

Command properties: **Mutating**, **Light**.

Semantics:

- Issue an authentication token for a user.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `user` | Yes |                           | Username. |
| `password_sha256` | No |                           | SHA256 hash of the user's password. |
| `description` | No |                           | Description for the token. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: The issued token.

### revoke_token

Command properties: **Mutating**, **Light**.

Semantics:

- Revoke an authentication token.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `user` | Yes |                           | Username. |
| `password_sha256` | No |                           | SHA256 hash of the user's password. |
| `token_sha256` | Yes |                           | SHA256 hash of the token to revoke. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

### list_user_tokens

Command properties: **Non-mutating**, **Light**.

Semantics:

- List authentication tokens for a user.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `user` | Yes |                           | Username. |
| `password_sha256` | No |                           | SHA256 hash of the user's password. |
| `with_metadata` | No | `false` | Whether to include token metadata. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: List of tokens.

## Journal operations { #journals }

### read_journal

Command properties: **Non-mutating**, **Heavy**.

Semantics:

- Read rows from a journal.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `path` | Yes |                           | Path to the journal. |

Input data:

- Type: `null`.

Output data:

- Type: `tabular`.
- Value: Rows from the journal.

### write_journal

Command properties: **Mutating**, **Heavy**.

Semantics:

- Write rows to a journal.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `path` | Yes |                           | Path to the journal. |
| `enable_chunk_preallocation` | No | `false` | Whether to enable chunk preallocation for better performance. |

Input data:

- Type: `tabular`.
- Value: Rows to write to the journal.

Output data:

- Type: `structured`.

### truncate_journal

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Truncate a journal to a specified row count.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `path` | Yes |                           | Path to the journal. |
| `row_count` | Yes |                           | Number of rows to keep from the beginning. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

## Chaos and Replication { #chaos }

### alter_replication_card

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Non-mutating**, **Light**.

Semantics:

- Modify a replication card's configuration.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `replication_card_id` | Yes |                           | Replication card ID. |
| `replicated_table_options` | No |                           | New options for replicated tables. |
| `enable_replicated_table_tracker` | No |                           | Whether to enable the replicated table tracker. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

### update_replication_progress

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Non-mutating**, **Light**.

Semantics:

- Update replication progress for a chaos table replica.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `replica_id` | Yes |                           | Replica ID. |
| `progress` | Yes |                           | New replication progress. |
| `force` | Yes |                           | Whether to force the update. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

### ping_chaos_lease

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Ping a chaos lease to extend its lifetime.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `chaos_lease_id` | Yes |                           | Chaos lease ID. |
| `ping_ancestors` | No | `true` | Whether to ping ancestor leases. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

## Shuffle operations { #shuffle }

### start_shuffle

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Start a shuffle operation for distributed data exchange.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `account` | Yes |                           | Account to use for the shuffle. |
| `partition_count` | Yes |                           | Number of partitions. |
| `parent_transaction_id` | Yes |                           | Parent transaction ID. |
| `medium` | No |                           | Storage medium to use. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Signed shuffle handle.

### read_shuffle_data

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Non-mutating**, **Heavy**.

Semantics:

- Read data from a shuffle partition.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `signed_shuffle_handle` | Yes |                           | Signed handle from start_shuffle. |
| `partition_index` | Yes |                           | Partition index to read from. |

Input data:

- Type: `null`.

Output data:

- Type: `tabular`.
- Value: Rows from the shuffle partition.

### write_shuffle_data

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Heavy**.

Semantics:

- Write data to a shuffle operation.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `signed_shuffle_handle` | Yes |                           | Signed handle from start_shuffle. |
| `partition_column` | Yes |                           | Column name to use for partitioning. |

Input data:

- Type: `tabular`.
- Value: Rows to write to the shuffle.

Output data:

- Type: `structured`.

## Distributed write sessions { #distributed_write }

### start_distributed_write_session

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Start a distributed write session for parallel writing to a table.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `path` | Yes |                           | Path to the target table. |
| `cookie_count` | No |                           | Number of write cookies to generate. |
| `session_timeout` | No |                           | Session timeout duration. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Session information and write cookies.

### ping_distributed_write_session

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Ping a distributed write session to keep it alive.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `session` | Yes |                           | Session identifier. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

### finish_distributed_write_session

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Finish a distributed write session and commit all writes.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `session` | Yes |                           | Session identifier. |
| `results` | Yes |                           | Write results from all participants. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

### write_table_fragment

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Heavy**.

Semantics:

- Write a fragment of data to a table as part of a distributed write session.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `cookie` | Yes |                           | Write cookie from start_distributed_write_session. |

Input data:

- Type: `tabular`.
- Value: Rows to write.

Output data:

- Type: `structured`.
- Value: Signed write result.

### start_distributed_write_file_session

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Start a distributed write session for parallel writing to a file.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `path` | Yes |                           | Path to the target file. |
| `cookie_count` | No |                           | Number of write cookies to generate. |
| `session_timeout` | No |                           | Session timeout duration. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Session information and write cookies.

### ping_distributed_write_file_session

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Ping a distributed file write session to keep it alive.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `session` | Yes |                           | Session identifier. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

### finish_distributed_write_file_session

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Finish a distributed file write session and commit all writes.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `session` | Yes |                           | Session identifier. |
| `results` | Yes |                           | Write results from all participants. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

### write_file_fragment

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Heavy**.

Semantics:

- Write a fragment of binary data to a file as part of a distributed write session.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `cookie` | Yes |                           | Write cookie from start_distributed_write_file_session. |

Input data:

- Type: `binary`.
- Value: Binary data to write.

Output data:

- Type: `structured`.
- Value: Signed write result.



## Other

### parse_ypath

Command properties: **Non-mutating**, **Light**.

Semantics:

- Perform parsing of the passed [YPath](../../user-guide/storage/ypath.md) by putting all IDs of the complex YPath into attributes.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------ |
| `path` | Yes |                           | The path that needs to be parsed. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

Example:

```bash
PARAMETERS { "path" = "//tmp/table[#1:#2]" }
OUTPUT { "path" = "<ranges=[{lower_limit={row_index=1};upper_limit={row_index=2}}]>//tmp/table" }
```

### execute_batch { #execute_batch }

Command properties:  **Mutating if the set includes mutating commands**, **Light**.

Semantics:

- Use a single query to execute the set of commands passed in the parameters.
- The command can (and will be) executed in parallel. It means that if a set includes both writing to and reading from the node, the reading result can either be the older value or the updated one.
- The set can only include light commands.
- The set can only include commands with the input type of `null` or `structured`.
- The set can only include commands with the output type of `null` or `structured`.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `requests` | Yes |                           | Description of executed queries. |
| `concurrency` | No | `50` | A numeric parameter that sets the maximum number of commands running on the cluster in parallel. Use this parameter to avoid exhausting your request rate limit. |

Input data:

- Type: `structured`.
- The queries executed are listed in the `requests` parameter, which is a list.
- Each element in this list is a dictionary including the following fields:

| **Parameter** | **Required** | **Default value** | **Description** |
| ---------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `command` | Yes |                           | Command name. |
| `parameters` | Yes |                           | Dictionary with the command parameters. |
| `input` | No |                           | Input for the query (for the commands with the `structured` input type, for example, `set`). |

Output data:

- Type: `structured`.
- The output produces a list of the same length as at the input.
- Each list item describes the result of a single query execution. This is a dictionary of the following format:

| **Parameter** | **Required** | **Description** |
| -------- | ------------- | ------------------------------------------------------------ |
| `error` | No | Error that arose during the query execution (if any). |
| `output` | No | Output for the query (for the successful commands with the `structured` input type, for example, `get`). |

Example:

```bash
PARAMETERS {
    "requests" = [
      {
        "command" = "set";
        "parameters" = {"path" = "//tmp/a"};
        "input" = "value_a";
      };
      {
        "command" = "get";
        "parameters" = {"path" = "//tmp/b"};
      };
      {
        "command" = "get";
        "parameters" = {"path" = "//nonexisting"};
      };
    ];
}
OUTPUT [
    { };
    { output = "value_b"; };
    { error = {...} };
]
```

### get_supported_features

Command properties: **Non-mutating**, **Light**.

Semantics:

- Returns a dictionary with the `features` field describing elementary data types, compression codecs, erasure codecs, and other features supported by the cluster.

Parameters:

- No

Input data:

- Type: `null`.

Output data:

- Type: `structured`. The `features` key in the response includes a dictionary with the following fields.

| **Key** | **Value type** | **Description** |
| ------------------ | ---------------- | ------------------------------------------------------------ |
| primitive\_types | list of rows | [Primitive types](../../user-guide/storage/data-types.md#schema_primitive). |
| erasure\_codecs | list of rows | [Erasure codecs](../../user-guide/storage/replication.md). |
| compression\_codecs | list of rows | [Compression codecs](../../user-guide/storage/compression.md). |


Example:

```bash
PARAMETERS { }
OUTPUT {
    "features" = {
      "primitive_types" = ["int8"; "int16"; ... ];
      "erasure_codecs" = ["none", "isa_lrc_12_2_2"; "isa_reed_solomon_6_3"; ... ];
      "compression_codecs" = ["none"; "snappy"; "brotli_1"; ... ];
    };
}
```

### generate_timestamp

Command properties: **Non-mutating**, **Light**.

Semantics:

- Generates a monotonous timestamp.

Parameters:

- No

Input data:

- Type: `null`.

Output data:

- Type: `structured`. The `timestamp` key in the response stores a `uint64` value.

Example:

```bash
PARAMETERS { }
OUTPUT {
    "timestamp" = 1723665447133469427u;
}
```

### get_version

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get version information about the YTsaurus cluster.

Parameters:

- No

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Version information including build version and features.

### get_current_user

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get the current authenticated user.

Parameters:

- No

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Current user name.

### discover_proxies

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Non-mutating**, **Light**.

Semantics:

- Discover available proxy addresses in the cluster.

Parameters:

- No

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: List of proxy addresses.

### get_bundle_config

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get configuration for a tablet cell bundle.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `bundle_name` | Yes |                           | Name of the bundle. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Bundle configuration.

### set_bundle_config

Command properties: **Mutating**, **Light**.

Semantics:

- Set configuration for a tablet cell bundle.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `bundle_name` | Yes |                           | Name of the bundle. |
| `bundle_config` | No |                           | New bundle configuration. |

Input data:

- Type: `structured`.
- Value: Bundle configuration.

Output data:

- Type: `null`.

### externalize

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Externalize a Cypress node to another cell.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `path` | Yes |                           | Path to the node. |
| `cell_tag` | Yes |                           | Target cell tag. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

### internalize

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Internalize an externalized Cypress node.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `path` | Yes |                           | Path to the node. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

### check_permission_by_acl

Command properties: **Non-mutating**, **Light**.

Semantics:

- Check if a user has specific permission according to an ACL.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `user` | Yes |                           | Username to check. |
| `permission` | Yes |                           | Permission to check (e.g., `read`, `write`). |
| `acl` | Yes |                           | Access control list to check against. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Permission check result.

### transfer_account_resources

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Transfer resource quota between accounts.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `source_account` | Yes |                           | Source account name. |
| `destination_account` | Yes |                           | Destination account name. |
| `resource_delta` | Yes |                           | Resources to transfer. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

### transfer_pool_resources

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Transfer resource quota between scheduler pools.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `source_pool` | Yes |                           | Source pool name. |
| `destination_pool` | Yes |                           | Destination pool name. |
| `pool_tree` | Yes |                           | Pool tree name. |
| `resource_delta` | Yes |                           | Resources to transfer. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

### transfer_bundle_resources

{% note info %}

This command is only available from API version 4 onward.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Transfer resource quota between tablet cell bundles.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `source_bundle` | Yes |                           | Source bundle name. |
| `destination_bundle` | Yes |                           | Destination bundle name. |
| `resource_delta` | Yes |                           | Resources to transfer. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.

### join_reduce

Command properties: **Mutating**, **Light**.

Semantics:

- Start a join-reduce operation to join multiple tables and reduce the result.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `spec` | Yes |                           | Operation specification. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Operation ID.

### remote_copy

Command properties: **Mutating**, **Light**.

Semantics:

- Copy data from another YTsaurus cluster.

Parameters:

| **Parameter** | **Required** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `spec` | Yes |                           | Operation specification including source cluster and paths. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Operation ID.
