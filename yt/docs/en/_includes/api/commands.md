# Commands

This section lists all the commands available in the YT system API and includes a full description of their options.

## Structure { #structure }

To put it simply, each command has the following structure:

- Command name (string).
- Description of the input and output formats (a [YSON string](../../user-guide/storage/data-types.md#yson) with attributes).
- Parameters of executed action (a YSON structure).
- Input and output data streams.

Each command also defines the type of its input and output data. Below all the possible options for the input and output data:

- No data (`null`).
- The data is a `binary` stream. For example, [file](../../user-guide/storage/files.md) contents.
- The data is structured (`structured`). For example, regular Cypress [nodes](../../user-guide/storage/formats.md#structured_data).
- The data is a `binary` stream. For example, [table](../../user-guide/storage/static-tables.md) contents.

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

The mutating light commands change the system status. That's why you need to hint the system that such a query has already been made. To do this, before executing the command, generate the `mutation_id` for the command (TODO example). This is a standard GUID that consists of four 32-bit numbers in the HEX format separated by a dash (`-`).

Specify the `mutation_id` generated both in the parameters of both the original query and in the retries.  In addition, you need to add, to the original query, the `retry` parameter with the value of `false`. To the retried queries, add the `retry` parameter with the value of `true`. Some light mutating commands do not support retries (an example is `concatenate`). If you need to retry such commands, you can use transactions.

{% note info "Note" %}

`mutation_id` are usually kept for 5-10 minutes.

{% endnote %}


#### Heavy commands { #hard }

You can't retry heavy commands However, you can implement a retry mechanism by using transactions.

## Transactions { #transactions }

[Transactions](../../user-guide/storage/transactions.md) are an integral [Cypress](../../user-guide/storage/cypress.md) property. Many commands that interact with Cypress this way or another, are transactional. Each command or group of commands has a separate indication of whether it is transactional or not. If the command is transactional, it supports the following parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| -------------------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `transaction_id` | No | *null-transaction-id* | ID of the current transaction (it will become a parent transaction for the transaction created by the command). |
| `ping_ancestor_transactions` | No | `false` | Whether to ping, when running the operation, all the parent transactions (to extend their TTL). |
| `timeout` | No | `15000` | Transaction TTL since the last extension (in ms). |
| `deadline` | No | `missing` | Deadline for the transaction execution time (in UTC). |
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

- `ping_tx` pings the transaction on the server (including all the parent transactions if `ping_ancestors`is specified). This way you can extend the TTL for the transaction.
- If the transaction started at the time `s` with the timeout (TTL) of `t`, then the transaction will complete at `s+t` by default.
- If you ping the transaction at the time `r` (`s < r < s + t`), it will be extended until `r + t`.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
INPUT   "my_integer" = 4 ;
INPUT   "my_double" = 2.718281828 ;
INPUT   "map" = { "a" = 1 ; "b" = 2 } ;
INPUT   "list" = [ 1, 2, 3 ]
INPUT }
```

### multiset_attributes

Command properties: **Mutating**, **Light**.

Semantics:

- Set several attributes at the specified path (if the attributes exist already, they are overwritten).

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `path` | Yes |                           | Path to attributes of a node in Cypress. The path *must* exist. |

Input data:

- Type: `structured`.
- Value: A map-node including new attribute values.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "path" = "//tmp/node/@" }
INPUT {
INPUT   "attribute1" = 4 ;
INPUT   "attribute2" = "attribute2 value";
INPUT }
```

### get { #get }

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get the content of the Cypress node.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `path` | Yes |                           | Path to the node in Cypress. The path *must* exist. |
| `attributes` | No | `[]` | A list of attributes that need to be obtained along with each node. |
| `max_size` | No | `missing` | Sets a limit on the number of children that will be issued in the case of virtual composite nodes (for regular map nodes, this option doesn't make sense). |
| `ignore_opaque` | No | `false` | Ignore the `opaque` attribute when executing a query (never use this option without explicit advice from YT developers). |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: Node content.

Example:

```bash
PARAMETERS { "path" = "//tmp/node" }
OUTPUT {
OUTPUT   "my_integer" = 4 ;
OUTPUT   "my_double" = 2.718281828 ;
OUTPUT   "map" = { "a" = 1 ; "b" = 2 } ;
OUTPUT   "list" = [ 1, 2, 3 ]
OUTPUT }
```

### list { #list }

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get a list of descendants for the Cypress node.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
OUTPUT   "home" ;
OUTPUT   "sys" ;
OUTPUT   "statbox" ;
OUTPUT   "tmp"
OUTPUT ]
```

### lock

Command properties: **Mutating**, **Light**.

Semantics:

- Set a lock on the Cypress node.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
- Value: ID of the created lock.

Example:

```bash
PARAMETERS { "path" = "//tmp/node" }
OUTPUT "0-1-c8-ab45cdfa"
```

### unlock

{% note warning "Attention!" %}

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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------------------    | -------------     | ------------------------- | ------------------------------------------------------------                                                                                                                                                                                                      |
| `source_path` | Yes |                           | Path to the source node in Cypress. The path *must* exist. |
| `destination_path` | Yes |                           | Path where the copy will be created. The path *must not* exist. |
| `recursive` | No | `false` | Whether to create the skipped levels (map nodes) at the destination path. |
| ignore_existing | No | `false` | If the node already exists at `destination_path`, do nothing  (you can't use this parameter together with `force = %true`). |
| `lock_existing` | No | `false` | Set an [exclusive lock](../../user-guide/storage/transactions.md#locks) on the node at `destination_path`, even if it already exists. This parameter is only used together with `ignore_existing`. If the command fails to set a lock, it returns an error. |
| `force` | No | `false` | Allows you to specify an existing node to be replaced, as the destination path. |
| `preserve_account` | No | `false` | Whether to keep the accounts of the source nodes or use the account at the destination path. |
| `preserve_expiration_time` | No | `false` | Whether the copy the `expiration_time` attribute or leave it empty. |
| `preserve_expiration_timeout` | No | `false` | Whether the copy the `expiration_timeout` attribute or leave it empty. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: ID of the created node.

Example:

```bash
PARAMETERS {
PARAMETERS   "source_path" = "//tmp/from" ;
PARAMETER    "destination_path" = "//tmp/to" ;
PARAMETERS }
OUTPUT "0-4-191-6c07cd58"
```

### move { #move}

Command properties: **Mutating**, **Light**.

Semantics:

- Move the Cypress node to a new path.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------------------    | -------------     | ------------------------- | ------------------------------------------------------------                                             |
| `source_path` | Yes |                           | Path to the source node in Cypress. The path *must* exist. |
| `destination_path` | Yes |                           | A new path in Cypress. The path *must not* exist. |
| `recursive` | No | `false` | Whether to create the skipped levels (map nodes) at the destination path. |
| `force` | No | `false` | Allows you to specify an existing node to be replaced, as the destination path. |
| `preserve_account` | No | `false` | Whether to keep the accounts of the source nodes or use the account at the destination path. |
| `preserve_expiration_timeout` | No | `false` | Whether the copy the `expiration_timeout` attribute or leave it empty. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS {
PARAMETERS   "source_path" = "//tmp/from" ;
PARAMETER    "destination_path" = "//tmp/to" ;
PARAMETERS }
```

### link { #link }

Command properties: **Mutating**, **Light**.

Semantics:

- Create a symbolic link to the object at the new path.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
PARAMETERS   "target_path" = "//tmp/from" ;
PARAMETER    "link_path" = "//tmp/to" ;
PARAMETERS }
OUTPUT "0-4-191-6c07cd58"
```

### exists { #exists }

Command properties: **Non-mutating**, **Light**.

Semantics:

- Checks what are the node exists.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
PARAMETERS   "path" = "//tmp/my_table/@_format" ;
PARAMETERS }
```

### concatenate { #concatenate }

Semantics:

- Merge the set of files or tables (in the order in which their paths are listed).

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
PARAMETERS   "source_paths" = ["//tmp/file1"; "//tmp/file2"];
PARAMETERS   "destination_path" = "//tmp/file";
PARAMETERS }
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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
PARAMETERS   "group" = "admins";
PARAMETERS   "member": "devs";
PARAMETERS   }
```

### remove_member

Command properties: **Mutating**, **Light**.

Semantics:

- Add to a group a user or another group.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
PARAMETERS   "group" = "admins";
PARAMETERS   "member": "devs";
PARAMETERS }
```

### check_permission

Command properties: **Non-mutating**, **Light**.

Semantics:

- Check if a user has a certain permission to access a certain Cypress node.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
PARAMETERS { "path" = "//sys/accounts/statbox";
PARAMETERS   "permission": "use";
PARAMETERS   "user": "vasya";
PARAMETERS }
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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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

## Working with file cache { #file_cache }

To learn more about the file cache, see the [File cache](../../user-guide/storage/file-cache.md) section.

### put_file_to_cache

Command properties: **Mutating**, **Light**.

Semantics:

- Add the file to the cache.
- Checks that, at the provided path, there's a file with the MD5 hash given in the command options.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
PARAMETERS   "path" = "//tmp/file";
PARAMETERS   "md5" = "a3dcb4d229de6fde0db5686dee47145d";
PARAMETERS   "cache_path" = "//tmp/yt_wrapper/file_storage/new_cache";
PARAMETERS }
OUTPUT "//tmp/yt_wrapper/file_storage/new_cache/5d/a3dcb4d229de6fde0db5686dee47145d"
```

### get_file_from_cache { #get_file_from_cache }

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get a path to the cached file.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
PARAMETERS   "md5" = "a3dcb4d229de6fde0db5686dee47145d";
PARAMETERS   "cache_path" = "//tmp/yt_wrapper/file_storage/new_cache";
PARAMETERS }
OUTPUT "//tmp/yt_wrapper/file_storage/new_cache/5d/a3dcb4d229de6fde0db5686dee47145d"
```

## Working with tables { #tables }

To learn more about static tables, see the [Static tables](../../user-guide/storage/static-tables.md) section.

To learn more about dynamic tables, see the [Dynamic tables](../../user-guide/dynamic-tables/overview.md) section.

### write_table

Command properties: **Mutating**, **Heavy**.

Scope: [Static tables](../../user-guide/storage/static-tables.md).

Synonyms (supported, but not recommended).

- write.

Semantics:

- Add new entries to a static table.
- The table must exist.
- If the table `path` includes the `append=%true` attribute, the entries are appended to the table; otherwise, the table is overwritten.
- If the table `path` includes the `sorted_by` attribute, the system checks that the data is sorted by the specified set of keys and the resulting table is labeled as sorted. You can write sorted data (with a `sorted_by` attribute specified) only if any new key is greater than or equal to any old key.
- You can write unsorted data (that is, without the `sorted_by` attribute) to any table, but the  table's sort flag will be removed.
- The command can be nested in a [transaction](#transactions).

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
PARAMETERS   "path" = "//tmp/node" ;
PARAMETERS   "table_writer" = { "codec_id" = "gzip" };
PARAMETERS }
INPUT { "id" = 1; "value" = 1.125; };
INPUT { "id" = 2; "value" = 2.000; };
INPUT { "id" = 3; "value" = 3.850; };
```

### read_table

Command properties: **Non-mutating**, **Heavy**.

Scope: [Static](../../user-guide/storage/static-tables.md) and [dynamic](../../user-guide/dynamic-tables/overview.md) tables.

Synonyms (supported, but not recommended).

- read.

Semantics:

- Retrieve entries from a Cypress table.
- The command can be nested in a [transaction](#transactions).

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `path` | Yes |                           | Path to a table in Cypress. The path *must* exist. |
| `table_reader` | No | From the driver configuration | [Table's read options](../../user-guide/storage/io-configuration.md#table_reader). |
| `control_attributes` | No | From the driver configuration | [Configuration of the control read attributes.](../../user-guide/storage/io-configuration.md#control_attributes) |
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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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

### select_rows

Command properties: **Non-mutating**, **Heavy**.

Scope: [Dynamic tables](../../user-guide/dynamic-tables/overview.md).

Semantics:

- Execute an SQL-like query for a dynamic table in accordance with the supported features.
- The transaction can be executed against a data snapshot with a specified timestamp.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `path` | Yes |                           | Path to the dynamic table. |
| `update` | No | `false` | If set to `false`, the columns missing in the input data will be written with the value of `Null` (overwriting the current value in the table). If set to `true`, such columns will preserve their previous value in the table. |
| `aggregate` | No | `false` | If set to `false`, the [aggregating columns](../../user-guide/dynamic-tables/sorted-dynamic-tables.md#aggr_columns) will be overwritten by the new value. If set to `true`, such columns will apply the delta from the source data. |
| `atomicity` | No | `full` | Supported values: `none` and `full`, [learn more](../../user-guide/dynamic-tables/sorted-dynamic-tables.md) |

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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------------- |
| `path` | Yes |                           | Path to the dynamic table. |
| `locks` | Yes |                           | List of the involved `lock groups` from the table schema. |
| `lock_type` | No | `shared strong` | Lock types: `shared weak`, `shared strong`, `exclusive` |

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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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

| **parameter** | **Mandatory** | **Default value** | **Description** |
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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| -------------------- | ---------- | ------------------------- | ---------------------------------------- |
| `path` | Yes |                           | Path to the dynamic table. |
| `first_tablet_index` | No | `0` | Index of the first unmounted tablet. |
| `last_tablet_index` | No | `tablet_count - 1` | Index of the last unmounted tablet. |
| `force` | No | `false` | Forcibly unmount the tablets. Using this flag poses a risk of data loss, so you need YT admin rights to use it. |

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
- In the case of such remounting, the table remains accessible for reading and writing.
- The settings are taken from the table attributes and eventually reach the node where the tablets are processed.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
   - When passing `tablet_count`, the system will select pivot keys based on the data available in the table as evenly as possible. If the table isn't large enough, you might get less tablets then requested as a result. At default settings, your resulting tablets can't be smaller than about 200 MB each. For smaller slicing, use the option`enable_slicing`.
   - If the first key column of the table has an integer type, then along with `tablet_count`, you can use `uniform=True`. In this case, uniform values from the range of the appropriate type will be selected as pivot keys. `0, 2^64/n, 2^64\*2/n, ...` for an unsigned 64-bit type and `-2^63, -2^63 + 2^64/n, -2^63 + 2^64\*2/n, ...` for a signed 64-bit type.
- For an ordered table, `table_count` specifies the number of new tablets that the sharded tablets are split into. In this case, if the resulting tablets are higher in numbers than the old ones, new empty tablets are created. If the resulting tablets are smaller in numbers, the corresponding number of source trailing tablets are merged into a single tablet in their natural order.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `path` | Yes |                           | Table path. |
| `first_tablet_index` | No | `0` | Index of the first resharded tablet. |
| `last_tablet_index` | No | `tablet_count - 1` | Index of the last resharded tablet. |
| `pivot_keys` | No |                           | Pivot keys for the new tablets (for a sorted table). |
| `tablet_count` | No |                           | Number of new tablets. |
| `uniform` | No | `false` | Uniformly reshard tablets based on an integer key column. |
| `enable_slicing` | No | `false` | Use sampling to increase granularity (for a more precise splitting into tablets) when pivot keys are selected automatically. This might help if you have many entries on one key and few entries on another key. |
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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
OUTPUT     "11-22-33-44";
OUTPUT ]
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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
PARAMETERS   "paths" = ["//tmp/table1{a,b,c}"; "//tmp/table2{a,b,c}"];
PARAMETERS   "transaction_id" = "1234-abcd-abcd-7890"
PARAMETERS }
OUTPUT {
OUTPUT    "column_data_weights" = {
OUTPUT        "a" = 8124;
OUTPUT        "b" = 124241241;
OUTPUT        "c" = 3121414;
OUTPUT    };
OUTPUT    "legacy_chunks_data_weight" = 100242;
OUTPUT    "timestamp_total_weight" = 50056;
OUTPUT }
```

## Running operations

For more information about running data processing operations, see [Data processing](../../user-guide/data-processing/operations/overview.md).

All the operations are run asynchronously, the specified commands only launch them. To find out whether the operation is complete or not, request the [operation status](../../user-guide/data-processing/operations/overview.md#status) using the `get_operation` command.
All the commands used to work with operations are also transactional. It means that everything you do with tables in an operation will be executed within the specified transaction when you run the operation.  The node responsible for the operation (`//sys/operations/<OP_ID>`) is updated by the [scheduler](../../user-guide/data-processing/scheduler/scheduler-and-pools.md)outside of any transactions.

### start_operation

Command properties: **Mutating**, **Light**.

Semantics:

- Run the operation of the specified type.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| -------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `operation_type` | Yes |                           | Operation type (can be one of these: `map`, `reduce`, `map_reduce`, `join_reduce`, `remote-copy`, `erase`, `sort`, `merge`, `vanilla`). |
| `spec` | Yes |                           | Operation specification. For more information, see [Setting up operations](../../user-guide/data-processing/operations/operations-options.md). |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: ID of the running operation.

Example:

```bash
PARAMETERS {  "operation_type" = "map_reduce";
PARAMETERS    "spec" = {
PARAMETERS      "input_table_paths" = [ "//tmp/table_in", "//tmp/table_in" ] ;
PARAMETERS      "output_table_path" = "//tmp/table_out"
PARAMETERS    }
PARAMETERS }
OUTPUT "37878b-ba919c15-cdc97f3a-8a983ece"
```

### merge

Command properties: **Mutating**, **Light**.

Semantics:

- Start merging the source tables.
- [Detailed description](../../user-guide/data-processing/operations/merge.md) of all specification parameters.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
PARAMETERS    "spec" = {
PARAMETERS      "input_table_paths" = [ "//tmp/table_in", "//tmp/table_in" ] ;
PARAMETERS      "output_table_path" = "//tmp/table_out"
PARAMETERS    }
PARAMETERS }
OUTPUT "37878b-ba919c15-cdc97f3a-8a983ece"
```

### erase

Command properties: **Mutating**, **Light**.

Semantics:

- Start erasing data from the source table.
- [Detailed description](../../user-guide/data-processing/operations/erase.md) of all specification parameters.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
PARAMETERS    "spec" = {
PARAMETERS      "table_path" = "//tmp/table[#0:#500]" ;
PARAMETERS    }
PARAMETERS }
OUTPUT "3f9e62-ce8d2965-6350842b-3e4628d2"
```

### map

Command properties: **Mutating**, **Light**.

Semantics:

- Run the map operation on the source tables, writing the output to the output tables.
- [Detailed description](../../user-guide/data-processing/operations/map.md) of all specification parameters.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
PARAMETERS    "spec" = {
PARAMETERS      "mapper" = {
PARAMETERS          "command" = "cat"
PARAMETERS      } ;
PARAMETERS      "input_table_paths"  = [ "//tmp/table_in", "//tmp/table_in" ] ;
PARAMETERS      "output_table_paths" = [ "//tmp/table_out" ]
PARAMETERS    }
PARAMETERS }
OUTPUT "33ab3f-bf1df917-b35fe9ed-c70a4bf4"
```

### reduce

Command properties: **Mutating**, **Light**.

Semantics:

- Run the reduce operation on the source tables, writing the output to the output tables.
- [Detailed description](../../user-guide/data-processing/operations/reduce.md) of all specification parameters.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
PARAMETERS    "spec" = {
PARAMETERS      "reducer" = {
PARAMETERS          "command" = "cat" ;
PARAMETERS      } ;
PARAMETERS      "input_table_paths"  = [ "//tmp/table_in", "//tmp/table_in" ] ;
PARAMETERS      "output_table_paths" = [ "//tmp/table_out" ] ;
PARAMETERS      "reduce_by"          = [ "my_key" ] ;
PARAMETERS }
OUTPUT "33ab3f-bf1df917-b35fe9ed-c70a4bf4"
```

### join_reduce

{% note warning "Attention!" %}

In the upcoming versions, we'll discontinue support for Join Reduce. Instead of the Join Reduce operation, you need to use the [Reduce](../../user-guide/data-processing/operations/reduce.md) operation with the `enable_key_guarantee` option.

{% endnote %}

Command properties: **Mutating**, **Light**.

Semantics:

- Run the `join_reduce` operation on the source tables, writing the output to the output tables.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------------------ | ------------- | ------------------------- | -------------------------------------------------- |
| `spec` | Yes |                           | Operation specification (see the relevant fields below). |
| *spec[input_table_paths]* | Yes |                           | List of input tables. |
| *spec[output_table_paths]* | Yes |                           | List of output tables. |
| *spec[join_by]* | Yes |                           | Columns that `join` runs against. |
| *spec[reducer]\[command]* | Yes |                           | Command that runs the reducer. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: ID of the running operation.

Example:

```bash
PARAMETERS {
PARAMETERS    "spec" = {
PARAMETERS      "reducer" = {
PARAMETERS          "command" = "cat" ;
PARAMETERS      } ;
PARAMETERS      "input_table_paths"  = [ "//tmp/table_in", "//tmp/table_in" ] ;
PARAMETERS      "output_table_paths" = [ "//tmp/table_out" ] ;
PARAMETERS      "join_by"          = [ "my_key" ] ;
PARAMETERS }
OUTPUT "33ab3f-bf1df917-b35fe9ed-c70a4bf4"
```

### sort

Command properties: **Mutating**, **Light**.

Semantics:

- Start sorting the source tables.
- [Detailed description](../../user-guide/data-processing/operations/sort.md) of all the specification parameters.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
PARAMETERS    "spec" = {
PARAMETERS      "input_table_paths" = [ "//tmp/table_in", "//tmp/table_in" ] ;
PARAMETERS      "output_table_path" = "//tmp/table_out" ;
PARAMETERS      "sort_by"           = [ "mykey" ];
PARAMETERS    }
PARAMETERS }
OUTPUT "37878b-ba919c15-cdc97f3a-8a983ece"
```

### map_reduce

Command properties: **Mutating**, **Light**.

Semantics:

- Run Map-Reduce on the source tables.
- [Detailed description](../../user-guide/data-processing/operations/mapreduce.md) of all the specification parameters.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
PARAMETERS    "spec" = {
PARAMETERS      "mapper" = {
PARAMETERS          "command" = "cat"
PARAMETERS      } ;
PARAMETERS      "reducer" = {
PARAMETERS          "command" = "cat" ;
PARAMETERS      } ;
PARAMETERS      "input_table_paths" = [ "//tmp/table_in", "//tmp/table_in" ] ;
PARAMETERS      "output_table_path" = "//tmp/table_out" ;
PARAMETERS      "reduce_by" = [ "my_key" ] ;
PARAMETERS    }
PARAMETERS }
OUTPUT "37878b-ba919c15-cdc97f3a-8a983ece"
```

### remote-copy

Command properties: **Mutating**, **Light**.

Semantics:

- Copy the source tables from the specified cluster.
- Detailed description of all [specification parameters](../../user-guide/data-processing/operations/remote-copy.md).

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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
PARAMETERS    "spec" = {
PARAMETERS      "input_table_paths" = [ "//tmp/table_in", "//tmp/table_in" ] ;
PARAMETERS      "output_table_path" = "//tmp/table_out" ;
PARAMETERS      "cluster_name"           = <cluster-name>;
PARAMETERS    }
PARAMETERS }
OUTPUT "37878b-ba919c15-cdc97f3a-8a983ece"
```

## Working with operations

{% note info "Note" %}

All the commands used to work with the operations are non-transactional.

{% endnote %}

### list_operations { #list_operations }

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get a list of operations that match the filters.

Parameters:

| **Parameter** | **Type** | **Mandatory** | **Default value** | **Description** |
| ---------------- | -------------- | ----------------- | ------------------------- | ------------------------------------------------------------ |
| `attributes` | `list<string>` | No | `Null` | List of operation attributes that need to be returned in the response. |
| `from_time` | `ISO 8601 string` | No | `Null` | Bottom limit for the time interval for operation selection (by the time when the operation began). |
| `to_time` | `ISO 8601 string` | No | `Null` | Top limit for the time interval for operation selection (by the time when the operation began). |
| `cursor_time` | `ISO 8601 string` | No | `Null` | Time beginning from which a list of operation should be returned. |
| `cursor_direction` | `{past, future}` | No | `past` | Time direction used to list the operations. |
| `user` | `string` | No | `Null` | Username to filter for. |
| `state` | `string` | No | `Null` | Operation state used to filter data. |
| `type` | `string` | No | `Null` | Type of filtering operation. |
| `filter` | `string` | No | `Null` | Substring that the operation's `filter_factors` should include. |
| `pool` | `string` | No | `Null` | Pool used for filtering. |
| `with_failed_jobs` | `bool` | No | `Null` | Return only the operations that have jobs with the `failed` status. |
| `access` | map | No | `Null` | Dictionary with the mandatory fields `subject` (a string) and `permissions` (a list of strings) that set a filter by access rights. If specified, only the operations for which a `subject` has every right in the `permissions` list, are returned. |
| `include_archive` | `bool` | No | `false` | Whether to request operations from the archive. |
| `include_counters` | `bool` | No | `true` | Whether to return statistics for the requested operations. |
| `limit` | `int` | No | `100` | List of operations that need to be returned in the response. |
| `enable_ui_mode` | `bool` | No | `false` | Whether to return the response in the old UI-compatible format. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Returns a dictionary with the following fields:
   - `operations`: List with explicit descriptions of operations. Each operation described is a dictionary that includes the selected operation attributes: `id`, `type`, `state`, `authenticated_user`, `brief_progress`, `brief_spec`, `start_time`, `suspended`, `weight`. The `weight`, `brief_progress`, and `brief_spec` attributes are optional.
   - `incomplete`: Whether the list of operations is complete (that is, whether all the operations in the range `from_time` — `to_time` are listed).
   - `pool_counts`: Statistics on pools.
   - `user_counts`: Statistics on users.
   - `state_counts`: Statistics on operation states.
   - `type_counts`: Statistics on operation type.
   - `failed_jobs_count`: Count of `failed` jobs for the operations.

Example:

```bash
PARAMETERS { }
OUTPUT {
OUTPUT     "operations" = [
OUTPUT         {
OUTPUT             "id" = "7001208d-fef089b3-3fe03e8-453d99a1";
OUTPUT             "type" = "remote-copy";
OUTPUT             "state" = "initializing";
OUTPUT             "authenticated_user" = "user-name";
OUTPUT             "brief_progress" = {};
OUTPUT             "brief_spec" = {
OUTPUT                 ...
OUTPUT             };
OUTPUT             "start_time" = "2018-02-06T11:06:34.200591Z";
OUTPUT             "suspended" = %false;
OUTPUT             "weight" = 1.;
OUTPUT         };
OUTPUT     ];
OUTPUT     "incomplete" = %true;
OUTPUT     "pool_counts" = {
OUTPUT         "pool-counts-example" = 2;
OUTPUT         "user-name-1" = 2;
OUTPUT         ...
OUTPUT     };
OUTPUT     "user_counts" = {
OUTPUT         "yql" = 52;
OUTPUT         "user-name-1" = 2;
OUTPUT     };
OUTPUT     "state_counts" = {
OUTPUT         "materializing" = 10;
OUTPUT         "pending" = 763;
OUTPUT         "running" = 1848;
OUTPUT         "completed" = 6654;
OUTPUT         "aborted" = 37;
OUTPUT         "failed" = 98;
OUTPUT     };
OUTPUT     "type_counts" = {
OUTPUT         "map" = 4294;
OUTPUT         "merge" = 1337;
OUTPUT         "erase" = 97;
OUTPUT         "sort" = 1126;
OUTPUT         "reduce" = 886;
OUTPUT         "map_reduce" = 1609;
OUTPUT         "remote-copy" = 24;
OUTPUT         "join_reduce" = 37;
OUTPUT     };
OUTPUT     "failed_jobs_count" = 109;
OUTPUT }
```

### get_operation { #get_operation }

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get information about an operation.

Parameters:

| **Parameter** | **Type** | **Mandatory** | **Default value** | **Description** |
| ------------ | ------- | ----------------- | ------------------------- | ----------------------- |
| `operation_id` | `GUID` | Yes |                           | Operation ID. |
| `attributes` | `list` | No | `[]` | Operation attributes. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Returns a dictionary with the requested operation attributes.

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
| `spec` | `map` | Specification given by the user at the beginning of the operation. |
| `full_spec` | `map` | Specification where all the fields omitted by the user are populated by defaults. |
| `unrecognized_spec` | `map` | Specification fields that were entered by the user but not recognized by the scheduler. |
| `controller_agent_address` | `string` | Address of the controller agent (host:port) responsible for the operation. |
| `events` | `list<map>` | List of events (state changes) that occurred with the operation. |
| `alerts` | `map` | Alerts (in the format of a dictionary `<alert_name> : <map_with_attributes>`) set up for the operation at the moment. |
| `result` | `map` | A map with an `error` field that can include an error if the operation failed. |
| `commited` | `bool` | Whether the operation results were committed. |
| `suspended` | `bool` | Whether the operation is currently suspended. |

Example:

```bash
PARAMETERS {  "`operation_id`" = "33ab3f-bf1df917-b35fe9ed-c70a4bf4"; attributes = [ "state" ] }
OUTPUT {
OUTPUT     "state" = "running";
OUTPUT }
```

### abort_operation

Command properties: **Mutating**, **Light**.

Semantics:

- Abort the operation.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `operation_id` | Yes |                           | Operation ID. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "`operation_id`" = "33ab3f-bf1df917-b35fe9ed-c70a4bf4" }
```

### complete_operation

Command properties: **Mutating**, **Light**.

Semantics:

- Instantly complete the operation, saving the currently calculated result.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `operation_id` | Yes |                           | Operation ID. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "`operation_id`" = "33ab3f-bf1df917-b35fe9ed-c70a4bf4" }
```

### suspend_operation

Command properties: **Mutating**, **Light**.

Semantics:

- Suspend the operation.
- This command is executed instantly. After that, no more jobs are started for the operation, and the running jobs are optionally aborted.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------------ | ------------- | ------------------------- | ----------------------------------------------- |
| `operation_id` | Yes |                           | Operation ID. |
| `abort_running_jobs` | No | `false` | Whether to abort the running operation jobs. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "`operation_id`" = "33ab3f-bf1df917-b35fe9ed-c70a4bf4" }
```

### resume_operation

Command properties: **Mutating**, **Light**.

Semantics:

- Resume the suspended operation.
- This command is executed instantly.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `operation_id` | Yes |                           | Operation ID. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "`operation_id`" = "33ab3f-bf1df917-b35fe9ed-c70a4bf4" }
```

### update_operation_parameters { #update_operation_parameters }

Command properties: **Mutating**, **Light**.

Semantics:

- Update the parameters of a running operation.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| -------------------------------------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `operation_id` | Yes |                           | Operation ID. |
| `parameters` | Yes |                           | Dictionary with the operation parameters. |
| *parameters[owners]* | No |                           | (deprecated, will be removed) List of new owners of the operation. |
| *parameters[acl]* | No |                           | New ACL of the operation (it overlaps the base ACL). |
| *parameters[pool]* | No |                           | Name of the pool to which the operation has to be switched over in all its trees. |
| *parameters[weight]* | No |                           | New weight of the operation in all the trees. |
| *parameters[scheduling_options_per_pool_tree]* | No |                           | Dictionary `{tree name: scheduler settings for this tree}`. The settings are described below. To learn more about the scheduler, see [Scheduler and pools](../../user-guide/data-processing/scheduler/scheduler-and-pools.md). |

Scheduler settings for the tree:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| --------------- | ------------- | ------------------------- | ------------------------------------------------------- |
| `weight` | No |                           | Weight of the operation in the tree. |
| `pool` | No |                           | Pool of the operation in the tree. |
| `resource_limits` | No |                           | Dictionary `{ resource name: limit}`. Resource limits in the tree. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS {"`operation_id`" = "33ab3f-bf1df917-b35fe9ed-c70a4bf4"; "parameters" = {"pool" = "username"; "scheduling_options_per_pool_tree" = {"physical" = {"weight" = 2; "resource_limits" = { "user_slots" = 1; "cpu" = 0.5; "network" = 10; "memory" = 1000000000}}}}}
```

## Working with jobs

{% note info "Note" %}

All the commands used to work with jobs are non-[transactional](#transactions).

{% endnote %}

### get_job { #get_job }

Command properties: **Non-mutating**, **Light**.

Semantics:

- Getting information about a job. The command can run both for running and completed jobs (if information about the jobs was saved).

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------ | ----------------- | ------------------------- | ----------------------- |
| `operation_id` | Yes |                           | Operation ID |
| `job_id` | Yes |                           | Job ID |

Input data:

- Type: `null`.

Output data:

- Type: `structured`. Map with the job's attributes.

| **Field** | **Type** | **Description** |
|------------------|-----------------|-----------------------------------------------------------------------------------------------------------------------------|
| `job_id` | `string` | String representation of the job ID (for example, `d840bb39-3d893e5b-3fe03e8-f009b1fb`). |
| `operation_id` | `string` | String representation of the operation ID (for example, `d840bb39-3d893e5b-3fe03e8-f009b1fb`). |
| `type` | `string` | Job type (for example, `vanilla`, `map`, `partition_reduce`). |
| `state` | `string` | Current job state (for example, `running`, `failed`, `completed`). |
| `address` | `string` | Address of the node (host:port) that ran the job. |
| `task_name` | `string` | Name of the task that the job responds to. |
| `start_time` | `ISO 8601 string` | Start time. |
| `finish_time` | `ISO 8601 string` | End time. |
| `pool` | `string` | Name of the [pool](../../user-guide/data-processing/scheduler/scheduler-and-pools.md) where the job was run. |
| `pool_tree` | `string` | Name of the [pool tree](../../user-guide/data-processing/scheduler/scheduler-and-pools.md) where the job was run. |
| `progress` | `float in [0,1]` | Evaluation of the share of work executed by the job by the current moment. |
| `stderr_size` | `integer` | Size of the saved stderr of the job (you can get stderr using the command `get_job_stderr`). |
| `error` | `map` | Dictionary with an error description (for a failed job). |
| `statistics` | `map` | Dictionary with the job's statistics |
| `brief_statistics` | `map` | Dictionary with brief statistics. |
| `input_paths` | `list<YPath>` | List of parts to tables (with row ranges) processed by the job. |
| `core_infos` | `list<map>` | List of dictionaries describing the core dumps saved by the job. |
| `events` | `list<map>` | List of dictionaries describing events (changes in state or phase) that occurred to the job. |
| `is_stale` | `bool` | Whether the information about the job is outdated (if `%true`, some fields might need update). Information about the job is considered outdated if it hasn't been updated for a long time. The information in the job archive is updated by the node running the job. The update process is asynchronous. If the node restarts for some reason (for example, as a result of an update), the information about the final job state (`completed`, `failed`, or `aborted`) could fail to be archived, and the job is always returned as `stale` as a result. Despite the `running` status, such jobs have usually been executed for a long time, and you should ignore them. |

Example:

```bash
PARAMETERS { "`operation_id`" = "e13c5406-e5dd6f5d-3fe03e8-fe05f0d3"; "`job_id`" = "f11ae559-a0375703-3fe0384-8f1"}
OUTPUT {
OUTPUT     "`operation_id`" = "e13c5406-e5dd6f5d-3fe03e8-fe05f0d3";
OUTPUT     "`job_id`" = "f11ae559-a0375703-3fe0384-8f1";
OUTPUT     "state" = "completed";
OUTPUT     "start_time" = "2018-02-06T09:37:02.858492Z";
OUTPUT     "finish_time" = "2018-02-06T09:42:19.185525Z";
OUTPUT     "address" = "<cluster-name>:9012";
OUTPUT     "statistics" = {
OUTPUT         "data" = {
OUTPUT              ...
OUTPUT         };
OUTPUT     };
OUTPUT     "events" = [
OUTPUT         ...
OUTPUT     ];
OUTPUT }
```


### list_jobs { #list_jobs }

{% note warning "Attention!" %}

This command can create a significant load on the cluster.

{% endnote %}

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get all the jobs that match the filters for a given operation.

Parameters:

| **Parameter** | **Type** | **Mandatory** | **Default value** | **Description** |
| ----------------- | ------------------------------------------------------------ | ----------------- | ------------------------- | ------------------------------------------------------------ |
| `operation_id` | `GUID` | Yes |                           | Operation ID. |
| `type (job_type)` | `EJobType` | No | `Null` | When you specify the parameter, the response will only include the jobs with the specified `job_type`. |
| `state (job_state)` | `EJobState` | No | `Null` | When you specify the parameter, the response will only include the jobs with the specified `job_state`. |
| `address` | `string` | No | `Null` | When you specify the parameter, the response will only include the jobs with the specified `address`. |
| `with_stderr` | `bool` | No | `Null` | At `Null`, all the jobs are returned. At `True`, only the jobs with a non-null `stderr` are returned. At `False`, only the jobs with a null `stderr` are returned. |
| `with_fail_context` | `bool` | No | `Null` | At `Null`, all the jobs are returned. At `True`, only the jobs for the saved `fail_context` are returned. At `False`, only the jobs that do not have `fail_context` are returned. |
| `sort_field` | `{none,type,state,start_time,finish_time,address,duration,progress,id}` | No | `none` | Sort fields. |
| `sort_order` | `{ascending,descending}` | No | `ascending` | Sorting order. |
| `limit` | `int` | No | `1000` | Limit on the number of returned jobs. |
| `offset` | `int` | No | `0` | Offset by the given number of jobs. |
| `data_source` | `EDataSource` | No | `auto` | Data source, acceptable values: `runtime`, `archive`, and `auto`. |

The `job_type`, `job_state`, `address`, and `has_stderr` parameters define the job *filter*. The response will only include the jobs that meet the filtering criteria.

The `sort_field` and`sort_order` define the order of jobs in the response. In this case, the `limit` and `offset` parameters define the slice (subset) of jobs in the response: the first `offset` jobs are skipped, and then `limit` of the remaining jobs is selected.

The `data_source` parameter regulates the source of data from which the jobs are taken:
- In the `runtime` mode, jobs are retrieved from the controller agent and Cypress and then merged.
- In the `archive` mode, jobs are retrieved from the archive and the controller agent and then merged.
- The `auto` mode automatically determines the source of jobs based on availability of operations in the controller agent.

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Structure with the fields `jobs`, `cypress_job_count`, `controller_agent_job_count`, and `archive_job_count`.
- `*_count` fields: Counter of jobs found for a given operation in the appropriate data sources, without filtering. If all three numbers are zeros, it means that there's no information about the operation's jobs. If all the three numbers aren't zeros, but the `jobs` response is empty, it means that all the jobs were filtered out. If you get `null` instead of a number, the corresponding data source wasn't polled.

- `jobs` field: List of structures that describe each job. Each job can have the following fields:
   - `id` (`guid`), `type` (`string`), `state` (`string`), `address` (`string`): Required fields.
   - `start_time` (`instant`), `finish_time` (`instant`), `progress` (`double`), `stderr_size` (`int`): Optional fields.
   - `error`, `brief_statistics`, `input_paths`, `core_infos`: Optional fields.

Example:

```bash
PARAMETERS { "`operation_id`" = "4505e8eb-28fa88e2-3fe03e8-c6fcd8fa"; }
OUTPUT {
OUTPUT     "jobs" = [
OUTPUT         {
OUTPUT             "id" = "55aff293-7ef14284-3fe0384-3e07";
OUTPUT             "type" = "map";
OUTPUT             "state" = "failed";
OUTPUT             "address" = "fb-sas2-0511-sas-yt-prod-portovm-14323.gencfg-c.host-name.net:9012";
OUTPUT             "start_time" = "2018-05-05T00:41:27.433832Z";
OUTPUT             "finish_time" = "2018-05-05T00:49:04.288196Z";
OUTPUT             "fail_context_size" = 973230u;
OUTPUT             "error" = {
OUTPUT                 "code" = 1205;
OUTPUT                 "message" = "User job failed";
OUTPUT                 ...
OUTPUT             };
OUTPUT             ...
OUTPUT         };
OUTPUT         ...
OUTPUT         {
OUTPUT             "id" = "69ae20a7-887b25ab-3fe0384-3cff";
OUTPUT             "type" = "map";
OUTPUT             "state" = "running";
OUTPUT             "address" = "fb-sas2-0101-sas-yt-prod-portovm-14323.gencfg-c.host-name.net:9012";
OUTPUT             "start_time" = "2018-05-07T13:04:03.339873Z";
OUTPUT             "progress" = 0.;
OUTPUT             "brief_statistics" = <
OUTPUT                 "timestamp" = "2018-05-07T13:04:08.431740Z";
OUTPUT             > {
OUTPUT                 "processed_input_compressed_data_size" = 0;
OUTPUT                 "processed_input_data_weight" = 0;
OUTPUT                 "processed_output_uncompressed_data_size" = 0;
OUTPUT                 "processed_output_compressed_data_size" = 0;
OUTPUT                 "processed_input_uncompressed_data_size" = 0;
OUTPUT                 "processed_input_row_count" = 0;
OUTPUT             };
OUTPUT         };
OUTPUT     ];
OUTPUT     "cypress_job_count" = 200;
OUTPUT     "scheduler_job_count" = 208;
OUTPUT     "archive_job_count" = #;
OUTPUT     "type_counts" = {
OUTPUT         "map" = 408;
OUTPUT     };
OUTPUT     "state_counts" = {
OUTPUT         "running" = 208;
OUTPUT         "failed" = 200;
OUTPUT     };
OUTPUT }
```

### signal_job

Command properties: **Mutating**, **Light**.

Semantics:

- Send a signal to the job.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `job_id` | Yes |                           | Job ID. |
| `signal_name` | Yes |                           | Type of the signal to be sent to the job: `SIGHUP`, `SIGINT`, `SIGALRM`, `SIGKILL`, `SIGTERM`, `SIGUSR1`, `SIGUSR2`. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "`job_id`" = "1225d-1f2fb8c4-f1075d39-5fb7cdff"; "signal_name" = "SIGUSR1" }
```

### abandon_job

Command properties: **Mutating**, **Light**.

Semantics:

- Abort the job and consider that its input data was processed successfully.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | -------------------- |
| `job_id` | Yes |                           | Job ID. |

Input data:

- Type: `null`

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "`job_id`" = "1225d-1f2fb8c4-f1075d39-5fb7cdff" }
```

### abort_job

Command properties: **Mutating**, **Light**.

Semantics:

- Abort the job, and let it later be restarted by the scheduler (as any aborted operation job).

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ----------------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `job_id` | Yes |                           | Job ID. |
| `interrupt_timeout` | No |                           | A timeout for successful completion of the job after it stops receiving input data. |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "`job_id`" = "1225d-1f2fb8c4-f1075d39-5fb7cdff" }
```

{% if audience == internal %}

### strace_job


{% note warning "Attention!" %}

The command is obsolete and will be deleted. Use `job shell` to replace it.

{% endnote %}

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get job strace.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | -------------------- |
| `job_id` | Yes |                           | Job ID. |

Input data:

- Type: `null`.

Output data:

- Type: `structured`.
- Value: traces of running processes.

Example:

```bash
PARAMETERS { "`job_id`" = "1225d-1f2fb8c4-f1075d39-5fb7cdff" }
```

{% endif %}

### dump_job_context

Command properties: **Non-mutating**, **Light**.

Semantics:

- Get the `input context` of the job.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `job_id` | Yes |                           | Job ID. |
| `path` | Yes |                           | A path where to save the set of input parameters received by the job. The path's components should exist (except for the file itself). |

Input data:

- Type: `null`.

Output data:

- Type: `null`.

Example:

```bash
PARAMETERS { "`job_id`" = "1225d-1f2fb8c4-f1075d39-5fb7cdff"; "path" = "//tmp/input_context" }
```

### get_job_input { #get_job_input }

Command properties: **Non-mutating**, **Heavy**.

Semantics:

- Get the full input for the job. The command is used both with running and failed jobs with a saved specification and all the input data available.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | -------------------- |
| `job_id` | Yes |                           | Job ID. |

Input data:

- Type: `null`.

Output data:

- Type: `binary`.
- Value: Job input.

Example:

```bash
PARAMETERS { "`job_id`" = "1225d-1f2fb8c4-f1075d39-5fb7cdff"}
```

### get_job_fail_context { #get_job_fail_context }

Command properties: **Non-mutating**, **Heavy**.

Semantics:

- Get the `fail context` of the job. The command is used with failed jobs.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `operation_id` | Yes |                           | Operation ID. |
| `job_id` | Yes |                           | Job ID. |

Input data:

- Type: `null`.

Output data:

- Type: `binary`.
- Value: `Fail context` of the job.

Example:

```bash
PARAMETERS { "`operation_id`" = "33ab3f-bf1df917-b35fe9ed-c70a4bf4"; "`job_id`" = "1225d-1f2fb8c4-f1075d39-5fb7cdff"}
```

### get_job_stderr { #job_stderr }

Command properties: **Non-mutating**, **Heavy**.

Semantics:

- Get the stderr of the job. The command is used with failed jobs.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ----------------------- |
| `operation_id` | Yes |                           | Operation ID. |
| `job_id` | Yes |                           | Job ID. |

Input data:

- Type: `null`.

Output data:

- Type: `binary`.
- Value: The stderr of the job.

Example:

```bash
PARAMETERS { "`operation_id`" = "33ab3f-bf1df917-b35fe9ed-c70a4bf4"; "`job_id`" = "1225d-1f2fb8c4-f1075d39-5fb7cdff }
```

## Other

### parse_ypath

Command properties: **Non-mutating**, **Light**.

Semantics:

- Perform parsing of the passed [YPath](../../user-guide/storage/ypath.md) by putting all IDs of the complex YPath into attributes.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
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

### execute_batch

Command properties:  **Mutating if the set includes mutating commands**, **Light**.

Semantics:

- Use a single query to execute the set of commands passed in the parameters.
- The command can (and will be) executed in parallel. It means that if a set includes both writing to and reading from the node, the reading result can either be the older value or the updated one.
- The set can only include light commands.
- The set can only include commands with the input type of `null` or `structured`.
- The set can only include commands with the output type of `null` or `structured`.

Parameters:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ------------ | ------------- | ------------------------- | ------------------------------------------------------------ |
| `requests` | Yes |                           | Description of executed queries. |
| `concurrency` | No | `50` | A numeric parameter that sets the maximum number of commands running on the cluster in parallel. Use this parameter to avoid exhausting your request rate limit. |

Input data:

- Type: `structured`.
- The queries executed are listed in the `requests` parameter, which is a list.
- Each element in this list is a dictionary including the following fields:

| **Parameter** | **Mandatory** | **Default value** | **Description** |
| ---------- | ------------- | ------------------------- | ------------------------------------------------------------ |
| `command` | Yes |                           | Command name. |
| `parameters` | Yes |                           | Dictionary with the command parameters. |
| `input` | No |                           | Input for the query (for the commands with the `structured` input type, for example, `set`). |

Output data:

- Type: `structured`.
- The output produces a list of the same length as at the input.
- Each list item describes the result of a single query execution. This is a dictionary of the following format:

| **Parameter** | **Mandatory** | **Description** |
| -------- | ------------- | ------------------------------------------------------------ |
| `error` | No | Error that arose during the query execution (if any). |
| `output` | No | Output for the query (for the successful commands with the `structured` input type, for example, `get`). |

Example:

```bash
PARAMETERS {
PARAMETERS    "requests" = [
PARAMETERS      {
PARAMETERS        "command" = "set";
PARAMETERS        "parameters" = {"path" = "//tmp/a"};
PARAMETERS        "input" = "value_a";
PARAMETERS      };
PARAMETERS      {
PARAMETERS        "command" = "get";
PARAMETERS        "parameters" = {"path" = "//tmp/b"};
PARAMETERS      };
PARAMETERS      {
PARAMETERS        "command" = "get";
PARAMETERS        "parameters" = {"path" = "//nonexisting"};
PARAMETERS      };
PARAMETERS    ];
PARAMETERS }
OUTPUT [
OUTPUT   { };
OUTPUT   { output = "value_b"; };
OUTPUT   { error = {...} };
OUTPUT ]
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
OUTPUT   "features" = {
OUTPUT     "primitive_types" = ["int8"; "int16"; ... ];
OUTPUT     "erasure_codecs" = ["lrc_12_2_2"; "reed_solomon_6_3"; ... ];
OUTPUT     "compression_codecs" = ["none"; "snappy"; "brotli_1"; ... ];
OUTPUT   };
OUTPUT }
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
OUTPUT   "timestamp" = 1723665447133469427u;
OUTPUT }
```
