Sometimes, you may need to know who's using a particular table, for example, to evaluate the consequences of it being moved or deleted. This might be difficult if the table is accessed by [links](../../user-guide/storage/objects.md#links).

For this, there is a special log that records events involving Cypress nodes that might be of interest to users.

Logs are written by master servers. Due to technical reasons, several servers produce the same sequence of entries, so duplication is to be expected. On top of that, some actions (for example, writing to a table) are represented as a sequence of several different events on different master servers (from different shards). This is covered in more detailed below.

Each log entry (table row) contains one command applied to a certain Cypress node.

Only the commands applied to the following types of nodes are recorded:

* Table
* File
* Document
* Journal

It must be noted that directories _are not included_ in this list.

The following commands are recorded:

* Basic (CRUD):
   * Create
   * Get
   * GetKey
   * Exists
   * List
   * Set
   * Remove
* Creating a symbolic link:
   * Link
* Locking:
   * Lock
   * Unlock
* Copying and moving:
   * Copy
   * Move
   * BeginCopy, EndCopy
* Reading and writing data:
   * GetBasicAttributes
   * Fetch
   * BeginUpload
   * EndUpload
* Changing the state of a dynamic table:
   * PrepareMount, CommitMount, AbortMount
   * PrepareUnmount, CommitUnmount, AbortUnmount
   * PrepareRemount, CommitRemount, AbortRemount
   * PrepareFreeze, CommitFreeze, AbortFreeze
   * PrepareUnfreeze, CommitUnfreeze, AbortUnfreeze
   * PrepareReshard, CommitReshard, AbortReshard
* Other:
   * CheckPermission

Below you can find some comments on the command semantics.

Each log entry has certain fields (table columns), which are represented in Table 1.

<small>Table 1 — Description of log fields</small>

| Field | Description |
|----------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| `instant` | Event time in the format `YYYY-MM-DD hh:mm:ss,sss` |
| `cluster` | Short cluster name |
| `method` | Command (see the above list) |
| `path` (see Note) | Path passed to the command as an argument |
| `original_path` (see Note) | Path passed to the command as an argument |
| `destination_path` (see Note) | Destination path for the Copy, Move, and Link commands (not applicable to other commands) |
| `original_destination_path` (see Note) | Destination path for the Copy, Move, and Link commands (not applicable to other commands) |
| `user` | User who gave the command |
| `type` | Type of node created with the Create command (not applicable to other commands) |
| `transaction_info` | Information about the transaction where the command was executed (not applicable to cases where the command was executed outside of a transaction) |

{% note info "Note" %}

The difference between `original_path` and `path` (as well as between `original_destination_path` and `destination_path`) is as follows:

* If a link (symbolic link) was specified as a path, `original_path` will contain the path to the link, whereas `path` will contain the actual path to the node.
* If this path leads to a shard, its log will feature the actual path under `original_path`, while the path relative to the root of the shard will be written in `path`.

Overall, this means that if you grep a relative path to a symbolic link, the command will always return entries containing the actual path to the node, while grepping the actual path finds accesses, including via symbolic links. **The key takeaway here is that you should search both by `path` and by `original_path`.**

{% endnote %}

The structure of the `transaction_info` field is shown in Table 2.

<small>Table 2 — Structure of the `transaction_info` field</small>

| Field | Description |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `transaction_id` | Transaction ID |
| `transaction_title` | Human-readable description of the transaction (specified by the client upon transaction start; the field is missing if the description wasn't specified) |
| `operation_id` | ID of the operation associated with the transaction |
| `operation_title` | Human-readable description of the operation associated with the transaction |
| `parent` | For a nested transaction, the description of its parent (for top-level transactions, the field is missing) |

Please note that the `parent` field is structured the same way as `transaction_info`. Thus, `transaction_info` contains the full recursive description of the ancestry of the transaction where the command was run.
