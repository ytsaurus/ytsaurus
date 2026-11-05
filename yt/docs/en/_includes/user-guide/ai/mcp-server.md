---
title: "Working with the MCP server | {{product-name}}"
description: "How to use the MCP server {{product-name}}: prompt examples for the AI assistant and a complete reference of available tools"
---

# Working with the MCP server {{product-name}}

The MCP server {{product-name}} is a mediator between the AI assistant and the {{product-name}} clusters. It provides the neural network with a set of tools for reading data and metadata. The assistant accesses the cluster directly and works with its current state, rather than generating answers from memory.

## How it works {#how-it-works}

You state the task in natural language, and the assistant selects and combines the necessary tools on its own. For example, to figure out why you can’t write data to a table, the assistant will sequentially check the path existence, the user’s permissions, and the account’s quota, and then explain the reason.

With the MCP server, the neural network can:

- Read data and metadata — the table schema, sample rows, directory contents.
- Check access permissions — what permissions the user has for the object.
- Monitor quotas and resources — free space on HDD and SSD, limits, and the current load of pools.
- Search for objects — tables, files, and other nodes by name and attributes.

You can specify the response format directly in the prompt.

## Installation {#installation}

The server installation and configuration are described in the section [Installing the MCP server](../../../admin-guide/install-mcp.md).

## Prompt examples {#examples}

Analyzing data and table structures

:   - Check the structure of the table `//home/team/users` on the `my_cluster` cluster and write a Python script that reads the `user_id` column from it.
    - Output a sample of data from the table `//home/team/logs/today`.
    - There are tables in the `//home/team/data` folder. Determine their schema.

Managing permissions and searching for objects

:   - Why does my script fail with an access error when writing to `//home/team/output`? Check the permissions for the user `ivanov`.
    - Find all tables with the `backup_` prefix in the `//home/team` directory and tell me who owns them.

Working with quotas and resources

:   - How much SSD space is left for the `my_account` account?
    - Check the limits for the `compute_pool` pool on the `my_cluster` cluster — maybe you’ve hit the CPU quota?

{% note tip %}

If the assistant says it can’t complete the task, guide it by explicitly specifying a method from the reference below. For example: “Use the `check_is_paths_exist` tool to make sure the folder exists.”

{% endnote %}

## Tool reference {#methods-reference}

This section lists all tools available to the AI assistant. You can reference their names in your prompts. Below is a summary by group. Click a tool name to go to its description.

### Navigation and object search {#nav-tools}

#|
|| **Tool** | **Purpose** ||
|| [list_dir](#list-dir) | Returns the contents of a node or directory ||
|| [find](#find) | Searches for objects in the cluster subtree ||
|| [check_is_paths_exist](#check-is-paths-exist) | Checks whether paths exist ||
|#

### Tables: data and schemas {#table-tools}

#|
|| **Tool** | **Purpose** ||
|| [common_client_read_table](#common-client-read-table) | Reads table rows ||
|| [common_client_sample_static_table](#common-client-sample-static-table) | Returns the first row of a table for a quick preview ||
|| [common_client_get_table_schema](#common-client-get-table-schema) | Returns the table schema ||
|| [common_client_infer_table_schema](#common-client-infer-table-schema) | Derives the schema from the table contents ||
|#

{% note info %}

The tools `common_client_read_table`, `common_client_sample_static_table`, and `common_client_infer_table_schema` read table rows via the cluster’s HTTP proxy. For them to work, the host running the MCP server must have network access to the HTTP proxy. To configure external access to the proxy, see the section [Configure external cluster access](../../../admin-guide/cluster-access-proxy/index.md). Metadata-reading tools, such as `common_client_get_table_schema` and `list_dir`, work via the Master server and don’t require this access.

{% endnote %}

### Access permissions {#access-tools}

#|
|| **Tool** | **Purpose** ||
|| [check_permission](#check-permission) | Checks the user’s permissions for a path ||
|| [common_client_whoami](#common-client-whoami) | Returns information about the current user ||
|#

### Quotas and resources {#quota-tools}

#|
|| **Tool** | **Purpose** ||
|| [get_attributes_account](#get-attributes-account) | Returns the account attributes ||
|| [get_attributes_account_limits_disk](#get-attributes-account-limits-disk) | Returns the disk quota and used space ||
|| [get_attributes_bundle](#get-attributes-bundle) | Returns the bundle attributes: resource limits and quotas ||
|| [get_attributes_pool](#get-attributes-pool) | Returns the pool attributes and current load ||
|| [get_account_property](#get-account-property) | Returns an account property, for example, the tree of child accounts ||
|#

### Infrastructure {#infra-tools}

#|
|| **Tool** | **Purpose** ||
|| [get_proxy](#get-proxy) | Returns the list of cluster proxy servers ||
|#

### list_dir {#list-dir}

Returns the contents of a node or directory in the {{product-name}} cluster.

The result includes metadata for each node: type (`file`, `table`, or `map_node`), account, creation time, and row count.

Parameters:

#|
|| **Parameter** | **Type** | **Required** | **Description** ||
|| `directory` | string | Yes | Path to the node or directory. Must start with `//` ||
|| `cluster` | string | Yes | Name of the {{product-name}} cluster ||
|#

Example request:

```json
{
  "directory": "//home/team",
  "cluster": "my_cluster"
}
```

### find {#find}

Searches for objects in the cluster subtree.

Parameters:

#|
|| **Parameter** | **Type** | **Required** | **Description** ||
|| `root_path` | string | Yes | Root path to start the search from. Must start with `//` ||
|| `cluster` | string | Yes | Name of the {{product-name}} cluster ||
|| `name` | string | No | Shell-style name pattern ||
|| `type` | array[string] | No | Object types: `table`, `file`, `document`, `account`, `user`, `list_node`, `map_node` ||
|| `attributes` | array[string] | No | Attributes to include in the result, for example, `account`, `owner` ||
|| `attributes_to_match` | object | No | Filter by attribute values, for example, by `owner` or `account` ||
|#

Example request:

```json
{
  "root_path": "//home/team",
  "cluster": "my_cluster",
  "name": "log_*",
  "type": ["table"],
  "attributes": ["owner"],
  "attributes_to_match": {
    "owner": "ivanov"
  }
}
```

### check_is_paths_exist {#check-is-paths-exist}

Checks whether paths exist in the {{product-name}} cluster.

The list can contain from 1 to 500 paths. Each path must start with `//` and must not end with `/`.

Parameters:

#|
|| **Parameter** | **Type** | **Required** | **Description** ||
|| `cluster` | string | Yes | Name of the {{product-name}} cluster ||
|| `paths` | array[string] | Yes | List of up to 500 paths. Each path must start with `//` and must not end with `/` ||
|#

Example request:

```json
{
  "paths": ["//home/team/data", "//tmp/temp_table"],
  "cluster": "my_cluster"
}
```

### common_client_read_table {#common-client-read-table}

Reads table rows.

{% note warning "Warning" %}

The data can be large and exceed the model’s context window. For a quick preview of the structure, use [`common_client_sample_static_table`](#common-client-sample-static-table).

{% endnote %}

Parameters:

#|
|| **Parameter** | **Type** | **Required** | **Description** ||
|| `table` | string | Yes | Path to the table ||
|| `method` | string | Yes | Must be `read_table` ||
|| `cluster` | string | Yes | Name of the {{product-name}} cluster ||
|#

Example request:

```json
{
  "table": "//home/team/users",
  "method": "read_table",
  "cluster": "my_cluster"
}
```

### common_client_sample_static_table {#common-client-sample-static-table}

Returns the first row of a static table. This is useful for a quick look at the data without loading the whole table.

Parameters:

#|
|| **Parameter** | **Type** | **Required** | **Description** ||
|| `table` | string | Yes | Path to the table with a row selector, for example, `//home/team/users[#0:#1]` for the first row. To get several rows, specify a range `[#0:#N]` ||
|| `method` | string | Yes | Must be `read_table` ||
|| `cluster` | string | Yes | Name of the {{product-name}} cluster ||
|#

Example request:

```json
{
  "table": "//home/team/users[#0:#1]",
  "method": "read_table",
  "cluster": "my_cluster"
}
```

### common_client_get_table_schema {#common-client-get-table-schema}

Returns the table schema. The schema is stored in the `value` field, and the `attributes` field contains the `strict` and `unique_keys` flags.

{% note info %}

If an empty schema is returned, use the [`common_client_infer_table_schema`](#common-client-infer-table-schema) method, which derives the schema from the table contents.

{% endnote %}

Parameters:

#|
|| **Parameter** | **Type** | **Required** | **Description** ||
|| `table_path` | string | Yes | Path to the table ||
|| `method` | string | Yes | Must be `get_table_schema` ||
|| `cluster` | string | Yes | Name of the {{product-name}} cluster ||
|#

Example request:

```json
{
  "table_path": "//home/team/users",
  "method": "get_table_schema",
  "cluster": "my_cluster"
}
```

### common_client_infer_table_schema {#common-client-infer-table-schema}

Determines the table schema from its contents. Use this method if [`common_client_get_table_schema`](#common-client-get-table-schema) returned an empty schema.

Parameters:

#|
|| **Parameter** | **Type** | **Required** | **Description** ||
|| `table` | string | Yes | Path to the table ||
|| `method` | string | Yes | Must be `infer_table_schema` ||
|| `cluster` | string | Yes | Name of the {{product-name}} cluster ||
|#

Example request:

```json
{
  "table": "//home/team/users",
  "method": "infer_table_schema",
  "cluster": "my_cluster"
}
```

### check_permission {#check-permission}

Use this method to check a user's permission to access a specified path. The response includes the `action` field with the check result: `allow` means access is granted, `deny` means access is denied.

Parameters:

#|
|| **Parameter** | **Type** | **Required** | **Description** ||
|| `path` | string | Yes | Path to the object ||
|| `cluster` | string | Yes | Name of the {{product-name}} cluster ||
|| `permission` | string | Yes | Permission: `read`, `write`, `use`, `create`, `administer` ||
|| `user_login` | string | Yes | User login ||
|#

Example request:

```json
{
  "path": "//home/team",
  "cluster": "my_cluster",
  "permission": "read",
  "user_login": "user_login"
}
```

Example response:

```json
{
  "action": "allow"
}
```

### common_client_whoami {#common-client-whoami}

Use this method to get information about the current user on the cluster.

Parameters:

#|
|| **Parameter** | **Type** | **Required** | **Description** ||
|| `method` | string | Yes | Must be `get_current_user` ||
|| `cluster` | string | Yes | Name of the {{product-name}} cluster ||
|#

Example request:

```json
{
  "method": "get_current_user",
  "cluster": "my_cluster"
}
```

### get_attributes_account {#get-attributes-account}

Use this method to get the account attributes on the cluster.

Parameters:

#|
|| **Parameter** | **Type** | **Required** | **Description** ||
|| `account` | string | Yes | Account name. No spaces allowed ||
|| `cluster` | string | Yes | Name of the {{product-name}} cluster ||
|| `attributes` | array[string] | Yes | Attributes, for example `inherit_acl`, `effective_acl`, `abc`, `resource_limits`, `resource_usage` ||
|#

Example request:

```json
{
  "account": "my_account",
  "cluster": "my_cluster",
  "attributes": ["resource_usage", "effective_acl"]
}
```

### get_attributes_account_limits_disk {#get-attributes-account-limits-disk}

Use this method to get the account's disk quota and the amount of used space on HDD and SSD.

Values are returned in bytes:

- `resource_limits.disk_space_per_medium.default` — account quota on HDD;
- `resource_limits.disk_space_per_medium.ssd_blobs` — account quota on SSD;
- `resource_usage.disk_space_per_medium.default` — used space on HDD;
- `resource_usage.disk_space_per_medium.ssd_blobs` — used space on SSD.

Other resources, such as the number of nodes, tablets, and static memory, are not included.

Parameters:

#|
|| **Parameter** | **Type** | **Required** | **Description** ||
|| `account` | string | Yes | Account name ||
|| `cluster` | string | Yes | Name of the {{product-name}} cluster ||
|| `attributes` | array[string] | Yes | You must specify both `resource_usage` and `resource_limits` ||
|#

Example request:

```json
{
  "account": "my_account",
  "cluster": "my_cluster",
  "attributes": ["resource_usage", "resource_limits"]
}
```

### get_attributes_bundle {#get-attributes-bundle}

Use this method to get the bundle attributes on the cluster: resource limits with details on the number of tablets and static memory, as well as CPU and memory quotas.

Parameters:

#|
|| **Parameter** | **Type** | **Required** | **Description** ||
|| `cluster` | string | Yes | Name of the {{product-name}} cluster ||
|| `attributes` | array[string] | Yes | List of requested attributes, for example `inherit_acl`, `effective_acl`, `resource_limits`, `resource_quota` ||
|| `bundle` | string | No | Bundle name. No spaces allowed ||
|#

Example request:

```json
{
  "bundle": "my_bundle",
  "cluster": "my_cluster",
  "attributes": ["resource_limits", "resource_quota"]
}
```

### get_attributes_pool {#get-attributes-pool}

Use this method to get the pool attributes on the cluster.

{% note info %}

The pool is searched in the pool tree specified in the `pool_tree` parameter. The default value `physical` doesn't work for all clusters — the tree name depends on the configuration. If the pool isn't found, ask the cluster administrator for the correct tree name.

{% endnote %}

Parameters:

#|
|| **Parameter** | **Type** | **Required** | **Description** ||
|| `cluster` | string | Yes | Name of the {{product-name}} cluster ||
|| `attributes` | array[string] | Yes | Attributes, for example `strong_guarantee_resources`, `integral_guaranties`, `max_operation_count`, `max_running_operation_count`, `running_operation_count`, `scheduling_status`, `starvation_status`, `resource_usage`. The `resource_usage` attribute shows the current pool load: CPU, GPU, memory, slots ||
|| `pool` | string | No | Pool name. Unique within the tree. No spaces allowed ||
|| `pool_tree` | string | No | Name of the pool tree. Default is `physical` ||
|#

Example request:

```json
{
  "pool": "pool_name",
  "pool_tree": "pool_tree",
  "cluster": "my_cluster",
  "attributes": ["max_operation_count", "effective_acl"]
}
```

### get_account_property {#get-account-property}

Use this method to get an account property.

Parameters:

#|
|| **Parameter** | **Type** | **Required** | **Description** ||
|| `account` | string | Yes | Account name ||
|| `cluster` | string | Yes | Name of the {{product-name}} cluster ||
|| `property` | string | Yes | Account property. For example, `childrens` returns the tree of child accounts ||
|#

Example request:

```json
{
  "account": "my_account",
  "cluster": "my_cluster",
  "property": "childrens"
}
```

### get_proxy {#get-proxy}

Use this method to get the list of cluster proxy servers with the specified attributes.

Parameters:

#|
|| **Parameter** | **Type** | **Required** | **Description** ||
|| `cluster` | string | Yes | Name of the {{product-name}} cluster ||
|| `attributes` | array[string] | Yes | Proxy server attributes: `proxy_type`, `type`, `role`, `version` ||
|| `proxies` | array[string] | No | List of proxy servers in the `fqdn:port` format. If not specified, the method applies to all cluster proxies ||
|| `proxy_type` | string | No | Proxy type: `http` or `rpc`. If not specified, all types are returned ||
|#

Example request:

```json
{
  "cluster": "my_cluster",
  "attributes": ["role", "version"],
  "proxy_type": "http"
}
```


<style>
  .dc-mini-toc__section_child {
    display: none;
}

@media screen and (max-width: 768px) {
    .dc-doc-page__content-mini-toc ul li ul {
        display: none;
    }
}
</style>
