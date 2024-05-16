# {{product-name}} CLI Commands

## Cypress commands



### concatenate

concatenates cypress nodes. This command applicable only to files and tables

```bash
usage: yt concatenate [-h] [--params PARAMS] --src SOURCE_PATHS --dst DESTINATION_PATH
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--src`    (Required) Source paths Accepted multiple times.

> `--dst`    (Required) Destination paths

### copy

copies Cypress node

```bash
usage: yt copy [-h] [--params PARAMS] [--source-path SOURCE_PATH] [--destination-path DESTINATION_PATH] [--preserve-account | --no-preserve-account] [--preserve-owner | --no-preserve-owner]
               [--preserve-creation-time] [--preserve-modification-time] [--preserve-expiration-time] [--preserve-expiration-timeout] [--preserve-acl | --no-preserve-acl] [-r] [-i] [-l] [-f]
               [--no-pessimistic-quota-check]
               [source_path] [destination_path]
```

#### Positional Arguments

> `source_path`    source address, path must exist. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `destination_path`    destination address, path must not exist. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--source-path`    source address, path must exist. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--destination-path`    destination address, path must not exist. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--preserve-account`

> `--no-preserve-account`

> `--preserve-owner`

> `--no-preserve-owner`

> `--preserve-creation-time`    preserve creation time of node

> `--preserve-modification-time`    preserve modification time of node

> `--preserve-expiration-time`    preserve expiration time of node

> `--preserve-expiration-timeout`    preserve expiration timeout of node

> `--preserve-acl`

> `--no-preserve-acl`

> `-r, --recursive`

> `-i, --ignore-existing`

> `-l, --lock-existing`

> `-f, --force`

> `--no-pessimistic-quota-check`

### create

creates Cypress node

```bash
usage: yt create [-h] [--params PARAMS] [--type TYPE] [--path PATH] [-r] [-i] [-l] [-f] [--attributes ATTRIBUTES] [type] [path]
```

#### Positional Arguments

> `type`    one of table, file, document, account, user, list_node, map_node, string_node, int64_node, uint64_node, double_node, …

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--type`    one of table, file, document, account, user, list_node, map_node, string_node, int64_node, uint64_node, double_node, …

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `-r, --recursive`

> `-i, --ignore-existing`

> `-l, --lock-existing`

> `-f, --force`

> `--attributes`    structured attributes in yson format

### create-account

. creates account

```bash
usage: yt create-account [-h] [--params PARAMS] [--name NAME] [--parent-name PARENT_NAME] [--resource-limits RESOURCE_LIMITS] [-i] [--allow-children-limit-overcommit] [--attributes ATTRIBUTES] [name]
```

#### Positional Arguments

> `name`

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--name`

> `--parent-name`

> `--resource-limits`    structured resource-limits in yson format

> `-i, --ignore-existing`

> `--allow-children-limit-overcommit`

> `--attributes`    structured attributes in yson format

### create-pool

. creates scheduler pool

```bash
usage: yt create-pool [-h] [--params PARAMS] [--name NAME] [--pool-tree POOL_TREE] [--parent-name PARENT_NAME] [--weight WEIGHT] [--mode MODE] [--fifo-sort-parameters FIFO_SORT_PARAMETERS]
                      [--max-operation-count MAX_OPERATION_COUNT] [--max-running-operation-count MAX_RUNNING_OPERATION_COUNT] [--forbid-immediate-operations] [--resource-limits RESOURCE_LIMITS]
                      [--min-share-resources MIN_SHARE_RESOURCES] [--create-ephemeral-subpools] [--ephemeral-subpool-config EPHEMERAL_SUBPOOL_CONFIG] [-i] [--attributes ATTRIBUTES]
                      [name] [pool_tree]
```

#### Positional Arguments

> `name`

> `pool_tree`

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--name`

> `--pool-tree`

> `--parent-name`

> `--weight`

> `--mode`    fifo or fair_share

> `--fifo-sort-parameters`    structured fifo-sort-parameters in yson format

> `--max-operation-count`

> `--max-running-operation-count`

> `--forbid-immediate-operations`

> `--resource-limits`    structured resource-limits in yson format

> `--min-share-resources`    structured min-share-resources in yson format

> `--create-ephemeral-subpools`

> `--ephemeral-subpool-config`    structured ephemeral-subpool-config in yson format

> `-i, --ignore-existing`

> `--attributes`    structured attributes in yson format

### exists

checks if Cypress node exists

```bash
usage: yt exists [-h] [--params PARAMS] [--path PATH] [--suppress-transaction-coordinator-sync] [--read-from READ_FROM] [--cache-sticky-group-size CACHE_STICKY_GROUP_SIZE] [path]
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--suppress-transaction-coordinator-sync`    suppress transaction coordinator sync

> `--read-from`    Can be set to «cache» to enable reads from system cache

> `--cache-sticky-group-size`    Size of sticky group size for read_from=»cache» mode

### externalize

externalize cypress node

```bash
usage: yt externalize [-h] [--params PARAMS] [--path PATH] --cell-tag CELL_TAG [path]
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--cell-tag`    (Required)

### find

searches for some nodes in Cypress subtree

```bash
usage: yt find [-h] [--params PARAMS] [--path PATH] [--name NAME] [--type TYPE] [--account ACCOUNT] [--owner OWNER] [--follow-links] [--attribute-filter ATTRIBUTE_FILTER] [--depth DEPTH] [-l]
               [--recursive-resource-usage] [--time-type {access_time,modification_time,creation_time}] [--read-from READ_FROM] [--cache-sticky-group-size CACHE_STICKY_GROUP_SIZE]
               [path]
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--name, -name`    pattern of node name, use shell-style wildcards:

> ```
> *
> ```

> , ?, [seq], [!seq]

> `--type`    one of table, file, document, account, user, list_node, map_node, string_node, int64_node, uint64_node, double_node, …

> `--account`

> `--owner`

> `--follow-links`    follow links

> `--attribute-filter`    yson map fragment with filtering attributes, e.g. k1=v1;k2=v2

> `--depth`    recursion depth (infinite by default)

> `-l, --long-format`    print some extra information about nodes

> `--recursive-resource-usage`    use recursive resource usage for in long format mode

> `--time-type`    Possible choices: access_time, modification_time, creation_time

> type of time to use in long-format, default: `«modification_time»`

> `--read-from`    Can be set to «cache» to enable reads from system cache

> `--cache-sticky-group-size`    Size of sticky group size for read_from=»cache» mode

### get

gets Cypress node content (attribute tree)

```bash
usage: yt get [-h] [--params PARAMS] [--path PATH] [--max-size MAX_SIZE] [--format FORMAT] [--attribute ATTRIBUTES] [--suppress-transaction-coordinator-sync] [--read-from READ_FROM]
              [--cache-sticky-group-size CACHE_STICKY_GROUP_SIZE]
              [path]
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--max-size`    maximum size of entries returned by get; if actual directory size exceeds that value only subset of entries will be listed (it’s not specified which subset); default value is enough to list any nonsystem directory.

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md), default: `«<format=pretty>yson»`

> `--attribute`    desired node attributes in the response Accepted multiple times.

> `--suppress-transaction-coordinator-sync`    suppress transaction coordinator sync

> `--read-from`    Can be set to «cache» to enable reads from system cache

> `--cache-sticky-group-size`    Size of sticky group size for read_from=»cache» mode

### internalize

internalize cypress node

```bash
usage: yt internalize [-h] [--params PARAMS] [--path PATH] --cell-tag CELL_TAG [path]
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--cell-tag`    (Required)

### link

makes link to Cypress node

```bash
usage: yt link [-h] [--params PARAMS] [--target-path TARGET_PATH] [--link-path LINK_PATH] [-r] [-i] [-l] [-f] [--attributes ATTRIBUTES] [target_path] [link_path]
```

#### Positional Arguments

> `target_path`    address of original node to link, path must exist. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `link_path`    address of resulting link, path must not exist. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--target-path`    address of original node to link, path must exist. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--link-path`    address of resulting link, path must not exist. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `-r, --recursive`    create parent nodes recursively

> `-i, --ignore-existing`

> `-l, --lock-existing`

> `-f, --force`    force create link even if destination already exists (supported only on cluster with 19+ version)

> `--attributes`    structured attributes in yson format

### list

lists directory (map_node) content. Node type must be «map_node»

```bash
usage: yt list [-h] [--params PARAMS] [--path PATH] [-l] [--format FORMAT] [--attribute ATTRIBUTES] [--max-size MAX_SIZE] [--recursive-resource-usage] [--suppress-transaction-coordinator-sync]
               [--read-from READ_FROM] [--cache-sticky-group-size CACHE_STICKY_GROUP_SIZE] [--absolute]
               [path]
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `-l, --long-format`    print some extra information about nodes

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md)

> `--attribute`    node attributes to add into response Accepted multiple times.

> `--max-size`    maximum size of entries returned by list; if actual directory size exceeds that value only subset of entries will be listed (it’s not specified which subset); default value is enough to list any nonsystem directory.

> `--recursive-resource-usage`    use recursive resource usage for in long format mode

> `--suppress-transaction-coordinator-sync`    suppress transaction coordinator sync

> `--read-from`    Can be set to «cache» to enable reads from system cache

> `--cache-sticky-group-size`    Size of sticky group size for read_from=»cache» mode

> `--absolute`    print absolute paths

### move

moves (renames) Cypress node

```bash
usage: yt move [-h] [--params PARAMS] [--source-path SOURCE_PATH] [--destination-path DESTINATION_PATH] [--preserve-account | --no-preserve-account] [--preserve-owner | --no-preserve-owner]
               [--preserve-creation-time] [--preserve-modification-time] [--preserve-expiration-time] [--preserve-expiration-timeout] [-r] [-f] [--no-pessimistic-quota-check]
               [source_path] [destination_path]
```

#### Positional Arguments

> `source_path`    old node address, path must exist. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `destination_path`    new node address, path must not exist. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--source-path`    old node address, path must exist. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--destination-path`    new node address, path must not exist. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--preserve-account`

> `--no-preserve-account`

> `--preserve-owner`

> `--no-preserve-owner`

> `--preserve-creation-time`    preserve creation time of node

> `--preserve-modification-time`    preserve modification time of node

> `--preserve-expiration-time`    preserve expiration time of node

> `--preserve-expiration-timeout`    preserve expiration timeout of node

> `-r, --recursive`

> `-f, --force`

> `--no-pessimistic-quota-check`

### remove

removes Cypress node

```bash
usage: yt remove [-h] [--params PARAMS] [--path PATH] [-r] [-f] [path]
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `-r, --recursive`

> `-f, --force`

### remove-attribute

removes attribute at given path

```bash
usage: yt remove-attribute [-h] [--params PARAMS] [--path PATH] [-r] [path] name
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `name`

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `-r, --recursive`

### set

sets new value to Cypress node

```bash
usage: yt set [-h] [--params PARAMS] [--path PATH] [--format FORMAT] [-r] [-f] [--suppress-transaction-coordinator-sync] [--value VALUE] [path] [value]
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `value`    new node attribute value, in yson format. You can specify in bash pipe: «cat file_with_value | yt set //tmp/my_node»

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md), default: `«yson»`

> `-r, --recursive`

> `-f, --force`

> `--suppress-transaction-coordinator-sync`    suppress transaction coordinator sync

> `--value`    new node attribute value, in yson format. You can specify in bash pipe: «cat file_with_value | yt set //tmp/my_node»

### set-attribute

sets attribute at given path

```bash
usage: yt set-attribute [-h] [--params PARAMS] [--path PATH] [-r] [path] name value
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `name`

> `value`    structured value in yson format

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `-r, --recursive`

## File commands



### download

downloads file from path in Cypress to local machine

```bash
usage: yt download [-h] [--params PARAMS] [--path PATH] [--file-reader FILE_READER] [--offset OFFSET] [--length LENGTH] [path]
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--file-reader`    structured file-reader in yson format

> `--offset`    offset in input file in bytes, 0 by default

> `--length`    length in bytes of desired part of input file, all file without offset by default

### read-file

downloads file from path in Cypress to local machine

```bash
usage: yt read-file [-h] [--params PARAMS] [--path PATH] [--file-reader FILE_READER] [--offset OFFSET] [--length LENGTH] [path]
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--file-reader`    structured file-reader in yson format

> `--offset`    offset in input file in bytes, 0 by default

> `--length`    length in bytes of desired part of input file, all file without offset by default

### upload

uploads file to destination path from stream on local machine

```bash
usage: yt upload [-h] [--params PARAMS] [--destination DESTINATION] [--file-writer FILE_WRITER] [--compressed] [--executable] [--compute-md5] [--no-compression] [destination]
```

#### Positional Arguments

> `destination`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--destination`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--file-writer`    structured file-writer in yson format

> `--compressed`    expect stream to contain compressed file data. Warning! This option disables retries. Data is passed directly to proxy without recompression.

> `--executable`    do file executable

> `--compute-md5`    compute md5 of file content

> `--no-compression`    disable compression

### write-file

uploads file to destination path from stream on local machine

```bash
usage: yt write-file [-h] [--params PARAMS] [--destination DESTINATION] [--file-writer FILE_WRITER] [--compressed] [--executable] [--compute-md5] [--no-compression] [destination]
```

#### Positional Arguments

> `destination`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--destination`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--file-writer`    structured file-writer in yson format

> `--compressed`    expect stream to contain compressed file data. Warning! This option disables retries. Data is passed directly to proxy without recompression.

> `--executable`    do file executable

> `--compute-md5`    compute md5 of file content

> `--no-compression`    disable compression

## Table commands



### alter-table

performs schema and other table meta information modifications

```bash
usage: yt alter-table [-h] [--params PARAMS] [--path PATH] [--schema [SCHEMA]] [--dynamic | --static] [--upstream-replica-id UPSTREAM_REPLICA_ID] [path]
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--schema`    new schema value, in yson format.

> `--dynamic`

> `--static`

> `--upstream-replica-id`    upstream replica id

### create-temp-table

creates temporary table by given path with given prefix and return name

```bash
usage: yt create-temp-table [-h] [--params PARAMS] [--path PATH] [--name-prefix NAME_PREFIX] [--expiration-timeout EXPIRATION_TIMEOUT] [--attributes ATTRIBUTES]
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    path where temporary table will be created. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--name-prefix`    prefix of table name

> `--expiration-timeout`    expiration timeout in ms

> `--attributes`    structured attributes in yson format

### dirtable

Upload/download to file system commands

```bash
usage: yt dirtable [-h] dirtable_command ...
```

#### Positional Arguments

> `dirtable_command`    Possible choices: upload, download, list-files, append-single-file

#### Sub-commands

##### upload

Upload directory to YT

```bash
yt dirtable upload [-h] --directory DIRECTORY [--part-size PART_SIZE] [--recursive] [--no-recursive] --yt-table YT_TABLE [--process-count PROCESS_COUNT] [--force] [--prepare-for-sky-share]
```

###### Named Arguments

> `--directory`

> `--part-size`    Default: `4194304`

> `--recursive`

> `--no-recursive`

> `--yt-table`

> `--process-count`    Default: `4`

> `--force`

> `--prepare-for-sky-share`

##### download

Download directory from YT

```bash
yt dirtable download [-h] --directory DIRECTORY --yt-table YT_TABLE [--process-count PROCESS_COUNT] [--exact-filenames EXACT_FILENAMES] [--filter-by-regexp FILTER_BY_REGEXP]
                     [--exclude-by-regexp EXCLUDE_BY_REGEXP]
```

###### Named Arguments

> `--directory`

> `--yt-table`

> `--process-count`    Default: `4`

> `--exact-filenames`    Files to extract (separated by comma)

> `--filter-by-regexp`    Files with name matching that regexp will be extracted

> `--exclude-by-regexp`    Files with name matching that regexp will not be extracted

##### list-files

List files from YT

```bash
yt dirtable list-files [-h] --yt-table YT_TABLE
```

###### Named Arguments

> `--yt-table`

##### append-single-file

Append single file to table

```bash
yt dirtable append-single-file [-h] --yt-table YT_TABLE --yt-name YT_NAME --fs-path FS_PATH [--process-count PROCESS_COUNT]
```

###### Named Arguments

> `--yt-table`

> `--yt-name`

> `--fs-path`

> `--process-count`    Default: `4`

### get-table-columnar-statistics

gets columnar statistics of tables listed in paths

```bash
usage: yt get-table-columnar-statistics [-h] [--params PARAMS] --path PATHS
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    (Required) Path to source table. See also: [YPATH](../../../user-guide/storage/ypath.md) Accepted multiple times.

### read

reads rows from table and parse (optionally)

```bash
usage: yt read [-h] [--params PARAMS] [--table TABLE] [--format FORMAT] [--table-reader TABLE_READER] [--control-attributes CONTROL_ATTRIBUTES] [--unordered] [--as-json-list] [table]
```

#### Positional Arguments

> `table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--format`    output format. (yson string), one of «yson», «json», «yamr», «dsv», «yamred_dsv», «schemaful_dsv» with modifications. See also: [Formats](../../../user-guide/storage/formats.md)

> `--table-reader`    structured table-reader in yson format

> `--control-attributes`    structured control-attributes in yson format

> `--unordered`

> `--as-json-list`    In case of JSON format output stream as JSON list instead of JSON lines format

### read-blob-table

reads file from blob table

```bash
usage: yt read-blob-table [-h] [--params PARAMS] [--table TABLE] [--part-index-column-name PART_INDEX_COLUMN_NAME] [--data-column-name DATA_COLUMN_NAME] [--part-size PART_SIZE] [--table-reader TABLE_READER]
                          [table]
```

#### Positional Arguments

> `table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--part-index-column-name`    name of column with part indexes

> `--data-column-name`    name of column with data

> `--part-size`    size of each blob

> `--table-reader`    structured table-reader in yson format

### read-table

reads rows from table and parse (optionally)

```bash
usage: yt read-table [-h] [--params PARAMS] [--table TABLE] [--format FORMAT] [--table-reader TABLE_READER] [--control-attributes CONTROL_ATTRIBUTES] [--unordered] [--as-json-list] [table]
```

#### Positional Arguments

> `table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--format`    output format. (yson string), one of «yson», «json», «yamr», «dsv», «yamred_dsv», «schemaful_dsv» with modifications. See also: [Formats](../../../user-guide/storage/formats.md)

> `--table-reader`    structured table-reader in yson format

> `--control-attributes`    structured control-attributes in yson format

> `--unordered`

> `--as-json-list`    In case of JSON format output stream as JSON list instead of JSON lines format

### write

writes rows from input_stream to table

```bash
usage: yt write [-h] [--params PARAMS] [--table TABLE] [--format FORMAT] [--table-writer TABLE_WRITER] [--compressed] [--no-compression] [table]
```

#### Positional Arguments

> `table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--format`    input format. (yson string), one of «yson», «json», «yamr», «dsv», «yamred_dsv», «schemaful_dsv» with modifications. See also: [Formats](../../../user-guide/storage/formats.md)

> `--table-writer`    structured table-writer in yson format

> `--compressed`    expect stream to contain compressed file data. Warning! This option disables retries. Data is passed directly to proxy without recompression.

> `--no-compression`    disable compression

### write-table

writes rows from input_stream to table

```bash
usage: yt write-table [-h] [--params PARAMS] [--table TABLE] [--format FORMAT] [--table-writer TABLE_WRITER] [--compressed] [--no-compression] [table]
```

#### Positional Arguments

> `table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--format`    input format. (yson string), one of «yson», «json», «yamr», «dsv», «yamred_dsv», «schemaful_dsv» with modifications. See also: [Formats](../../../user-guide/storage/formats.md)

> `--table-writer`    structured table-writer in yson format

> `--compressed`    expect stream to contain compressed file data. Warning! This option disables retries. Data is passed directly to proxy without recompression.

> `--no-compression`    disable compression

## Dynamic table commands



### alter-table-replica

changes mode and enables or disables a table replica or replicated table tracker for table replica

```bash
usage: yt alter-table-replica [-h] [--params PARAMS] [--enable | --disable] [--enable-replicated-table-tracker | --disable-replicated-table-tracker] [--mode MODE] replica_id
```

#### Positional Arguments

> `replica_id`

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--enable`    enable table replica

> `--disable`    disable table replica

> `--enable-replicated-table-tracker`    enable replicated table tracker for table replica

> `--disable-replicated-table-tracker`    disable replicated table tracker for table replica

> `--mode`    alternation mode, can be «sync» or «async»

### balance-tablet-cells

reassign tablets evenly among tablet cells

```bash
usage: yt balance-tablet-cells [-h] [--params PARAMS] [--bundle BUNDLE] [--tables [TABLES ...]] [--sync] [bundle]
```

#### Positional Arguments

> `bundle`    tablet cell bundle

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--bundle`    tablet cell bundle

> `--tables`    tables to balance. If omitted, all tables of bundle will be balanced

> `--sync`

### delete

. Use "delete-rows" instead of "delete"

```bash
usage: yt delete [-h] [--params PARAMS]
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

### delete-rows

deletes rows with keys from input_stream from dynamic table

```bash
usage: yt delete-rows [-h] [--params PARAMS] [--table TABLE] [--format FORMAT] [--atomicity {full,none}] [--durability {sync,async}] [--require-sync-replica | --no-require-sync-replica] [table]
```

#### Positional Arguments

> `table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--format`    input format. (yson string), one of «yson», «json», «yamr», «dsv», «yamred_dsv», «schemaful_dsv» with modifications. See also: [Formats](../../../user-guide/storage/formats.md)

> `--atomicity`    Possible choices: full, none

> `--durability`    Possible choices: sync, async

> `--require-sync-replica`

> `--no-require-sync-replica`

### explain-query

explains a SQL-like query on dynamic table

```bash
usage: yt explain-query [-h] [--params PARAMS] [--query QUERY] [--timestamp TIMESTAMP] [--input-row-limit INPUT_ROW_LIMIT] [--output-row-limit OUTPUT_ROW_LIMIT] [--allow-full-scan | --forbid-full-scan]
                        [--allow-join-without-index | --forbid-join-without-index] [--execution-pool EXECUTION_POOL] [--format FORMAT] [--syntax-version SYNTAX_VERSION]
                        [query]
```

#### Positional Arguments

> `query`

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--query`

> `--timestamp`

> `--input-row-limit`

> `--output-row-limit`

> `--allow-full-scan`

> `--forbid-full-scan`

> `--allow-join-without-index`

> `--forbid-join-without-index`

> `--execution-pool`

> `--format`

> `--syntax-version`

### freeze-table

freezes the table

```bash
usage: yt freeze-table [-h] [--params PARAMS] [--path PATH] [--first-tablet-index FIRST_TABLET_INDEX] [--last-tablet-index LAST_TABLET_INDEX] [--sync] [path]
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--first-tablet-index`

> `--last-tablet-index`

> `--sync`

### get-tablet-errors

returns dynamic table tablet and replication errors

```bash
usage: yt get-tablet-errors [-h] [--params PARAMS] [--path PATH] [--limit LIMIT] [--format FORMAT] [path]
```

#### Positional Arguments

> `path`    path to dynamic table. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    path to dynamic table. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--limit`    number of tablets with errors

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md)

### get-tablet-infos

returns various runtime tablet information

```bash
usage: yt get-tablet-infos [-h] [--params PARAMS] [--path PATH] [--tablet-indexes TABLET_INDEXES [TABLET_INDEXES ...]] [--format FORMAT] [path]
```

#### Positional Arguments

> `path`    path to dynamic table. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    path to dynamic table. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--tablet-indexes`

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md)

### insert

. Use "insert-rows" instead of "insert"

```bash
usage: yt insert [-h] [--params PARAMS]
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

### insert-rows

inserts rows from input_stream to dynamic table

```bash
usage: yt insert-rows [-h] [--params PARAMS] [--table TABLE] [--format FORMAT] [--atomicity {full,none}] [--durability {sync,async}] [--require-sync-replica | --no-require-sync-replica] [--update | --no-update]
                      [--aggregate | --no-aggregate] [--lock_type LOCK_TYPE]
                      [table]
```

#### Positional Arguments

> `table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--format`    input format. (yson string), one of «yson», «json», «yamr», «dsv», «yamred_dsv», «schemaful_dsv» with modifications. See also: [Formats](../../../user-guide/storage/formats.md)

> `--atomicity`    Possible choices: full, none

> `--durability`    Possible choices: sync, async

> `--require-sync-replica`

> `--no-require-sync-replica`

> `--update`

> `--no-update`

> `--aggregate`

> `--no-aggregate`

> `--lock_type`

### lookup

. Use "lookup-rows" instead of "lookup"

```bash
usage: yt lookup [-h] [--params PARAMS]
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

### lookup-rows

lookups rows in dynamic table

```bash
usage: yt lookup-rows [-h] [--params PARAMS] [--table TABLE] [--format FORMAT] [--versioned] [--column-name COLUMN_NAMES] [table]
```

#### Positional Arguments

> `table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--format`    input format. (yson string), one of «yson», «json», «yamr», «dsv», «yamred_dsv», «schemaful_dsv» with modifications. See also: [Formats](../../../user-guide/storage/formats.md)

> `--versioned`    return all versions of the requested rows

> `--column-name`    column name to lookup Accepted multiple times.

### mount-table

mounts the table

```bash
usage: yt mount-table [-h] [--params PARAMS] [--path PATH] [--first-tablet-index FIRST_TABLET_INDEX] [--last-tablet-index LAST_TABLET_INDEX] [--freeze] [--sync]
                      [--cell-id CELL_ID | --target-cell-ids TARGET_CELL_IDS [TARGET_CELL_IDS ...]]
                      [path]
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--first-tablet-index`

> `--last-tablet-index`

> `--freeze`

> `--sync`

> `--cell-id`    tablet cell id where the tablets will be mounted to, if omitted then an appropriate cell is chosen automatically

> `--target-cell-ids`    tablet cell id for each tablet in range [first-tablet-index, last-tablet-index]. Should be used if exact destination cell for each tablet is known.

### remount-table

remounts the table

```bash
usage: yt remount-table [-h] [--params PARAMS] [--path PATH] [--first-tablet-index FIRST_TABLET_INDEX] [--last-tablet-index LAST_TABLET_INDEX] [path]
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--first-tablet-index`

> `--last-tablet-index`

### reshard-table

changes pivot keys separating tablets of a given table

```bash
usage: yt reshard-table [-h] [--params PARAMS] [--path PATH] [--first-tablet-index FIRST_TABLET_INDEX] [--last-tablet-index LAST_TABLET_INDEX] [--tablet-count TABLET_COUNT] [--sync] [--uniform] [--enable-slicing]
                        [--slicing-accuracy SLICING_ACCURACY]
                        [path] [pivot_keys ...]
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `pivot_keys`

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--first-tablet-index`

> `--last-tablet-index`

> `--tablet-count`

> `--sync`

> `--uniform`

> `--enable-slicing`

> `--slicing-accuracy`

### reshard-table-automatic

automatically balance tablets of a mounted table according to tablet balancer config

```bash
usage: yt reshard-table-automatic [-h] [--params PARAMS] [--path PATH] [--sync] [path]
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--sync`

### select

. Use "select-rows" instead of "select"

```bash
usage: yt select [-h] [--params PARAMS]
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

### select-rows

executes a SQL-like query on dynamic table

```bash
usage: yt select-rows [-h] [--params PARAMS] [--query QUERY] [--timestamp TIMESTAMP] [--input-row-limit INPUT_ROW_LIMIT] [--output-row-limit OUTPUT_ROW_LIMIT] [--verbose-logging]
                      [--enable-code-cache | --disable-code-cache] [--allow-full-scan | --forbid-full-scan] [--allow-join-without-index | --forbid-join-without-index] [--execution-pool EXECUTION_POOL]
                      [--format FORMAT] [--print-statistics] [--syntax-version SYNTAX_VERSION]
                      [query]
```

#### Positional Arguments

> `query`

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--query`

> `--timestamp`

> `--input-row-limit`

> `--output-row-limit`

> `--verbose-logging`

> `--enable-code-cache`

> `--disable-code-cache`

> `--allow-full-scan`

> `--forbid-full-scan`

> `--allow-join-without-index`

> `--forbid-join-without-index`

> `--execution-pool`

> `--format`

> `--print-statistics`

> `--syntax-version`

### trim-rows

trim rows of the dynamic table

```bash
usage: yt trim-rows [-h] [--params PARAMS] [--path PATH] [path] tablet_index trimmed_row_count
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `tablet_index`

> `trimmed_row_count`

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

### unfreeze-table

unfreezes the table

```bash
usage: yt unfreeze-table [-h] [--params PARAMS] [--path PATH] [--first-tablet-index FIRST_TABLET_INDEX] [--last-tablet-index LAST_TABLET_INDEX] [--sync] [path]
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--first-tablet-index`

> `--last-tablet-index`

> `--sync`

### unmount-table

unmounts the table

```bash
usage: yt unmount-table [-h] [--params PARAMS] [--path PATH] [--first-tablet-index FIRST_TABLET_INDEX] [--last-tablet-index LAST_TABLET_INDEX] [--force] [--sync] [path]
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--first-tablet-index`

> `--last-tablet-index`

> `--force`

> `--sync`

## Run operation commands



### erase

erases table or part of it

```bash
usage: yt erase [-h] [--params PARAMS] [--table TABLE] [--print-statistics] [--async] [--spec SPEC] [table]
```

#### Positional Arguments

> `table`    path to table to erase. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--table`    path to table to erase. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--print-statistics`

> `--async`    do not track operation progress

> `--spec`    structured spec in yson format

### join-reduce

runs join-reduce operation

```bash
usage: yt join-reduce [-h] [--params PARAMS] [--binary command] --src SOURCE_TABLE [SOURCE_TABLE ...] --dst DESTINATION_TABLE [--file YT_FILES] [--local-file LOCAL_FILES] [--job-count JOB_COUNT]
                      [--memory-limit MEMORY_LIMIT] [--spec SPEC] [--format FORMAT] [--input-format INPUT_FORMAT] [--output-format OUTPUT_FORMAT] [--print-statistics] [--async] --join-by JOIN_BY
                      [command]
```

#### Positional Arguments

> `command`

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--binary`

> `--src`    (Required)  Accepted multiple times.

> `--dst`    (Required)  Accepted multiple times.

> `--file`    > Accepted multiple times.

> `--local-file`    > Accepted multiple times.

> `--job-count`

> `--memory-limit`    in MB

> `--spec`    structured spec in yson format

> `--format`

> `--input-format`

> `--output-format`

> `--print-statistics`

> `--async`    do not track operation progress

> `--join-by`    (Required) Columns to join by. In order to choose descending sort order, provide a map of form "{name=foo; sort_order=descending}"

### map

runs map operation

```bash
usage: yt map [-h] [--params PARAMS] [--binary command] --src SOURCE_TABLE [SOURCE_TABLE ...] --dst DESTINATION_TABLE [--file YT_FILES] [--local-file LOCAL_FILES] [--job-count JOB_COUNT]
              [--memory-limit MEMORY_LIMIT] [--spec SPEC] [--format FORMAT] [--input-format INPUT_FORMAT] [--output-format OUTPUT_FORMAT] [--print-statistics] [--async] [--ordered]
              [command]
```

#### Positional Arguments

> `command`

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--binary`

> `--src`    (Required)  Accepted multiple times.

> `--dst`    (Required)  Accepted multiple times.

> `--file`    > Accepted multiple times.

> `--local-file`    > Accepted multiple times.

> `--job-count`

> `--memory-limit`    in MB

> `--spec`    structured spec in yson format

> `--format`

> `--input-format`

> `--output-format`

> `--print-statistics`

> `--async`    do not track operation progress

> `--ordered`    Force ordered input for mapper.

### map-reduce

runs map (optionally), sort, reduce and reduce-combine (optionally) operations

```bash
usage: yt map-reduce [-h] [--params PARAMS] [--mapper MAPPER] --reducer REDUCER [--reduce-combiner REDUCE_COMBINER] --src SOURCE_TABLE --dst DESTINATION_TABLE [--map-file MAP_YT_FILES]
                     [--map-local-file MAP_LOCAL_FILES] [--reduce-file REDUCE_YT_FILES] [--reduce-local-file REDUCE_LOCAL_FILES] [--reduce-combiner-file REDUCE_COMBINER_YT_FILES]
                     [--reduce-combiner-local-file REDUCE_COMBINER_LOCAL_FILES] [--mapper-memory-limit MAPPER_MEMORY_LIMIT] [--reducer-memory-limit REDUCER_MEMORY_LIMIT]
                     [--reduce-combiner-memory-limit REDUCE_COMBINER_MEMORY_LIMIT] --reduce-by REDUCE_BY [--sort-by SORT_BY] [--spec SPEC] [--format FORMAT] [--map-input-format MAP_INPUT_FORMAT]
                     [--map-output-format MAP_OUTPUT_FORMAT] [--reduce-input-format REDUCE_INPUT_FORMAT] [--reduce-output-format REDUCE_OUTPUT_FORMAT] [--reduce-combiner-input-format REDUCE_COMBINER_INPUT_FORMAT]
                     [--reduce-combiner-output-format REDUCE_COMBINER_OUTPUT_FORMAT] [--async]
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--mapper`

> `--reducer`    (Required)

> `--reduce-combiner`

> `--src`    (Required)  Accepted multiple times.

> `--dst`    (Required)  Accepted multiple times.

> `--map-file`    > Accepted multiple times.

> `--map-local-file`    > Accepted multiple times.

> `--reduce-file`    > Accepted multiple times.

> `--reduce-local-file`    > Accepted multiple times.

> `--reduce-combiner-file`    > Accepted multiple times.

> `--reduce-combiner-local-file`    > Accepted multiple times.

> `--mapper-memory-limit, --map-memory-limit`    in MB

> `--reducer-memory-limit, --reduce-memory-limit`    in MB

> `--reduce-combiner-memory-limit`    in MB

> `--reduce-by`    (Required) Columns to reduce by. In order to choose descending sort order, provide a map of form "{name=foo; sort_order=descending}"

> `--sort-by`    Columns to sort by. Must be superset of reduce-by columns. By default is equal to –reduce-by option. In order to choose descending sort order, provide a map of form "{name=foo; sort_order=descending}"

> `--spec`    structured spec in yson format

> `--format`    (yson string), one of «yson», «json», «yamr», «dsv», «yamred_dsv», «schemaful_dsv» with modifications. See also: [Formats](../../../user-guide/storage/formats.md)

> `--map-input-format`    see –format help

> `--map-output-format`    see –format help

> `--reduce-input-format`    see –format help

> `--reduce-output-format`    see –format help

> `--reduce-combiner-input-format`    see –format help

> `--reduce-combiner-output-format`    see –format help

> `--async`    do not track operation progress

### merge

merges source tables to destination table

```bash
usage: yt merge [-h] [--params PARAMS] --src SOURCE_TABLE [SOURCE_TABLE ...] --dst DESTINATION_TABLE [--mode {unordered,ordered,sorted,auto}] [--print-statistics] [--async] [--spec SPEC]
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--src`    (Required)  Accepted multiple times.

> `--dst`    (Required) path to destination table. For append mode add <append=true> before path. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--mode`    Possible choices: unordered, ordered, sorted, auto

> use sorted mode for saving sortedness. unordered mode by default, ordered for saving order of chunks. Mode auto chooses from sorted and unordered modes depending on sortedness of source tables., default: `«auto»`

> `--print-statistics`

> `--async`    do not track operation progress

> `--spec`    structured spec in yson format

### reduce

runs reduce operation

```bash
usage: yt reduce [-h] [--params PARAMS] [--binary command] --src SOURCE_TABLE [SOURCE_TABLE ...] --dst DESTINATION_TABLE [--file YT_FILES] [--local-file LOCAL_FILES] [--job-count JOB_COUNT]
                 [--memory-limit MEMORY_LIMIT] [--spec SPEC] [--format FORMAT] [--input-format INPUT_FORMAT] [--output-format OUTPUT_FORMAT] [--print-statistics] [--async] [--reduce-by REDUCE_BY]
                 [--sort-by SORT_BY] [--join-by JOIN_BY]
                 [command]
```

#### Positional Arguments

> `command`

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--binary`

> `--src`    (Required)  Accepted multiple times.

> `--dst`    (Required)  Accepted multiple times.

> `--file`    > Accepted multiple times.

> `--local-file`    > Accepted multiple times.

> `--job-count`

> `--memory-limit`    in MB

> `--spec`    structured spec in yson format

> `--format`

> `--input-format`

> `--output-format`

> `--print-statistics`

> `--async`    do not track operation progress

> `--reduce-by`    Columns to reduce by. In order to choose descending sort order, provide a map of form "{name=foo; sort_order=descending}"

> `--sort-by`    Columns to sort by. In order to choose descending sort order, provide a map of form "{name=foo; sort_order=descending}"

> `--join-by`    Columns to join by. In order to choose descending sort order, provide a map of form "{name=foo; sort_order=descending}"

### remote-copy

copies source table from remote cluster to destination table on current cluster

```bash
usage: yt remote-copy [-h] [--params PARAMS] --src SOURCE_TABLE [SOURCE_TABLE ...] --dst DESTINATION_TABLE --cluster CLUSTER_NAME [--network NETWORK_NAME] [--copy-attributes] [--print-statistics] [--async]
                      [--cluster-connection CLUSTER_CONNECTION] [--spec SPEC]
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--src`    (Required) path to source tables in remote cluster Accepted multiple times.

> `--dst`    (Required) path to destination table in current cluster

> `--cluster`    (Required) remote cluster proxy, like smith

> `--network`

> `--copy-attributes`    specify this flag to coping node attributes too

> `--print-statistics`

> `--async`    do not track operation progress

> `--cluster-connection`    structured cluster-connection in yson format

> `--spec`    structured spec in yson format

### shuffle

shuffles table randomly

```bash
usage: yt shuffle [-h] [--params PARAMS] --table TABLE [--temp-column-name TEMP_COLUMN_NAME] [--print-statistics] [--async]
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--table`    (Required)

> `--temp-column-name`

> `--print-statistics`

> `--async`    do not track operation progress

### sort

sorts source tables to destination table

```bash
usage: yt sort [-h] [--params PARAMS] --src SOURCE_TABLE [SOURCE_TABLE ...] --dst DESTINATION_TABLE --sort-by SORT_BY [--print-statistics] [--async] [--spec SPEC]
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--src`    (Required)  Accepted multiple times.

> `--dst`    (Required)

> `--sort-by`    (Required) Columns to sort by. In order to choose descending sort order, provide a map of form "{name=foo; sort_order=descending}"

> `--print-statistics`

> `--async`    do not track operation progress

> `--spec`    structured spec in yson format

### vanilla

run vanilla operation

```bash
usage: yt vanilla [-h] [--params PARAMS] [--print-statistics] [--async] [--tasks TASKS] [--spec SPEC]
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--print-statistics`

> `--async`    do not track operation progress

> `--tasks`    task descriptions. structured tasks in yson format

> `--spec`    structured spec in yson format

## Operation commands



### abort-op

aborts operation

```bash
usage: yt abort-op [-h] [--params PARAMS] [--reason REASON] [--operation OPERATION] [operation]
```

#### Positional Arguments

> `operation`    operation id

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--reason`    abort reason

> `--operation`    operation id

### complete-op

completes operation

```bash
usage: yt complete-op [-h] [--params PARAMS] [--operation OPERATION] [operation]
```

#### Positional Arguments

> `operation`    operation id

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--operation`    operation id

### get-operation

get operation attributes through API

```bash
usage: yt get-operation [-h] [--params PARAMS] [--attribute ATTRIBUTES] [--include-scheduler] [--operation OPERATION_ID] [--format FORMAT] [operation_id]
```

#### Positional Arguments

> `operation_id`    operation id

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--attribute`    desired attributes in the response Accepted multiple times.

> `--include-scheduler`    request runtime operation information

> `--operation`    operation id

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md)

### list-operations

list operations that satisfy given options

```bash
usage: yt list-operations [-h] [--params PARAMS] [--user USER] [--state STATE] [--type TYPE] [--filter FILTER] [--pool-tree POOL_TREE] [--pool POOL] [--with-failed-jobs | --without-failed-jobs]
                          [--from-time FROM_TIME] [--to-time TO_TIME] [--cursor-time CURSOR_TIME] [--cursor-direction CURSOR_DIRECTION] [--include-archive] [--no-include-counters] [--limit LIMIT]
                          [--format FORMAT]
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--user`    filter operations by user

> `--state`    filter operations by state

> `--type`    filter operations by operation type

> `--filter`    filter operation by some text factor. For example, part of the title can be passed to this option

> `--pool-tree`    filter operations by pool tree

> `--pool`    filter operations by pool. If –pool-tree is set, filters operations with this pool in specified pool tree

> `--with-failed-jobs`    show only operations with failed jobs

> `--without-failed-jobs`    show only operations without failed jobs

> `--from-time`    lower limit for operations start time. Time is accepted as unix timestamp or time string in {{product-name}} format

> `--to-time`    upper limit for operations start time. Time is accepted as unix timestamp or time string in {{product-name}} format

> `--cursor-time`    cursor time. Used in combination with –cursor-direction and –limit. Time is accepted as unix timestamp or time string in {{product-name}} format

> `--cursor-direction`    cursor direction, can be one of («none», «past», «future»). Used in combination with –cursor-time and –limit

> `--include-archive`    include operations from archive in result

> `--no-include-counters`    do not include operation counters in result

> `--limit`    maximum number of operations in output

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md)

### resume-op

continues operation after suspending

```bash
usage: yt resume-op [-h] [--params PARAMS] [--operation OPERATION] [operation]
```

#### Positional Arguments

> `operation`    operation id

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--operation`    operation id

### suspend-op

suspends operation

```bash
usage: yt suspend-op [-h] [--params PARAMS] [--operation OPERATION] [--abort-running-jobs] [operation]
```

#### Positional Arguments

> `operation`    operation id

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--operation`    operation id

> `--abort-running-jobs`    abort running jobs

### track-op

synchronously tracks operation, prints current progress and finalize at the completion

```bash
usage: yt track-op [-h] [--params PARAMS] [--operation OPERATION] [operation]
```

#### Positional Arguments

> `operation`    operation id

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--operation`    operation id

### update-op-parameters

updates operation runtime parameters

```bash
usage: yt update-op-parameters [-h] [--params PARAMS] [--operation OPERATION_ID] [operation_id] parameters
```

#### Positional Arguments

> `operation_id`    operation id

> `parameters`    structured parameters in yson format

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--operation`    operation id

## Job commands



### abort-job

interrupts running job with preserved result

```bash
usage: yt abort-job [-h] [--params PARAMS] [--interrupt-timeout INTERRUPT_TIMEOUT] job_id
```

#### Positional Arguments

> `job_id`    job id, for example: 5c51-24e204-384-9f3f6437

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--interrupt-timeout`    try to interrupt job before abort during timeout (in ms)

### get-job

get job of operation

```bash
usage: yt get-job [-h] [--params PARAMS] [--job-id JOB_ID] [--operation-id OPERATION_ID] [--format FORMAT] [job_id] [operation_id]
```

#### Positional Arguments

> `job_id`    job id, for example: 5c51-24e204-384-9f3f6437

> `operation_id`    operation id, for example: 876084ca-efd01a47-3e8-7a62e787

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--job-id`    job id, for example: 5c51-24e204-384-9f3f6437

> `--operation-id`    operation id, for example: 876084ca-efd01a47-3e8-7a62e787

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md)

### get-job-input

get full input of the specified job

```bash
usage: yt get-job-input [-h] [--params PARAMS] [--job-id JOB_ID] [job_id]
```

#### Positional Arguments

> `job_id`    job id, for example: 5c51-24e204-384-9f3f6437

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--job-id`    job id, for example: 5c51-24e204-384-9f3f6437

### get-job-input-paths

get input paths of the specified job

```bash
usage: yt get-job-input-paths [-h] [--params PARAMS] [--job-id JOB_ID] [job_id]
```

#### Positional Arguments

> `job_id`    job id, for example: 5c51-24e204-384-9f3f6437

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--job-id`    job id, for example: 5c51-24e204-384-9f3f6437

### get-job-spec

get spec of the specified job

```bash
usage: yt get-job-spec [-h] [--params PARAMS] [--job-id JOB_ID] [--omit-node-directory | --no-omit-node-directory] [--omit-input-table-specs | --no-omit-input-table-specs]
                       [--omit-output-table-specs | --no-omit-output-table-specs]
                       [job_id]
```

#### Positional Arguments

> `job_id`    job id, for example: 5c51-24e204-384-9f3f6437

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--job-id`    job id, for example: 5c51-24e204-384-9f3f6437

> `--omit-node-directory`    Whether node directory should be removed from job spec (true by default)

> `--no-omit-node-directory`    Whether node directory should be removed from job spec (true by default)

> `--omit-input-table-specs`    Whether input table specs should be removed from job spec (false by default)

> `--no-omit-input-table-specs`    Whether input table specs should be removed from job spec (false by default)

> `--omit-output-table-specs`    Whether output table specs should be removed from job spec (false by default)

> `--no-omit-output-table-specs`    Whether output table specs should be removed from job spec (false by default)

### get-job-stderr

gets stderr of the specified job

```bash
usage: yt get-job-stderr [-h] [--params PARAMS] [--job-id JOB_ID] [--operation-id OPERATION_ID] [job_id] [operation_id]
```

#### Positional Arguments

> `job_id`    job id, for example: 5c51-24e204-384-9f3f6437

> `operation_id`    operation id, for example: 876084ca-efd01a47-3e8-7a62e787

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--job-id`    job id, for example: 5c51-24e204-384-9f3f6437

> `--operation-id`    operation id, for example: 876084ca-efd01a47-3e8-7a62e787

### list-jobs

list jobs of operation

```bash
usage: yt list-jobs [-h] [--params PARAMS] [--operation OPERATION_ID] [--job-type JOB_TYPE] [--job-state JOB_STATE] [--address ADDRESS] [--job-competition-id JOB_COMPETITION_ID]
                    [--sort-field {type,state,start_time,finish_time,address,duration,progress,id}] [--sort-order SORT_ORDER] [--limit LIMIT] [--offset OFFSET] [--with-spec] [--with-stderr] [--with-fail-context]
                    [--with-competitors] [--with-monitoring-descriptor] [--include-cypress] [--include-runtime] [--include-archive] [--data-source {auto,runtime,archive,manual}] [--format FORMAT]
                    [operation_id]
```

#### Positional Arguments

> `operation_id`    operation id

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--operation`    operation id

> `--job-type`    filter jobs by job type

> `--job-state`    filter jobs by job state

> `--address`    filter jobs by node address

> `--job-competition-id`    filter jobs by job competition id

> `--sort-field`    Possible choices: type, state, start_time, finish_time, address, duration, progress, id

> field to sort jobs by

> `--sort-order`    sort order. Can be either «ascending» or «descending»

> `--limit`    output limit

> `--offset`    offset starting from zero

> `--with-spec`

> `--with-stderr`

> `--with-fail-context`

> `--with-competitors`    with competitive jobs

> `--with-monitoring-descriptor`

> `--include-cypress`    include jobs from Cypress in result. Have effect only if –data-source is set to «manual»

> `--include-runtime`    include jobs from controller agents in result. Have effect only if –data-source is set to «manual»

> `--include-archive`    include jobs from archive in result. Have effect only if –data-source is set to «manual»

> `--data-source`    Possible choices: auto, runtime, archive, manual

> data sources to list jobs from

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md)

### run-job-shell

runs interactive shell in the job sandbox

```bash
usage: yt run-job-shell [-h] [--params PARAMS] [--shell-name SHELL_NAME] [--timeout TIMEOUT] [--command COMMAND] job_id [command]
```

#### Positional Arguments

> `job_id`    job id, for example: 5c51-24e204-384-9f3f6437

> `command`

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--shell-name`    name of the job shell to start

> `--timeout`    inactivity timeout in milliseconds after job has finished, by default 60000 milliseconds

> `--command`

## Transaction commands



### abort-tx

aborts transaction. All changes will be lost

```bash
usage: yt abort-tx [-h] [--params PARAMS] [--transaction TRANSACTION] [transaction]
```

#### Positional Arguments

> `transaction`    transaction id, for example: 5c51-24e204-1-9f3f6437

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--transaction`    transaction id, for example: 5c51-24e204-1-9f3f6437

### commit-tx

saves all transaction changes

```bash
usage: yt commit-tx [-h] [--params PARAMS] [--transaction TRANSACTION] [transaction]
```

#### Positional Arguments

> `transaction`    transaction id, for example: 5c51-24e204-1-9f3f6437

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--transaction`    transaction id, for example: 5c51-24e204-1-9f3f6437

### lock

tries to lock the path

```bash
usage: yt lock [-h] [--params PARAMS] [--path PATH] [--mode {snapshot,shared,exclusive}] [--waitable] [--wait-for WAIT_FOR] [--child-key CHILD_KEY] [--attribute-key ATTRIBUTE_KEY] [path]
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--mode`    Possible choices: snapshot, shared, exclusive

> blocking type, exclusive by default

> `--waitable`    wait for lock if node is under blocking

> `--wait-for`    wait interval in milliseconds

> `--child-key`    child key of shared lock

> `--attribute-key`    attribute key of shared lock

### ping-tx

prolongs transaction lifetime

```bash
usage: yt ping-tx [-h] [--params PARAMS] [--transaction TRANSACTION] [transaction]
```

#### Positional Arguments

> `transaction`    transaction id, for example: 5c51-24e204-1-9f3f6437

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--transaction`    transaction id, for example: 5c51-24e204-1-9f3f6437

### start-tx

starts transaction

```bash
usage: yt start-tx [-h] [--params PARAMS] [--attributes ATTRIBUTES] [--timeout TIMEOUT]
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--attributes`    structured attributes in yson format

> `--timeout`    transaction lifetime since last ping in milliseconds

### unlock

tries to unlock the path

```bash
usage: yt unlock [-h] [--params PARAMS] [--path PATH] [path]
```

#### Positional Arguments

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

## ACL (permission) commands



### add-member

adds member to Cypress node group

```bash
usage: yt add-member [-h] [--params PARAMS] [--member MEMBER] [--group GROUP] [member] [group]
```

#### Positional Arguments

> `member`

> `group`

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--member`

> `--group`

### check-permission

checks permission for Cypress node

```bash
usage: yt check-permission [-h] [--params PARAMS] [--user USER] [--permission PERMISSION] [--path PATH] [--read-from READ_FROM] [--cache-sticky-group-size CACHE_STICKY_GROUP_SIZE] [--columns COLUMNS]
                           [--format FORMAT]
                           [user] [permission] [path]
```

#### Positional Arguments

> `user`

> `permission`    one of read, write, administer, create, use

> `path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--user`

> `--permission`    one of read, write, administer, create, use

> `--path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--read-from`    Can be set to «cache» to enable reads from system cache

> `--cache-sticky-group-size`    Size of sticky group size for read_from=»cache» mode

> `--columns`    structured columns in yson format

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md), default: `«<format=pretty>yson»`

### issue-token

issues a new token for user

```bash
usage: yt issue-token [-h] [--params PARAMS] [--password PASSWORD] user
```

#### Positional Arguments

> `user`    user to issue token

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--password`    user password

### list-user-tokens

lists sha256-encoded user tokens

```bash
usage: yt list-user-tokens [-h] [--params PARAMS] [--password PASSWORD] user
```

#### Positional Arguments

> `user`    user to revoke token

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--password`    user password

### remove-member

removes member from Cypress node group

```bash
usage: yt remove-member [-h] [--params PARAMS] [--member MEMBER] [--group GROUP] [member] [group]
```

#### Positional Arguments

> `member`

> `group`

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--member`

> `--group`

### revoke-token

revokes user token

```bash
usage: yt revoke-token [-h] [--params PARAMS] [--password PASSWORD] [--token TOKEN] [--token-sha256 TOKEN_SHA256] user
```

#### Positional Arguments

> `user`    user to revoke token

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--password`    user password

> `--token`    token to revoke

> `--token-sha256`    sha256-encoded token to revoke

### set-user-password

updates user password

```bash
usage: yt set-user-password [-h] [--params PARAMS] [--current-password CURRENT_PASSWORD] [--new-password NEW_PASSWORD] user
```

#### Positional Arguments

> `user`    user to set password

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--current-password`    current user password

> `--new-password`    new user password

## Diagnostics



### download-core-dump

downloads core dump for a given operation_id and job_id from a given core_table_path. Tool for downloading job core dumps

```bash
usage: yt download-core-dump [-h] [--params PARAMS] [--operation-id OPERATION_ID] [--core-table-path CORE_TABLE_PATH] [--job-id JOB_ID] [--core-index CORE_INDICES] [--output-directory OUTPUT_DIRECTORY]
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--operation-id`    Operation id (should be specified if proper core file naming is needed)

> `--core-table-path`    A path to the core table

> `--job-id`    Id of a job that produced core dump. If not specified, an arbitrary job with core dumps is taken

> `--core-index`    Indices of core dumps to download (indexing inside single job). Several indices may be specified. If not specified, all core dumps will be downloaded. Requires –job-id to be specified Accepted multiple times.

> `--output-directory`    A directory to save the core dumps. Defaults to the current working directory, default: `«.»`

### job-tool

Tool helps to debug user job code by preparing job environment on local machine.

It downloads all necessary job files, fail context (small portion of job input data)
and prepares run script.

```bash
usage: yt job-tool [-h] [--params PARAMS] command ...
```

#### Positional Arguments

> `command`    Possible choices: prepare-job-environment, run-job

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

#### Sub-commands

##### prepare-job-environment

prepare all necessary stuff for job

```bash
yt job-tool prepare-job-environment [-h] [--operation-id OPERATION_ID] [--job-id JOB_ID] [--job-path JOB_PATH] [--run] [--full-input | --context] [operation_id] [job_id]
```

###### Positional Arguments

> `operation_id`

> `job_id`

###### Named Arguments

> `--operation-id`

> `--job-id`

> `--job-path`    output directory to store job environment. Default: `<cwd>/job_<job_id>`

> `--run`    run job when job environment is prepared

> `--full-input, --full`    download input context of a job

> `--context`    download fail context of a job

##### run-job

runs job binary

```bash
yt job-tool run-job [-h] [--job-path JOB_PATH] [--env ENV] [job_path]
```

###### Positional Arguments

> `job_path`    path to prepared job environment

###### Named Arguments

> `--job-path`    path to prepared job environment

> `--env`    environment to use in script run in YSON format

## Parquet commands



### dump-parquet

dump parquet into a file from table with a strict schema

```bash
usage: yt dump-parquet [-h] [--params PARAMS] [--table TABLE] --output-file OUTPUT_FILE [table]
```

#### Positional Arguments

> `table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--output-file`    (Required)

### upload-parquet

upload parquet from a file into a table that must be created with a strict schema

```bash
usage: yt upload-parquet [-h] [--params PARAMS] [--table TABLE] --input-file INPUT_FILE [table]
```

#### Positional Arguments

> `table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--input-file`    (Required)

## Queue



### advance-consumer

advances consumer offset for the given queue

```bash
usage: yt advance-consumer [-h] [--params PARAMS] [--consumer-path CONSUMER_PATH] [--queue-path QUEUE_PATH] --partition-index PARTITION_INDEX [--old-offset OLD_OFFSET] --new-offset NEW_OFFSET
                           [consumer_path] [queue_path]
```

#### Positional Arguments

> `consumer_path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `queue_path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--consumer-path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--queue-path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--partition-index`    (Required)

> `--old-offset`

> `--new-offset`    (Required)

### list-queue-consumer-registrations

lists queue consumer registrations

```bash
usage: yt list-queue-consumer-registrations [-h] [--params PARAMS] [--queue-path QUEUE_PATH] [--consumer-path CONSUMER_PATH] [--format FORMAT]
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--queue-path`    Path to queue in Cypress; cluster may be specified. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--consumer-path`    Path to consumer in Cypress; cluster may be specified. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md)

### pull-consumer

reads rows from a single partition of a queue (i.e. any ordered dynamic table) with authorization via consumer

```bash
usage: yt pull-consumer [-h] [--params PARAMS] [--consumer-path CONSUMER_PATH] [--queue-path QUEUE_PATH] --offset OFFSET --partition-index PARTITION_INDEX [--max-row-count MAX_ROW_COUNT]
                        [--max-data-weight MAX_DATA_WEIGHT] [--replica-consistency {none,sync}] [--format FORMAT]
                        [consumer_path] [queue_path]
```

#### Positional Arguments

> `consumer_path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `queue_path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--consumer-path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--queue-path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--offset`    (Required)

> `--partition-index`    (Required)

> `--max-row-count`

> `--max-data-weight`

> `--replica-consistency`    Possible choices: none, sync

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md)

### pull-queue

reads rows from a single partition of a queue (i.e. any ordered dynamic table)

```bash
usage: yt pull-queue [-h] [--params PARAMS] [--queue-path QUEUE_PATH] --offset OFFSET --partition-index PARTITION_INDEX [--max-row-count MAX_ROW_COUNT] [--max-data-weight MAX_DATA_WEIGHT]
                     [--replica-consistency {none,sync}] [--format FORMAT]
                     [queue_path]
```

#### Positional Arguments

> `queue_path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--queue-path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--offset`    (Required)

> `--partition-index`    (Required)

> `--max-row-count`

> `--max-data-weight`

> `--replica-consistency`    Possible choices: none, sync

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md)

### register-queue-consumer

registers queue consumer

```bash
usage: yt register-queue-consumer [-h] [--params PARAMS] [--queue-path QUEUE_PATH] [--consumer-path CONSUMER_PATH] (--vital | --non-vital) [--partitions [PARTITIONS ...]] [queue_path] [consumer_path]
```

#### Positional Arguments

> `queue_path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `consumer_path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--queue-path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--consumer-path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--vital`    Whether the consumer is vital

> `--non-vital`    Whether the consumer is vital

> `--partitions`

### unregister-queue-consumer

unregisters queue consumer

```bash
usage: yt unregister-queue-consumer [-h] [--params PARAMS] [--queue-path QUEUE_PATH] [--consumer-path CONSUMER_PATH] [queue_path] [consumer_path]
```

#### Positional Arguments

> `queue_path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `consumer_path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--queue-path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--consumer-path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

## Query tracker



### abort-query

abort query

```bash
usage: yt abort-query [-h] [--params PARAMS] [--message MESSAGE] [--stage STAGE] query_id
```

#### Positional Arguments

> `query_id`    query id

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--message`    optional abort message

> `--stage`    query tracker stage, defaults to «production»

### get-query

get query

```bash
usage: yt get-query [-h] [--params PARAMS] [--attribute ATTRIBUTES] [--stage STAGE] [--format FORMAT] query_id
```

#### Positional Arguments

> `query_id`    query id

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--attribute`    desired attributes in the response Accepted multiple times.

> `--stage`    query tracker stage, defaults to «production»

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md)

### get-query-result

get query result

```bash
usage: yt get-query-result [-h] [--params PARAMS] [--result-index RESULT_INDEX] [--stage STAGE] [--format FORMAT] query_id
```

#### Positional Arguments

> `query_id`    query id

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--result-index`    index of query result, defaults to 0

> `--stage`    query tracker stage, defaults to «production»

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md)

### list-queries

list operations that satisfy given options

```bash
usage: yt list-queries [-h] [--params PARAMS] [--user USER] [--engine ENGINE] [--state STATE] [--filter FILTER] [--from-time FROM_TIME] [--to-time TO_TIME] [--cursor-time CURSOR_TIME]
                       [--cursor-direction CURSOR_DIRECTION] [--limit LIMIT] [--attribute ATTRIBUTES] [--stage STAGE] [--format FORMAT]
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--user`    filter queries by user

> `--engine`    filter queries by engine

> `--state`    filter queries by state

> `--filter`    filter queries by some text factor. For example, part of the query can be passed to this option

> `--from-time`    lower limit for operations start time. Time is accepted as unix timestamp or time string in {{product-name}} format

> `--to-time`    upper limit for operations start time. Time is accepted as unix timestamp or time string in {{product-name}} format

> `--cursor-time`    cursor time. Used in combination with –cursor-direction and –limit. Time is accepted as unix timestamp or time string in {{product-name}} format

> `--cursor-direction`    cursor direction, can be one of («none», «past», «future»). Used in combination with –cursor-time and –limit

> `--limit`    maximum number of operations in output

> `--attribute`    desired attributes in the response Accepted multiple times.

> `--stage`    query tracker stage, defaults to «production»

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md)

### read-query-result

read query result

```bash
usage: yt read-query-result [-h] [--params PARAMS] [--result-index RESULT_INDEX] [--stage STAGE] [--format FORMAT] query_id
```

#### Positional Arguments

> `query_id`    query id

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--result-index`    index of query result, defaults to 0

> `--stage`    query tracker stage, defaults to «production»

> `--format`    output format. (yson string), one of «yson», «json», «yamr», «dsv», «yamred_dsv», «schemaful_dsv» with modifications. See also: [Formats](../../../user-guide/storage/formats.md)

### start-query

start query

```bash
usage: yt start-query [-h] [--params PARAMS] [--settings SETTINGS] [--files FILES] [--access-control-object ACCESS_CONTROL_OBJECT] [--stage STAGE] engine query
```

#### Positional Arguments

> `engine`    engine of a query, one of «ql», «yql», «chyt», «spyt»

> `query`    query text

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--settings`    additional settings of a query in structured form. structured settings in yson format

> `--files`    query files, a YSON list of files, each of which is represented by a map with keys «name», «content», «type».Field «type» is one of «raw_inline_data», «url». structured files in yson format

> `--access-control-object`    optional access control object name

> `--stage`    query tracker stage, defaults to «production»

## Other commands



### admin

Administer commands

```bash
usage: yt admin [-h] admin_command ...
```

#### Positional Arguments

> `admin_command`    Possible choices: switch-leader

#### Sub-commands

##### switch-leader

Switch master cell leader

```bash
yt admin switch-leader [-h] [--cell-id CELL_ID] [--new-leader-address NEW_LEADER_ADDRESS]
```

###### Named Arguments

> `--cell-id`

> `--new-leader-address`

### chyt

ClickHouse over {{product-name}} commands

```bash
usage: yt chyt [-h] clickhouse_command ...
```

#### Positional Arguments

> `clickhouse_command`    Possible choices: start-clique, execute, ctl

#### Sub-commands

##### start-clique

starts a CHYT clique consisting of a given number of instances

```bash
yt chyt start-clique [-h] --instance-count INSTANCE_COUNT [--alias ALIAS] [--cypress-ytserver-clickhouse-path CYPRESS_YTSERVER_CLICKHOUSE_PATH]
                     [--cypress-clickhouse-trampoline-path CYPRESS_CLICKHOUSE_TRAMPOLINE_PATH] [--cypress-ytserver-log-tailer-path CYPRESS_YTSERVER_LOG_TAILER_PATH]
                     [--cypress-base-config-path CYPRESS_BASE_CONFIG_PATH] [--cpu-limit CPU_LIMIT] [--cypress-geodata-path CYPRESS_GEODATA_PATH] [--abort-existing] [--artifact-path ARTIFACT_PATH]
                     [--skip-version-compatibility-validation] [--spec SPEC] [--clickhouse-config CLICKHOUSE_CONFIG] [--memory-config MEMORY_CONFIG]
```

###### Named Arguments

> `--instance-count`    (Required)

> `--alias, --operation-alias`    Alias for clique; may be also specified via CHYT_PROXY env variable

> `--cypress-ytserver-clickhouse-path`

> `--cypress-clickhouse-trampoline-path`

> `--cypress-ytserver-log-tailer-path`

> `--cypress-base-config-path`    Default: `«//sys/clickhouse/config»`

> `--cpu-limit`

> `--cypress-geodata-path`

> `--abort-existing`    Abort existing operation under same alias

> `--artifact-path`    path for artifact directory; by default equals to //sys/clickhouse/kolkhoz/<operation_alias>

> `--skip-version-compatibility-validation`    (For developer use only)

> `--spec`    structured spec in yson format

> `--clickhouse-config`    ClickHouse configuration patch. structured clickhouse-config in yson format

> `--memory-config`    Memory configuration. structured memory-config in yson format

##### execute

executes ClickHouse query in given CHYT clique

```bash
yt chyt execute [-h] [--alias ALIAS] [--query QUERY] [--format FORMAT] [--setting SETTING] [query]
```

###### Positional Arguments

> `query`    Query to execute; do not specify FORMAT in query, use –format instead

###### Named Arguments

> `--alias, --operation-alias`    Alias for clique; may be also specified via CHYT_PROXY env variable

> `--query`    Query to execute; do not specify FORMAT in query, use –format instead

> `--format`    ClickHouse data format; refer to [https://clickhouse.tech/docs/en/interfaces/formats/](https://clickhouse.tech/docs/en/interfaces/formats/); default is TabSeparated, default: `«TabSeparated»`

> `--setting`    Add ClickHouse setting to query in format <key>=<value>. Accepted multiple times.

##### ctl

CHYT controller

```bash
yt chyt ctl [--address ADDRESS] command ...
```

###### Positional Arguments

> `command`

###### Named Arguments

> `--address`    controller service address

### clickhouse

ClickHouse over {{product-name}} commands

```bash
usage: yt clickhouse [-h] clickhouse_command ...
```

#### Positional Arguments

> `clickhouse_command`    Possible choices: start-clique, execute, ctl

#### Sub-commands

##### start-clique

starts a CHYT clique consisting of a given number of instances

```bash
yt clickhouse start-clique [-h] --instance-count INSTANCE_COUNT [--alias ALIAS] [--cypress-ytserver-clickhouse-path CYPRESS_YTSERVER_CLICKHOUSE_PATH]
                           [--cypress-clickhouse-trampoline-path CYPRESS_CLICKHOUSE_TRAMPOLINE_PATH] [--cypress-ytserver-log-tailer-path CYPRESS_YTSERVER_LOG_TAILER_PATH]
                           [--cypress-base-config-path CYPRESS_BASE_CONFIG_PATH] [--cpu-limit CPU_LIMIT] [--cypress-geodata-path CYPRESS_GEODATA_PATH] [--abort-existing] [--artifact-path ARTIFACT_PATH]
                           [--skip-version-compatibility-validation] [--spec SPEC] [--clickhouse-config CLICKHOUSE_CONFIG] [--memory-config MEMORY_CONFIG]
```

###### Named Arguments

> `--instance-count`    (Required)

> `--alias, --operation-alias`    Alias for clique; may be also specified via CHYT_PROXY env variable

> `--cypress-ytserver-clickhouse-path`

> `--cypress-clickhouse-trampoline-path`

> `--cypress-ytserver-log-tailer-path`

> `--cypress-base-config-path`    Default: `«//sys/clickhouse/config»`

> `--cpu-limit`

> `--cypress-geodata-path`

> `--abort-existing`    Abort existing operation under same alias

> `--artifact-path`    path for artifact directory; by default equals to //sys/clickhouse/kolkhoz/<operation_alias>

> `--skip-version-compatibility-validation`    (For developer use only)

> `--spec`    structured spec in yson format

> `--clickhouse-config`    ClickHouse configuration patch. structured clickhouse-config in yson format

> `--memory-config`    Memory configuration. structured memory-config in yson format

##### execute

executes ClickHouse query in given CHYT clique

```bash
yt clickhouse execute [-h] [--alias ALIAS] [--query QUERY] [--format FORMAT] [--setting SETTING] [query]
```

###### Positional Arguments

> `query`    Query to execute; do not specify FORMAT in query, use –format instead

###### Named Arguments

> `--alias, --operation-alias`    Alias for clique; may be also specified via CHYT_PROXY env variable

> `--query`    Query to execute; do not specify FORMAT in query, use –format instead

> `--format`    ClickHouse data format; refer to [https://clickhouse.tech/docs/en/interfaces/formats/](https://clickhouse.tech/docs/en/interfaces/formats/); default is TabSeparated, default: `«TabSeparated»`

> `--setting`    Add ClickHouse setting to query in format <key>=<value>. Accepted multiple times.

##### ctl

CHYT controller

```bash
yt clickhouse ctl [--address ADDRESS] command ...
```

###### Positional Arguments

> `command`

###### Named Arguments

> `--address`    controller service address

### detect-porto-layer

```bash
usage: yt detect-porto-layer [-h] [--params PARAMS]
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

### execute

. execute your command

```bash
usage: yt execute [-h] [--params PARAMS] command_name execute_params
```

#### Positional Arguments

> `command_name`

> `execute_params`    structured execute_params in yson format

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

### execute-batch

executes requests in parallel as one batch request

```bash
usage: yt execute-batch [-h] [--params PARAMS] requests [requests ...]
```

#### Positional Arguments

> `requests`    Request description

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

### explain-id

```bash
usage: yt explain-id [-h] [--params PARAMS] [--local] id
```

#### Positional Arguments

> `id`    id (GUID like string)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--local`    Do not gather info from cluster

### flow

YT Flow commands

```bash
usage: yt flow [-h] flow_command ...
```

#### Positional Arguments

> `flow_command`    Possible choices: start-pipeline, stop-pipeline, pause-pipeline, get-pipeline-spec, set-pipeline-spec, remove-pipeline-spec, get-pipeline-dynamic-spec, set-pipeline-dynamic-spec, remove-pipeline-dynamic-spec

#### Sub-commands

##### start-pipeline

start {{product-name}} Flow pipeline. Start {{product-name}} Flow pipeline

```bash
yt flow start-pipeline [-h] [--pipeline-path PIPELINE_PATH] [pipeline_path]
```

###### Positional Arguments

> `pipeline_path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

###### Named Arguments

> `--pipeline-path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

##### stop-pipeline

stop {{product-name}} Flow pipeline. Stop {{product-name}} Flow pipeline

```bash
yt flow stop-pipeline [-h] [--pipeline-path PIPELINE_PATH] [pipeline_path]
```

###### Positional Arguments

> `pipeline_path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

###### Named Arguments

> `--pipeline-path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

##### pause-pipeline

pause {{product-name}} Flow pipeline. Pause {{product-name}} Flow pipeline

```bash
yt flow pause-pipeline [-h] [--pipeline-path PIPELINE_PATH] [pipeline_path]
```

###### Positional Arguments

> `pipeline_path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

###### Named Arguments

> `--pipeline-path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

##### get-pipeline-spec

get {{product-name}} Flow pipeline spec. Get {{product-name}} Flow pipeline spec

```bash
yt flow get-pipeline-spec [-h] [--pipeline-path PIPELINE_PATH] [--format FORMAT] [--spec-path SPEC_PATH] [pipeline_path]
```

###### Positional Arguments

> `pipeline_path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

###### Named Arguments

> `--pipeline-path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md), default: `«<format=pretty>yson»`

> `--spec-path`    Path to part of the spec

##### set-pipeline-spec

set {{product-name}} Flow pipeline spec. Set {{product-name}} Flow pipeline spec

```bash
yt flow set-pipeline-spec [-h] [--pipeline-path PIPELINE_PATH] [--format FORMAT] [--expected-version EXPECTED_VERSION] [--force] [--spec-path SPEC_PATH] [--value VALUE] [pipeline_path] [value]
```

###### Positional Arguments

> `pipeline_path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `value`    new spec attribute value

###### Named Arguments

> `--pipeline-path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md), default: `«yson»`

> `--expected-version`    Pipeline spec expected version

> `--force`    Set spec even if pipeline is paused

> `--spec-path`    Path to part of the spec

> `--value`    new spec attribute value

##### remove-pipeline-spec

remove {{product-name}} Flow pipeline spec. Remove {{product-name}} Flow pipeline spec

```bash
yt flow remove-pipeline-spec [-h] [--pipeline-path PIPELINE_PATH] [--format FORMAT] [--expected-version EXPECTED_VERSION] [--force] [--spec-path SPEC_PATH] [pipeline_path]
```

###### Positional Arguments

> `pipeline_path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

###### Named Arguments

> `--pipeline-path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md), default: `«<format=pretty>yson»`

> `--expected-version`    Pipeline spec expected version

> `--force`    Remove spec even if pipeline is paused

> `--spec-path`    Path to part of the spec

##### get-pipeline-dynamic-spec

get {{product-name}} Flow pipeline dynamic spec. Get {{product-name}} Flow pipeline dynamic spec

```bash
yt flow get-pipeline-dynamic-spec [-h] [--pipeline-path PIPELINE_PATH] [--format FORMAT] [--spec-path SPEC_PATH] [pipeline_path]
```

###### Positional Arguments

> `pipeline_path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

###### Named Arguments

> `--pipeline-path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md), default: `«<format=pretty>yson»`

> `--spec-path`    Path to part of the spec

##### set-pipeline-dynamic-spec

set {{product-name}} Flow pipeline dynamic spec. Set {{product-name}} Flow pipeline dynamic spec

```bash
yt flow set-pipeline-dynamic-spec [-h] [--pipeline-path PIPELINE_PATH] [--format FORMAT] [--expected-version EXPECTED_VERSION] [--spec-path SPEC_PATH] [--spec SPEC] [pipeline_path] [spec]
```

###### Positional Arguments

> `pipeline_path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `spec`    new spec attribute value

###### Named Arguments

> `--pipeline-path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md), default: `«yson»`

> `--expected-version`    Pipeline spec expected version

> `--spec-path`    Path to part of the spec

> `--spec`    new spec attribute value

##### remove-pipeline-dynamic-spec

remove {{product-name}} Flow pipeline dynamic spec. Remove {{product-name}} Flow pipeline dynamic spec

```bash
yt flow remove-pipeline-dynamic-spec [-h] [--pipeline-path PIPELINE_PATH] [--format FORMAT] [--expected-version EXPECTED_VERSION] [--spec-path SPEC_PATH] [pipeline_path]
```

###### Positional Arguments

> `pipeline_path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

###### Named Arguments

> `--pipeline-path`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md), default: `«<format=pretty>yson»`

> `--expected-version`    Pipeline spec expected version

> `--spec-path`    Path to part of the spec

### generate-timestamp

generates timestamp

```bash
usage: yt generate-timestamp [-h] [--params PARAMS]
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

### get-features

retrieves supported cluster features (data types, codecs etc.). Get cluster features (types, codecs etc.)

```bash
usage: yt get-features [-h] [--params PARAMS] [--format FORMAT]
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--format`    response or input format: yson or json, for example: «<format=binary>yson». See also: [Formats](../../../user-guide/storage/formats.md)


{% if audience == "internal" %}
### idm

IDM-related commands

```bash
usage: yt idm [-h] (--path PATH | --account ACCOUNT | --bundle TABLET_CELL_BUNDLE | --group GROUP | --pool POOL) [--pool-tree POOL_TREE] [--address ADDRESS] idm_command ...
```

#### Positional Arguments

> `idm_command`    Possible choices: show, request, revoke, copy

#### Named Arguments

> `--path`    Cypress node path

> `--account`    Account name

> `--bundle`    Tablet cell bundle name

> `--group`    {{product-name}} group name

> `--pool`    Pool name

> `--pool-tree`    Pool tree name

> `--address`    IDM integration service address

#### Sub-commands

##### show

Show IDM information

```bash
yt idm show [-h] [--immediate]
```

###### Named Arguments

> `--immediate, -i`    Show only immediate IDM information (not inherited)

##### request

Request IDM role

```bash
yt idm request [-h] [--responsibles [RESPONSIBLES ...]] [--read-approvers [READ_APPROVERS ...]] [--auditors [AUDITORS ...]] [--set-inherit-acl | --unset-inherit-acl]
               [--set-inherit-responsibles | --unset-inherit-responsibles] [--set-boss-approval | --unset-boss-approval] [--members [MEMBERS ...]] [--subjects [SUBJECTS ...]] [--comment COMMENT] [--dry-run]
               [--permissions PERMISSIONS]
```

###### Named Arguments

> `--responsibles, -r`    User logins space separated, default: `[]`

> `--read-approvers, -a`    User logins space separated, default: `[]`

> `--auditors, -u`    User logins space separated, default: `[]`

> `--set-inherit-acl`    Enable ACL inheritance

> `--unset-inherit-acl`    Disable ACL inheritance

> `--set-inherit-responsibles`    Enable inheritance of responsibles, read approvers, auditors, boss_approval

> `--unset-inherit-responsibles`    Disable inheritance of responsibles, read approvers, auditors, boss_approval

> `--set-boss-approval`    Enable boss approval requirement for personal roles

> `--unset-boss-approval`    Disable boss approval requirement for personal roles

> `--members, -m`    Members list to remove or add. Only for groups, default: `[]`

> `--subjects, -s`    Space separated user logins or staff/ABC groups like idm-group:ID or tvm apps like tvm-app:ID, default: `[]`

> `--comment`    Comment for the role

> `--dry-run`    Do not make real changes

> `--permissions, -p`    Permissions like: R - read; RW - read, write, remove; M - mount, U - use, default: `[]`

##### revoke

Revoke IDM role

```bash
yt idm revoke [-h] [--responsibles [RESPONSIBLES ...]] [--read-approvers [READ_APPROVERS ...]] [--auditors [AUDITORS ...]] [--set-inherit-acl | --unset-inherit-acl]
              [--set-inherit-responsibles | --unset-inherit-responsibles] [--set-boss-approval | --unset-boss-approval] [--members [MEMBERS ...]] [--subjects [SUBJECTS ...]] [--comment COMMENT] [--dry-run]
              [--permissions PERMISSIONS] [--revoke-all-roles]
```

###### Named Arguments

> `--responsibles, -r`    User logins space separated, default: `[]`

> `--read-approvers, -a`    User logins space separated, default: `[]`

> `--auditors, -u`    User logins space separated, default: `[]`

> `--set-inherit-acl`    Enable ACL inheritance

> `--unset-inherit-acl`    Disable ACL inheritance

> `--set-inherit-responsibles`    Enable inheritance of responsibles, read approvers, auditors, boss_approval

> `--unset-inherit-responsibles`    Disable inheritance of responsibles, read approvers, auditors, boss_approval

> `--set-boss-approval`    Enable boss approval requirement for personal roles

> `--unset-boss-approval`    Disable boss approval requirement for personal roles

> `--members, -m`    Members list to remove or add. Only for groups, default: `[]`

> `--subjects, -s`    Space separated user logins or staff/ABC groups like idm-group:ID or tvm apps like tvm-app:ID, default: `[]`

> `--comment`    Comment for the role

> `--dry-run`    Do not make real changes

> `--permissions, -p`    Permissions like: R - read; RW - read, write, remove; M - mount, U - use, default: `[]`

> `--revoke-all-roles`    Revoke all IDM roles

##### copy

Copy IDM permissions

```bash
yt idm copy [-h] [--immediate] [--erase] [--dry-run] destination
```

###### Positional Arguments

> `destination`    Destination object

###### Named Arguments

> `--immediate, -i`    Only copy immediate IDM permissions

> `--erase, -e`    Erase all existing permissions from destination object

> `--dry-run`    Do not make real changes

{% endif %}


### jupyt

Jupyter over {{product-name}} commands

```bash
usage: yt jupyt [-h] jupyter_command ...
```

#### Positional Arguments

> `jupyter_command`    Possible choices: ctl

#### Sub-commands

##### ctl

JUPYT controller

```bash
yt jupyt ctl [--address ADDRESS] command ...
```

###### Positional Arguments

> `command`

###### Named Arguments

> `--address`    controller service address

### run-command-with-lock

. Run command under lock

```bash
usage: yt run-command-with-lock [-h] [--params PARAMS] [--shell] [--poll-period POLL_PERIOD] [--conflict-exit-code CONFLICT_EXIT_CODE] [--set-address] [--address-path ADDRESS_PATH] [--recursive]
                                path command [command ...]
```

#### Positional Arguments

> `path`    Path to the lock in Cypress

> `command`    Command to execute

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--shell`    Run command in subshell

> `--poll-period`    Poll period for command process in seconds, default: `1.0`

> `--conflict-exit-code`    Exit code in case of lock conflict, default: `1`

> `--set-address`    Set address of current host (in lock attribute by default)

> `--address-path`    Path to set host address

> `--recursive`    Create lock path recursively

### run-compression-benchmarks

Gets input table and recompresses it in all available codecs.

For each codec prints compression ratio, cpu_write and cpu_read.

```bash
usage: yt run-compression-benchmarks [-h] [--params PARAMS] [--table TABLE] [--all-codecs] [--sample-size SAMPLE_SIZE] [--format {json,csv}] [--max-operations MAX_OPERATIONS] [--time-limit-sec TIME_LIMIT_SEC]
                                     [table]
```

#### Positional Arguments

> `table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--table`    address in Cypress. See also: [YPATH](../../../user-guide/storage/ypath.md)

> `--all-codecs`    benchmark every level of codecs with levels

> `--sample-size`    approximate table’s sample fragment size in bytes, default: `1000000000`

> `--format`    Possible choices: json, csv

> output format, default: `«json»`

> `--max-operations`    max count of parallel operations, default: `10`

> `--time-limit-sec`    time limit for one operation in seconds, default: `200`

### show-default-config

returns default configuration of python API

```bash
usage: yt show-default-config [-h] [--params PARAMS] [--with-remote-patch] [--only-remote-patch]
```

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--with-remote-patch`    with patch from cluster

> `--only-remote-patch`    show only patch from cluster

### show-spec

shows available spec options of the operation

```bash
usage: yt show-spec [-h] [--params PARAMS] operation
```

#### Positional Arguments

> `operation`    operation type

#### Named Arguments

> `--params`    specify additional params. structured params in yson format


{% if audience == "internal" %}
### sky-share

shares table on cluster via skynet

```bash
usage: yt sky-share [-h] [--params PARAMS] [--cluster CLUSTER] [--key-column KEY_COLUMNS] [--enable-fastbone] path
```

#### Positional Arguments

> `path`    table path

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--cluster`    cluster name, by default it is derived from proxy url

> `--key-column`    create a separate torrent for each unique key and print rbtorrent list in JSON format Accepted multiple times.

> `--enable-fastbone`    download over fastbone if all necessary firewall rules are present
{% endif %}


### spark

Spark over {{product-name}} commands

```bash
usage: yt spark [-h] spark_command ...
```

#### Positional Arguments

> `spark_command`    Possible choices: start-cluster, find-cluster

#### Sub-commands

##### start-cluster

start Spark Standalone cluster in {{product-name}} Vanilla Operation. See [About Spark](../../../user-guide/data-processing/spyt/overview.md) section. Start Spark Standalone cluster in {{product-name}} Vanilla Operation

```bash
yt spark start-cluster [-h] --spark-worker-core-count SPARK_WORKER_CORE_COUNT --spark-worker-memory-limit SPARK_WORKER_MEMORY_LIMIT --spark-worker-count SPARK_WORKER_COUNT
                       [--spark-worker-timeout SPARK_WORKER_TIMEOUT] [--operation-alias OPERATION_ALIAS] [--discovery-path DISCOVERY_PATH] [--pool POOL] [--spark-worker-tmpfs-limit SPARK_WORKER_TMPFS_LIMIT]
                       [--spark-master-memory-limit SPARK_MASTER_MEMORY_LIMIT] [--spark-history-server-memory-limit SPARK_HISTORY_SERVER_MEMORY_LIMIT] [--dynamic-config-path DYNAMIC_CONFIG_PATH]
                       [--operation-spec OPERATION_SPEC]
```

###### Named Arguments

> `--spark-worker-core-count`    (Required) Number of cores that will be available on Spark worker

> `--spark-worker-memory-limit`    (Required) Amount of memory that will be available on Spark worker

> `--spark-worker-count`    (Required) Number of Spark workers

> `--spark-worker-timeout`    Worker timeout to wait master start, default: `«5m»`

> `--operation-alias`    Alias for the underlying {{product-name}} operation

> `--discovery-path`    Cypress path for discovery files and logs, the same path must be used in find-spark-cluster. SPARK_YT_DISCOVERY_PATH env variable is used by default

> `--pool`    Pool for the underlying {{product-name}} operation

> `--spark-worker-tmpfs-limit`    Limit of tmpfs usage per Spark worker, default: `«150G»`

> `--spark-master-memory-limit`    Memory limit on Spark master, default: `«2G»`

> `--spark-history-server-memory-limit`    Memory limit on Spark History Server, default: `«8G»`

> `--dynamic-config-path`    {{product-name}} path of dynamic config, default: `«//sys/spark/bin/releases/spark-launch-conf»`

> `--operation-spec`    {{product-name}} Vanilla Operation spec. structured operation-spec in yson format, default: `{"annotations": {"is_spark": True}, "max_failed_job_count": 5, "max_stderr_count": 150}`

##### find-cluster

print Spark urls. Print URLs of running Spark cluster

```bash
yt spark find-cluster [-h] [--discovery-path DISCOVERY_PATH]
```

###### Named Arguments

> `--discovery-path`    Cypress path for discovery files and logs, the same path must be used in start-spark-cluster. SPARK_YT_DISCOVERY_PATH env variable is used by default

### spyt

Spark over {{product-name}} commands

```bash
usage: yt spyt [-h] spark_command ...
```

#### Positional Arguments

> `spark_command`    Possible choices: start-cluster, find-cluster

#### Sub-commands

##### start-cluster

start Spark Standalone cluster in {{product-name}} Vanilla Operation. See [About Spark](../../../user-guide/data-processing/spyt/overview.md) section. Start Spark Standalone cluster in {{product-name}} Vanilla Operation

```bash
yt spyt start-cluster [-h] --spark-worker-core-count SPARK_WORKER_CORE_COUNT --spark-worker-memory-limit SPARK_WORKER_MEMORY_LIMIT --spark-worker-count SPARK_WORKER_COUNT
                      [--spark-worker-timeout SPARK_WORKER_TIMEOUT] [--operation-alias OPERATION_ALIAS] [--discovery-path DISCOVERY_PATH] [--pool POOL] [--spark-worker-tmpfs-limit SPARK_WORKER_TMPFS_LIMIT]
                      [--spark-master-memory-limit SPARK_MASTER_MEMORY_LIMIT] [--spark-history-server-memory-limit SPARK_HISTORY_SERVER_MEMORY_LIMIT] [--dynamic-config-path DYNAMIC_CONFIG_PATH]
                      [--operation-spec OPERATION_SPEC]
```

###### Named Arguments

> `--spark-worker-core-count`    (Required) Number of cores that will be available on Spark worker

> `--spark-worker-memory-limit`    (Required) Amount of memory that will be available on Spark worker

> `--spark-worker-count`    (Required) Number of Spark workers

> `--spark-worker-timeout`    Worker timeout to wait master start, default: `«5m»`

> `--operation-alias`    Alias for the underlying {{product-name}} operation

> `--discovery-path`    Cypress path for discovery files and logs, the same path must be used in find-spark-cluster. SPARK_YT_DISCOVERY_PATH env variable is used by default

> `--pool`    Pool for the underlying {{product-name}} operation

> `--spark-worker-tmpfs-limit`    Limit of tmpfs usage per Spark worker, default: `«150G»`

> `--spark-master-memory-limit`    Memory limit on Spark master, default: `«2G»`

> `--spark-history-server-memory-limit`    Memory limit on Spark History Server, default: `«8G»`

> `--dynamic-config-path`    {{product-name}} path of dynamic config, default: `«//sys/spark/bin/releases/spark-launch-conf»`

> `--operation-spec`    {{product-name}} Vanilla Operation spec. structured operation-spec in yson format, default: `{"annotations": {"is_spark": True}, "max_failed_job_count": 5, "max_stderr_count": 150}`

##### find-cluster

print Spark urls. Print URLs of running Spark cluster

```bash
yt spyt find-cluster [-h] [--discovery-path DISCOVERY_PATH]
```

###### Named Arguments

> `--discovery-path`    Cypress path for discovery files and logs, the same path must be used in start-spark-cluster. SPARK_YT_DISCOVERY_PATH env variable is used by default

### transfer-account-resources

transfers resources between accounts

```bash
usage: yt transfer-account-resources [-h] [--params PARAMS] [--source-account SOURCE_ACCOUNT] [--destination-account DESTINATION_ACCOUNT] [--resource-delta RESOURCE_DELTA] [source_account] [destination_account]
```

#### Positional Arguments

> `source_account`

> `destination_account`

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--source-account, --src`

> `--destination-account, --dst`

> `--resource-delta`    structured resource-delta in yson format

### transfer-pool-resources

transfers resources between pools

```bash
usage: yt transfer-pool-resources [-h] [--params PARAMS] [--source-pool SOURCE_POOL] [--destination-pool DESTINATION_POOL] [--pool-tree POOL_TREE] [--resource-delta RESOURCE_DELTA]
                                  [source_pool] [destination_pool] [pool_tree]
```

#### Positional Arguments

> `source_pool`

> `destination_pool`

> `pool_tree`

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--source-pool, --src`

> `--destination-pool, --dst`

> `--pool-tree`

> `--resource-delta`    structured resource-delta in yson format

### transform

transforms source table to destination table writing data with given compression and erasure codecs

```bash
usage: yt transform [-h] [--params PARAMS] [--src SOURCE_TABLE] [--dst DESTINATION_TABLE] [--erasure-codec ERASURE_CODEC] [--compression-codec COMPRESSION_CODEC] [--optimize-for OPTIMIZE_FOR]
                    [--desired-chunk-size DESIRED_CHUNK_SIZE] [--check-codecs] [--spec SPEC]
                    [source_table] [destination_table]
```

#### Positional Arguments

> `source_table`    source table

> `destination_table`    destination table (if not specified source table will be overwritten)

#### Named Arguments

> `--params`    specify additional params. structured params in yson format

> `--src`    source table

> `--dst`    destination table (if not specified source table will be overwritten)

> `--erasure-codec`    desired erasure codec for table

> `--compression-codec`    desired compression codec for table

> `--optimize-for`    desired chunk format for table. Possible values: [«scan», «lookup»]

> `--desired-chunk-size`    desired chunk size in bytes

> `--check-codecs`    check if table already has proper codecs before transforming

> `--spec`    structured spec in yson format
