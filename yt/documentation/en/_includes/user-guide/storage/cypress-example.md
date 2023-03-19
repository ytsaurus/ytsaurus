# Working with the meta information tree

This section includes the most popular [Cypress](../../../user-guide/storage/cypress.md) command-line (CLI) code examples. For a more detailed description of the commands and their options, please see the Commands section.

## List { #list }

This command displays a list of all keys for the `<path>` node. The node must have type map_node.

### Description of the list command

```bash
yt list [-l] [--format FORMAT] [--attribute ATTRIBUTES]
[--max-size MAX_SIZE] [--absolute] <path>
```

The `-l` option displays additional attributes (node type, account, amount of space used, most recent change date).

### Calling the list command

```bash
yt list -l --max-size 3 //home/dev
string_node   dev            0 2018-12-22 09:30 some_key
  map_node    dev         1248 2019-01-22 13:14 ifs
  map_node    dev  69856453509 2019-02-26 09:05 zuci42
WARNING: list is incomplete, check --max-size option
```

The `--format` option defines the output data format. Supported formats are `json` and `yson`. For more information, see [Formats](../../../user-guide/storage/formats.md). The `--format` option is incompatible with `-l`.

### Calling list with the format option

```bash
yt list --format json //home/dev
["some_key","ifs","zuci42","data_com","andozer","renadeen","is","babenko","ivanashevi","alximik","tsufiev","alxsev","t","psushin"]
```

You can use the `--attribute` option (which you can specify more than once) to request additional attributes for the nodes referenced by the listed keys. `--attribute account`, for instance, will add the account to which the node belongs to each key. You can only use `--attribute` together with `--format`.

### Calling list with the attribute option

```bash
yt list --attribute account --attribute owner --format "<format=pretty>json" --max-size 3 //home/dev
{
    "$attributes": {
        "incomplete": true
    },
    "$value": [
        {
            "$attributes": {
                "account": "dev",
                "owner": "babenko"
            },
            "$value": "some_key"
        },
        {
            "$attributes": {
                "account": "dev",
                "owner": "ifsmirnov"
            },
            "$value": "ifs"
        },
        {
            "$attributes": {
                "account": "dev",
                "owner": "renadeen"
            },
            "$value": "zuci42"
        }
    ]
}
```

The `--max-size` option helps limit the number of elements returned. This will display arbitrary elements.

### Calling list with the max-size option

```bash
yt list --max-size 3 //home/dev
some_key
ifs
zuci42
WARNING: list is incomplete, check --max-size option
```

The `--absolute` option will display absolute paths.

### Calling list with the absolute option

```bash
yt list --absolute --max-size 3 //home/dev
//home/dev/some_key
//home/dev/ifs
//home/dev/zuci42
WARNING: list is incomplete, check --max-size option
```

## Create { #create }

This command creates a node of the type specified in `<type>` in the path specified in `<path>`. The command returns the identifier of the object created. Supported objects are listed in the [Objects](../../../user-guide/storage/objects.md) section.

### Description of create command

```bash
yt create [-r, --recursive] [-i,--ignore-existing] [-l,--lock-existing] [-f, --force] [--attributes ATTRIBUTES] <type> <path>
```

The `-r, --recursive` option creates all the intermediate nodes of type `map_node`.

### Calling create with recursive option

```bash
yt create --recursive map_node //home/dev/test1/test2/test3
127c1-388f86-3fe012f-6f994b03
```

The `-i, --ignore-existing` option will not recreate a specified node from scratch if it already exists. In addition, the existing and the requested nodes must be of the same type, otherwise the request will return an error.

### Calling create without the ignore-existing option

```bash
yt create map_node //home/dev/test1/test2/test3
Node //home/dev/test1/test2/test3 already exists
```
### Calling create with the ignore-existing option

```bash
yt create --ignore-existing map_node //home/dev/test1/test2/test3
127c1-388f86-3fe012f-6f994b03
```

The `-l, --lock-existing` option is compatible with `--ignore-existing`. If indicated together, an [exclusive lock](../../../user-guide/storage/transactions.md#locks) will be taken out on the specified node even if it already exists. (In the event of a lock conflict, the command will return an error). Since actually creating a node always takes out an implicit (automatic) lock on the node, the option makes the outcome of the command more predictable when using `--ignore-existing`.

The `-f, --force` option helps force-create an object. In the event that the specified node already exists, it is deleted and replaced with a new one. In this situation, the existing node can be of any type. Re-creation changes a node's ID.

### Calling create with the force option

```bash
yt create --force map_node //home/dev/test1/test2/test3
127c1-4352be-3fe012f-6acab448
```

You can use the `--attributes` option to set the attributes of the node being created.

### Calling create with the attributes option

```bash
yt create --attributes "{test_attr=test1;}" map_node //home/dev/test1/test2/test3
127c2-1b78-3fe012f-288106fe
```
### Checking the output of create with the attributes option
```bash
yt get //home/dev/test1/test2/test3/@test_attr
"test1"
```

## Remove { #remove }

This command deletes a node in the path specified in `<path>`. If it is a composite non-empty node, the command will return an error.

### Description of remove command

```bash
yt remove [-r, --recursive] [-f, --force] <path>
```

The `-r, --recursive` option recursively deletes the entire subtree.

### Calling remove without recursive option

```bash
yt remove //home/dev/test1
Cannot remove non-empty composite node
```
### Calling remove with recursive option
```bash
yt remove --recursive //home/dev/test1
```

The `-f, --force` option enables you to ignore the fact that the specified node does not exist.

### Calling remove without the force option

```bash
yt remove //home/dev/test1
Node //home/dev has no child with key "test1"
```
### Calling remove with the force option
```bash
yt remove --force //home/dev/test1
```

## Exists { #exists }

This command helps check for the existence of the `<path>` node passed in to it.

### Description of the exists command

```bash
yt exists <path>
```
### Calling exists on an existing node
```bash
yt exists //home/dev/test1/test2/test3
true
```
### Calling exists on a non-existent node
```bash
yt exists //home/dev/test1/test2/test4
false
```

## Get { #get }

This command displays the entire Cypress subtree found in `<path>`.

### Description of the get command

```bash
yt get [--max-size MAX_SIZE] [--format FORMAT] [--attribute ATTRIBUTES] <path>
```

`<path>` must exist.

### Calling get on a non-existent node

```bash
yt get //home/dev/test1/test2/test4
Error resolving path //home/dev/test1/test2/test4
Node //home/dev/test1/test2 has no child with key "test4"
```
### Calling get on an existing node
```bash
yt get //home/dev/test1/test2
{
    "test3" = {};
}
```
### Calling get on a node attribute
```bash
yt get //home/dev/test1/test2/@account
"dev"
```

The `--max-size` option helps limit the number of nodes returned in the case of virtual composite nodes.

{% note warning "Attention!" %}

For conventional map nodes, this option is useless.

{% endnote %}

### Calling get without the max-size option
```bash
yt get //sys/transactions/@type
"transaction_map"
```
### Calling get with the max-size option
```bash
yt get --max-size 3 //sys/transactions
<
    "incomplete" = %true;
> {
    "12814-3b9ce8-3fe0001-3b10627" = #;
    "12817-1cc292-3fe0001-fcdca56f" = #;
    "1268e-42d32d-3fe0001-d0fc5c4f" = #;
}
```

The `--format` option defines the output data format. Supported formats are `json` and `yson`. For more information, see [Formats](../../../user-guide/storage/formats.md).

### Calling get with the format option

```bash
yt get --format "<format=pretty>json" //home/dev/test1
{
    "test2": {
        "test3": {

        }
    }
}
```

Special-type objects as well as **opaque** nodes (objects with `opaque` attribute set to `%true` ) are displayed as entity. No attributes associated with subtree nodes are displayed by default. But you can request that they be displayed using the `--attribute` option in a manner similar to the `list` command.

### Calling get with the attribute option

```bash
yt get --attribute opaque //home/dev/test1/test2
<
    "opaque" = %false;
> {
    "test3" = <
        "opaque" = %false;
    > {};

```


Some nodes may return as opaque (such nodes will have no additional attributes) if the subtree being walked turns out to be too large.

## Set { #set }

The `set` command assigns the specified `value` in [YSON](../../../user-guide/storage/yson.md) format using the specified `<path>`.

### Description of the set command

```bash
yt set [--format FORMAT] [-r, --recursive] [-f, --force]<path> <value>
```
### Calling set
```bash
yt set //home/dev/test1/@some_attr test
```
### Verifying a set call
```bash
yt get //home/dev/test1/@some_attr
"test"
```

The `--format` option defines the `value` format. Supported formats are `json` and `yson`. For more information, see [Formats](../../../user-guide/storage/formats.md).

### Calling set with the format option

```bash
yt set --format json //home/dev/test1/test2/test4 '{"some_test_key":"some_test_value"}'
```
### Calling set without the format option
```bash
yt get //home/dev/test1/test2/test4
{
    "some_test_key" = "some_test_value";
}
```

The `-r, --recursive` option helps create all the non-existent intermediate nodes in a path.

### Calling set without the recursive option

```bash
yt set //home/dev/test1/test2/test3 some_test_value
Node //home/dev has no child with key "test1"
```

### Calling set with the recursive option
```bash
yt set --recursive //home/dev/test1/test2/test3 some_test_value
```

The `-f, --force` option modifies any Cypress nodes rather than just attributes and documents.

## Copy, Move { #copy_move }

This command copies/moves a node from `<source_path>` to `<destination_path>`. When copying files or tables, chunks are not physically copied, and all changes occur at the metadata level.

{% note warning "Attention!" %}

A move first copies and then deletes a source node, thereby changing the IDs of all the objects in the subtree.

{% endnote %}

### Description of copy/move command

```bash
yt copy/move [--preserve-account | --no-preserve-account]
               [--preserve-expiration-time] [--preserve-expiration-timeout]
               [--preserve-creation-time]
               [-r, --recursive] [-f, --force]
               <source_path> <destination_path>
```

The `--preserve-account` option helps keep the account of the node being copied instead of using the account of the destination node's parent folder.

The `--preserve-expiration-time` option helps keep the specified [delete time](../../../user-guide/storage/cypress.md) of the node being copied.

The `--preserve-expiration-timeout` option helps keep the specified [delete interval](../../../user-guide/storage/cypress.md) of the node being copied.

The `--preserve-creation-time` option helps keep the [create time](../../../user-guide/storage/cypress.md) of the node being copied. **For copying only.**

### Verifying an account

```bash
yt get //home/dev/test1/test_file/@account
"dev"
```
### Calling the copy command
```bash
yt copy //home/dev/test1/test_file //home/dev/test1/test2/test_file_new

yt get //home/dev/test1/test2/test_file_new/@account
"tmp"
```
### Calling copy with the preserve-account option
```bash
yt copy --preserve-account //home/dev/test1/test_file //home/dev/test1/test2/test_file_new_p

yt get //home/dev/test1/test2/test_file_new_p/@account
"dev"
```

The `-r, --recursive` option controls behavior if the intermediate nodes in `<destination_path>` do not exist. If this option is enabled, these nodes will be created recursively. Otherwise, the command will return an error.

### Calling copy without the recursive option

```bash
yt copy //home/dev/test1/test_file //home/dev/test1/test2/test3/test_file_new
Node //home/dev/test1/test2 has no child with key "test3"
```
### Calling copy with the recursive option
```bash
yt copy --recursive //home/dev/test1/test_file //home/dev/test1/test2/test3/test_file_new
```

The `-f, --force` option makes the command run even if the destination node exists. This deletes the existing node.

### Calling copy without the force option

```bash
yt copy //home/dev/test1/test_file //home/dev/test1/test2/test_file_new
Node //home/dev/test1/test2/test_file_new already exists
```
### Calling copy with the force option
```bash
yt copy --force //home/dev/test1/test_file //home/dev/test1/test2/test_file_new
```

## Link { #link }

This command creates a [symbolic link](../../../user-guide/storage/links.md) in `<link_path>` to an object found in `<target_path>`.

### Description of the link command

```bash
yt link [-r, --recursive]
          [-i, --ignore-existing] [-l, --lock-existing]
          [-f, --force]
          <target_path> <link_path>
```

The `-r, --recursive` option controls behavior if the intermediate nodes in `<link_path>` do not exist. If this option is enabled, these nodes will be created recursively. Otherwise, the command will return an error.

### Calling link without the recursive option

```bash
yt link //home/dev/test1/test_file //home/dev/test1/test2/link_test_file
Node //home/dev/test1 has no child with key "test2"
```
### Calling link with the recursive option
```bash
yt link --recursive  //home/dev/test1/test_file //home/dev/test1/test2/link_test_file
```

The `-i, --ignore-existing` option will not recreate a specified link from scratch if it already exists.

### Calling link without the ignore-existing option

```bash
yt link //home/dev/test1/test_file //home/dev/test1/test2/link_test_file
Node //home/dev/test1/test2/link_test_file already exists
```
### Calling link with the ignore-existing option
```bash
yt link --ignore-existing //home/dev/test1/test_file //home/dev/test1/test2/link_test_file
```

The `-l, --lock-existing` option guarantees that an [exclusive lock](../../../user-guide/storage/transactions.md#locks) will be taken out on the link even if it already exists. (Only useful with `--ignore-existing`; if a lock cannot be set, the command will return an error).

The `-f, --force` option makes the command run even if the destination node exists. This deletes the existing node.

## Concatenate { #concatenate }

The `concatenate` command combines data from the nodes in `<source_paths>` into a node in `<destination_path>`. All the nodes must exist and belong to the same type by being either tables or files.

info "Note"
: the data is combined at the meta information level, that is, only on the master

### Description of the concatenate command
```bash
yt concatenate --src <source_paths> --dst <destination_path>
```

{% note warning "Attention!" %}

The running time of this request is a function of the (total) number of chunks making up the source nodes.  At the same time, many client libraries have a limitation on the maximum command running time, normally on the order of tens of seconds. Therefore, concatenate is only worth using if you are certain that there are few chunks, about 10,000, in the output paths. Otherwise, the Merge operation is to be used. It will not perform better (and may take many minutes for tables of millions of chunks) but a client will track its operation by periodically requesting its status which removes virtually all constraints on the time it takes to run.

{% endnote %}
