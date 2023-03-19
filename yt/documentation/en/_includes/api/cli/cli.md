# Overview

This section contains information about the {{product-name}} command line: [description](#description), [installation methods](../../../api/cli/install.md), and [use cases](../../../api/cli/examples.md).

## Description { #description }

The {{product-name}} CLI (Command Line Interface) is the most convenient way to interact with the {{product-name}} system from the console. The CLI fully implements the features of all the [commands](../../../api/commands.md) supported by {{product-name}}. The name of the command is specified as the first argument followed by its options. To get help on the CLI, just run the command:

```bash
yt --help
yt read --help
```

To work with a cluster, you need to get an access token and put it in the `~/.yt/token` file or the `YT_TOKEN` environment variable.

For more information about the tokens and authentication, see the [Authentication](../../../user-guide/storage/auth.md) section.

In addition to the access token, you need to specify the cluster to work with. To do this, you can specify the proxy server name in the `YT_PROXY` environment variable: `export YT_PROXY=<cluster-name>` or pass the proxy server name to each command individually via the `proxy` option, such as `--proxy <cluster-name>`.

Below are some features of the CLI:

- All underscores in command and option names were replaced by hyphens.

- `input_table_paths` and `output_table_paths` are specified via the `--src` and `--dst` options.

- To specify multiple columns for sort or reduce, pass the `sort-by` or `reduce-by` option several times, for example, `--sort-by a --sort-by b` will sort by composite key `a b`.

   {% note warning "Attention!" %}

   If you write `--sort-by a,b`, sorting by one column named `a,b` will be performed.

   {% endnote %}

- The `write` command writes data chunk-by-chunk and under the transaction.

- There is a `find` command (implemented on top of get) which partially replicates the unix find utility.

- By default, the `list` command (without specifying the `--format` parameter) just enters a list of node elements, each on a separate string. It also has the `-l` option that will enter useful information about the nodes along with their names.

- By default, any files uploaded into {{product-name}} are uploaded without the executable flag. If you want to upload a binary file that will run in a job, you must specify the `--executable` option when uploading or set the `/@executable=true` attribute after uploading.

- [The formats of the data](../../../user-guide/storage/formats.md) received at the command input and appearing at the command output can be specified through the following environment variables:
   - `YT_ARGUMENTS_FORMAT`: Manages the format for the values of the options of the `--spec`, `--attributes` type. The default value is `yson`.
   - `YT_STRUCTURED_DATA_FORMAT`: Manages the format of the get/set command. The default value is `yson`.
   - `YT_TABULAR_DATA_FORMAT`: Manages the format of reading/writing tables, as well as starting operation jobs. There is no default value, the variable value must be set explicitly or the `--format` option must be specified.

- There is a method to create aliases for commands. Aliases are specified in the `~/.yt/aliases` file in `name=opts` format. Example:

   ```bash
   mkdir=create map_node
   ls=list
   rm=remove
   mv=move
   cp=copy
   ```

- Auto-completion of binary file commands and paths for bash is supported.

