---
title: "Installing the MCP Server | {{product-name}}"
description: "How to install the ytsaurus-mcp package and configure the {{product-name}} MCP server to work with the Cline AI assistant in Visual Studio Code"
---

# Installing the {{product-name}} MCP Server

This guide shows you how to install the `ytsaurus-mcp` package and configure the {{product-name}} MCP server to work with an AI assistant. We use [Cline](https://marketplace.visualstudio.com/items?itemName=saoudrizwan.claude-dev) in Visual Studio Code as an example. The {{product-name}} MCP server follows an open standard and works with any MCP clients, such as Cursor, Windsurf, Claude Desktop, Roo Code, and others. You’ll configure them the same way.

To learn about the server’s capabilities and available tools, see the section [Working with the MCP Server](../../user-guide/ai/mcp-server.md).

## Prerequisites {#prerequisites}

Before you start, prepare the following:

- Python 3.10 or later.
- A token to access the {{product-name}} cluster. To learn how to issue a token, see the section [Managing Tokens](../../user-guide/storage/auth#token-management). The token is usually stored in the `~/.yt/token` file.
- An MCP‑compatible AI assistant with an attached LLM model: Cline in Visual Studio Code, Cursor, Claude Desktop. The assistant won’t respond without an attached model, so you’ll connect the model in [Step 2](#configure-cline).

## Step 1. Install the Package {#install-package}

Install the `ytsaurus-mcp` package:

```bash
pip3 install ytsaurus-mcp
```

On recent Python builds, such as 3.14, installing with the system `pip3` might fail while building dependencies. If this happens, install the package using `python3 -m pip`:

```bash
python3 -m pip install ytsaurus-mcp
```

Next, find the absolute path to the executable file—you’ll need it in the `command` parameter of the MCP client configuration. Run the command:

{% list tabs group=defaultTabsGroup-ouitjm5w %}

- macOS / Linux

  ```bash
  which mcp_yt_server
  ```

- Windows PowerShell

  ```powershell
  (Get-Command mcp_yt_server).Source
  ```

{% endlist %}

The command returns the absolute path to the executable file, for example:

```bash
$ which mcp_yt_server

/usr/local/bin/mcp_yt_server
```

Write down the path you get—you’ll use it in [Step 2](#configure-cline).

Make sure the server is installed correctly. To do this, display the list of available tools—the command prints them and then exits:

```bash
$ mcp_yt_server --show-tools
```

{% cut "Example output" %}

```text
Tools:
- list_dir (ListDir)
- find (Search)
- get_attributes_account (GetAttributes)
- get_attributes_account_limits_disk (GetAttributes)
- get_attributes_bundle (GetAttributes)
- get_attributes_pool (GetAttributes)
- check_is_paths_exist (CheckIsPathsExists)
- common_client_get_table_schema (CommonCypress)
- common_client_read_table (CommonCypress)
- common_client_sample_static_table (CommonCypress)
- common_client_infer_table_schema (CommonCypress)
- common_client_whoami (CommonCypress)
- check_permission (CheckPermissions)
- get_account_property (AccountProperty)
- get_proxy (GetProxy)
```

{% endcut %}

The server isn’t tied to a specific cluster: the cluster name is passed in each request to a tool. See [Step 3](#check) to learn how to specify the cluster in a request to the assistant.

## Step 2. Configure an MCP Client Using Cline as an Example {#configure-cline}

The configuration has three parts. First, you install the Cline extension, then you attach an LLM model, and only then you configure the MCP server itself. Without an LLM model, the assistant won’t respond or call tools.

### 2.1. Install the Cline Extension {#install-cline}

1. Install the [Cline](https://marketplace.visualstudio.com/items?itemName=saoudrizwan.claude-dev) extension from the Visual Studio Code marketplace or from the VS Code extensions panel.
1. Restart Visual Studio Code.

### 2.2. Attach an LLM Model {#configure-llm}

{% note warning "The assistant doesn’t work without an LLM model" %}

Connect an LLM provider before configuring the MCP server. Otherwise, the assistant won’t respond to requests or call tools.

{% endnote %}

To attach an LLM model:

1. In the sidebar of Visual Studio Code, open the Cline plugin.
1. Click the gear icon **Settings**.
1. Enter the provider’s API key and select a model for planning and actions.
1. Click **Done**.

### 2.3. Add the MCP Server {#add-mcp-server}

To add the MCP server:

1. In the sidebar of Visual Studio Code, open the Cline plugin.
1. Click the wrench icon—the **Customize** tab opens.
1. In the window that opens, select **MCP**—this is where you set up MCP servers.
1. Click **Edit Configuration**—the `cline_mcp_settings.json` file opens. Add the configuration. Substitute the absolute paths you got in [Step 1](#install-package):

   - `command`—the absolute path to the `mcp_yt_server` executable file from the `which mcp_yt_server` output;
   - `--yt-token-file`—the absolute path to the token file. This argument is optional if the token is set in the `MCP_YT_TOKEN` environment variable. See the section [Advanced Settings](#advanced) for more details.

   {% cut "Example configuration" %}
   
   ```json
   {
     "mcpServers": {
       "local-yt-server-python": {
         "env": {},
         "args": [
           "--log-file=/tmp/out.log",
           "--log-level=DEBUG",
           "--yt-token-file=/Users/ivan/.yt/token"
         ],
         "command": "/usr/local/bin/mcp_yt_server",
         "disabled": false,
         "alwaysAllow": [],
         "type": "stdio"
       }
     }
   }
   ```

   {% endcut %}

   Only change `command` and `--yt-token-file`—leave the other fields as they are. The `--log-file` and `--log-level` arguments are optional: they enable debug logging and are only needed to diagnose issues. See the section [Advanced Settings](#advanced) to learn about the server’s additional capabilities.

1. Save the file and click **Done**.
1. Make sure the server appears in the list of servers on the **Installed** tab and shows a green indicator—this means it started successfully. If the indicator is red, check the paths to the executable file and token in the configuration.

## Step 3. Check That It Works {#check}

Make sure that after adding the server in [Step 2](#configure-cline), it shows a green indicator in the **Installed** list. Then, send the assistant a simple request, for example:

“Show the contents of the `//home` directory on the `<cluster-name>` cluster”

The cluster is a parameter for each tool call, not a global server setting. So, you specify the cluster name in the text of the request to the assistant, not in the `mcpServers` configuration.

If the assistant returns a list of objects, the server is working correctly. If you see an authorization error, check the path to the token file in the configuration and make sure the token is valid. See the section [Managing Tokens](https://ytsaurus.tech/docs/ru/user-guide/storage/auth#token-management) for more details.

See the section [MCP Server Methods](../../user-guide/ai/mcp-server.md) for the full list of available tools and example requests.

## Advanced Settings {#advanced}

You don’t need these parameters for basic operation—configure them only if necessary.

{% note info "The server supports read‑only operations only" %}

The {{product-name}} MCP server is limited to reading data and configs and doesn’t modify or delete objects.

{% endnote %}

### Specifying the Token via an Environment Variable {#token-env}

Instead of the `--yt-token-file` token file, you can pass the token via the `MCP_YT_TOKEN` environment variable in the `env` field of the configuration:

```json
"env": { "MCP_YT_TOKEN": "<token>" }
```

The server checks token sources in the following order:

1. The `MCP_YT_TOKEN` environment variable—highest priority.
1. The `--yt-token-file` argument.
1. The `yt` client’s default value—the `~/.yt/token` file or the `YT_TOKEN` variable.

The `MCP_YT_TOKEN` variable is read by the MCP server itself. The `YT_TOKEN` variable belongs to the `ytsaurus-client` library and is used only as a fallback when neither `MCP_YT_TOKEN` nor `--yt-token-file` is set.

### Selecting a Group of Tools {#tools-groups}

By default, all tools from three groups are enabled: `common`, `account`, and `admin`. To enable only specific groups, specify the corresponding flags in `args`:

#|
|| **Flag** | **Tools Included** ||
|| `--tools-common` | General tools for working with paths, tables, and clusters ||
|| `--tools-account` | Tools for working with accounts ||
|| `--tools-admin` | Tools for administrators ||
|#

If you specify at least one `--tools-*` flag, only the selected groups are enabled. If you don’t specify any flag, all three groups are enabled.

### Transport {#transport}

By default, the server uses the `stdio` transport—this mode is used in Cline and most local MCP clients. For network access, use `sse`:

```json
"args": ["--server-transport=sse"]
```

### Logging {#logging}

To diagnose issues, enable debug logging:

#|
|| **Argument** | **Description** ||
|| `--log-file=<path>` | Path to the log file, for example `/tmp/mcp_yt_server.log` ||
|| `--log-level=<level>` | Log verbosity level: `INFO`, `ERROR`, or `DEBUG` ||
|#

Example:

```json
"args": [
  "--log-file=/tmp/mcp_yt_server.log",
  "--log-level=DEBUG"
]
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
