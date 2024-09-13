# Proxy

This section contains information about the different interfaces for working with the {{product-name}} system: [HTTP](#http_proxy) and [RPC](#rpc_proxy) proxies.

## HTTP proxy { #http_proxy }

**HTTP proxy** is a universal language-agnostic interface to work with {{product-name}}.

{% note warning "Warning" %}

The low-level HTTP API is not a good choice for writing simple and reliable application code. You need to set up proper logging of queries and their correlation IDs labeling, balance the load on the proxy, acquire additional explicit locks on tables, handle errors, repeat actions in some cases, and so on.

We strongly recommend using our supported SDKs for your language (for example, for [Python](../../../api/python/start.md) or [C++](../../../api/cpp/description.md)) that already address these issues.

To work in the console, you should use the [CLI](../../../api/cli/cli.md) instead of sending direct HTTP requests.

{% endnote %}

HTTP proxies differ depending on the query type. Queries to the metainformation tree like `get` or `set` are considered relatively light and processed by so called control (light) HTTP proxies.

Read or write queries (`read-table`, `write-table`) can potentially transfer a significantly larger amount of data, more heavily load {{product-name}} cluster nodes, and are therefore processed by so called heavy proxies (data proxies).

For an example of how to interact with {{product-name}} via the HTTP interface, view the [HTTP proxy](../../../user-guide/proxy/http.md) section.

For reference information about the HTTP proxy, see [HTTP proxy reference](../../../user-guide/proxy/http-reference.md).

## RPC proxy { #rpc_proxy }

**RPC proxy** is a special interface for interacting with {{product-name}} via the RPC protocol. The RPC proxy is accessed via TCP, port 9013. What basically distinguishes the RPC proxy from the HTTP proxy:

- Higher query processing speed achieved due to the following features:
  - Work with the RPC proxy is performed using a special internal protocol that is better suited for working with a large number of concurrent queries.
  - In the RPC proxy, a query is sent in native {{product-name}} format and does not require additional time and resources for conversion.
  - The ability to maintain compatibility at the user code level with changes in the protocol of interaction of components within a {{product-name}} cluster.

The following programming languages are supported:

- [C++](../../../user-guide/proxy/rpc.md#c_plus_plus);
- [Java](../../../user-guide/proxy/rpc.md#java);
- [Python](../../../user-guide/proxy/rpc.md#python).

Examples are available for each programming language.

