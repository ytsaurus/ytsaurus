# Proxy

This section contains information about the different interfaces for working with the {{product-name}} system: [HTTP](#http_proxy) and [RPC](#rpc_proxy) proxies.

## HTTP proxy { #http_proxy }

**HTTP proxy** is a universal language-agnostic interface to work with {{product-name}}.


{% note warning "Warning" %}

The low-level HTTP API is not a good choice for writing simple and reliable application code. Using it will require you to correctly log queries and label their correlation IDs, balance the load on the proxy, acquire additional explicit locks on tables, handle errors, repeat actions in some cases, and so on.

We strongly recommend using our supported SDKs for your language (for example, for [Python](../../../api/python/start.md) or [С++](../../../api/cpp/description.md)) that already address these issues.

To work in the console, you should use the [CLI](../../../api/cli/cli.md) instead of sending direct HTTP requests.

{% endnote %}

Each HTTP proxy is assigned a certain role that specifies which kind of load the proxy will handle.
- Cypress requests (e.g. `get`, `set`, `create`) are relatively light and processed by control HTTP proxies, the `control` proxy role.
- Read or write requests (for example, `read-table`, `write-table`) can potentially transfer a significantly larger amount of data, resulting in a bigger load on {{product-name}} cluster nodes. Such queries are therefore processed by so-called heavy proxies (data proxies), the `data` role.
- Individual processes can use their own roles so that their load does not interfere with other processes.

Client SDKs typically hide role-related operations. The only thing you may need to do is to specify the role in the client configuration if you need a non-standard role for heavy queries. However, when working directly with HTTP, light and heavy queries should be sent to different hosts. If you send a heavy query via a light HTTP proxy, the server will return an error. For more information, see [List of heavy proxies](../../../user-guide/proxy/http-reference.md#hosts).


To get examples of how to interract with {{product-name}} via the HTTP interface, check the [HTTP proxy](../../../user-guide/proxy/http.md) section.

For reference information about the HTTP proxy, see [HTTP proxy reference](../../../user-guide/proxy/http-reference.md).

## RPC proxy { #rpc_proxy }

**RPC proxy** is a special interface for interacting with {{product-name}} via the RPC protocol. The RPC proxy is accessed via TCP, port 9013. What basically distinguishes the RPC proxy from the HTTP proxy:

- Higher query processing speed achieved due to the following features:
  - Work with the RPC proxy is performed using a special internal protocol that is better suited for working with a large number of concurrent queries.
  - In the RPC proxy, a query is sent in native {{product-name}} format and does not require additional time and resources for conversion.
  - The ability to maintain compatibility at the user code level with changes in the protocol of interaction of components within a {{product-name}} cluster.

Like HTTP proxies, all RPC proxies are divided into roles. The role is specified in connection configuration and allows to balance the user load. The default rpc proxy role is `default`.


The following programming languages are supported:

- [C++](../../../user-guide/proxy/rpc.md#c_plus_plus)
- [Java](../../../user-guide/proxy/rpc.md#java)
- [Python](../../../user-guide/proxy/rpc.md#python)

Examples are available for each programming language.
