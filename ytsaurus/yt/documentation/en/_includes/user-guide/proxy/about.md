# Proxy

This section contains information about the different interfaces for working with the {{product-name}} system: [HTTP](#http_proxy) and [RPC](#rpc_proxy) proxies.

## HTTP proxy { #http_proxy }

**HTTP proxy** is a universal interface for working with the {{product-name}} system.


{% note info "Note" %}

There are more convenient interfaces for working with the {{product-name}} system: [Python API](../../../api/python/start.md) and [С++ API](../../../api/c/description.md).

{% endnote %}


This interface can be useful in one of two cases:

1. You need to use {{product-name}} [commands](../../../api/commands.md) in a browser or console utility (such as [curl](https://en.wikipedia.org/wiki/CURL)).
2. You need to write a layer with abstractions to {{product-name}}.

HTTP proxies differ depending on the query type. Queries to the metainformation tree like `get` or `set` are considered relatively light and processed by so called control (light) HTTP proxies.
Read or write queries (`read-table`, `write-table`) can potentially transfer a significantly larger amount of data, more heavily load {{product-name}} cluster nodes, and are therefore processed by so called heavy proxies (data proxies).

An example of user interaction with {{product-name}} via the HTTP interface is available in the [HTTP proxy](../../user-guide/proxy/http.md) section.

For reference information about the HTTP proxy, see [HTTP proxy reference](../../user-guide/proxy/http-reference.md).

## RPC proxy { #rpc_proxy }

**RPC proxy** is a special interface for interacting with {{product-name}} via the RPC protocol. The RPC proxy is accessed via TCP, port 9013. What basically distinguishes the RPC proxy from the HTTP proxy:

- Higher query processing speed achieved due to the following features:
   - Work with the RPC proxy is performed using a special internal protocol that is better suited for working with a large number of concurrent queries.
   - In the RPC proxy, a query is sent in native {{product-name}} format and does not require additional time and resources for conversion.
   - The ability to maintain compatibility at the user code level with changes in the protocol of interaction of components within a {{product-name}} cluster.

The following programming languages are supported:

- [C++](../../user-guide/proxy/rpc.md#c_plus_plus);
- [Java](../../user-guide/proxy/rpc.md#java);
- [Python](../../user-guide/proxy/rpc.md#python).

Examples are available for each programming language.

