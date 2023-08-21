# Query anatomy before and after the ClickHouse engine

This article describes the CHYT structure and where you can get the query execution information.

## Query IDs: `trace id`, `query id`, and `datalens request id` { #id }

There are several IDs associated with any query which enable you to restore all the necessary query execution information post factum.

- `query id` is the ID assigned to each query in ClickHouse. Unlike original ClickHouse, this ID in CHYT is not controlled by the user and always has the *{{product-name}} GUID* form — four hexadecimal uint32 separated by hyphens.
- `trace id` is the ID that enables you to chain calls in distributed systems, creating a query execution "trace". This is a part of the [opentracing](https://opentracing.io/) protocol, specifically its implementation called [Jaeger Tracing](https://www.jaegertracing.io/) that is used in {{product-name}}. `trace id` is also a {{product-name}} GUID which in some situations coincides with `query id` and in other situations does not as will be detailed below.

## Query path from the client to heavy proxies { #way-to-proxy }

To better understand the logic by which these IDs exist and are assigned, figure out through which components and in which order the query passes to CHYT. The only public API to access CHYT at the moment is the HTTP API, so the following description applies to the HTTP protocol.

The first important point to execute the query is the so called *heavy {{product-name}} proxies*. They find out where instances of the clique with a given alias currently live, as well as those to which all {{product-name}} users have network access.

The 3 most popular ways to access {{product-name}} are shown below.

![](../../../../../../images/chyt_before_clique.png){ .center }

When you access CHYT directly from the script or from the command line using the curl utility, an SLB balancer such as `http://$YT_PROXY` is used as the endpoint. Behind it is a complex construction of balancers that directs the query to so called *control proxies* which respond with an HTTP redirect to the heavy proxies that serve all the heavy workload in {{product-name}}. With this access interface, `query id` is the same as `trace id`: you can see them in the `X-Yt-Trace-Id` and `X-ClickHouse-Query-Id` headers. Below is an example of interaction with CHYT using the CURL utility where the most interesting response headers are indicated.

```bash
curl -v --location-trusted 'http://$YT_PROXY/query?database=*ch_public' -d 'select max(a) from "//sys/clickhouse/sample_table"' -H "Authorization: OAuth `cat ~/.yt/token`"
*   Trying 2a02:6b8:0:3400:0:1d6:0:2:80...
* Connected to $YT_PROXY (2a02:6b8:0:3400:0:1d6:0:2) port 80 (#0)
> POST /query?database=*ch_public HTTP/1.1
> Host: $YT_PROXY
> User-Agent: curl/7.69.1-DEV
> Accept: */*
> Authorization: OAuth <i>...<my_token>...</i>
> Content-Length: 50
> Content-Type: application/x-www-form-urlencoded
>
* upload completely sent off: 50 out of 50 bytes
* Mark bundle as not supporting multiuse
<b> // Getting redirect to a heavy proxy.</b>
< HTTP/1.1 307 Temporary Redirect
< Content-Length: 0
< Location: http://sas4-9923-proxy-$YT_PROXY/query?database=*ch_public
< X-{{product-name}}-Trace-Id: 8e9bcc43-5c2be9b4-56f18c4e-117ea314  
<
* Connection #0 to host $YT_PROXY left intact
* Issue another request to this URL: 'http://sas4-9923-$YT_PROXY/query?database=*ch_public'
*   Trying 2a02:6b8:c1b:1a15:0:4397:169c:0:80...
* Connected to sas4-9923-$YT_PROXY (2a02:6b8:c1b:1a15:0:4397:169c:0) port 80 (#1)
> POST /query?database=*ch_public HTTP/1.1
> Host: sas4-9923-$YT_PROXY
> User-Agent: curl/7.69.1-DEV
> Accept: */*
> Authorization: OAuth <i>...<my_token>...</i>
> Content-Length: 50
> Content-Type: application/x-www-form-urlencoded
>
* upload completely sent off: 50 out of 50 bytes
* Mark bundle as not supporting multiuse
< HTTP/1.1 200 OK
< Transfer-Encoding: chunked
<b>  // Pay attention, query id = trace id. </b>
< <b>X-ClickHouse-Query-Id:</b> 3fa9405e-15b29877-524e3e67-2be50e94
< <b>X-Yt-Trace-Id:</b> 3fa9405e-15b29877-524e3e67-2be50e94
<b>  // For technical reasons, X-{{product-name}}-Trace-Id will appear twice.</b>
< X-{{product-name}}-Trace-Id: 3fa9405e-15b29877-524e3e67-2be50e94
< Keep-Alive: timeout=10
<b>  // Address of the query coordinator instance. </b>
< <b>X-ClickHouse-Server-Display-Name:</b> sas2-1374-node-$YT_PROXY
< X-{{product-name}}-Request-Id: 3fa9405d-26285349-db14531a-2a12b9f9
< Date: Sun, 05 Apr 2020 18:49:57 GMT
< Content-Type: text/tab-separated-values; charset=UTF-8
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<b>  // Address of the heavy proxy that served the query. </b>
< <b>X-{{product-name}}-Proxy:</b> sas4-9923-$YT_PROXY
<
1100
* Connection #1 to host sas4-9923-$YT_PROXY left intact
```

