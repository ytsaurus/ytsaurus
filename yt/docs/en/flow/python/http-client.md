# Shared HTTP clients in Python

Python processing callbacks can send HTTP requests through `ctx.http_client` and `ctx.https_client`. The companion creates these pooled `HttpClient` instances once per process and shares them across serving threads and computations. Resource lifecycle hooks receive the same clients in `ResourceContext`.

Use these clients after configuring the [Python companion](getting-started.md). For a maintained request example, see the [local HTTP tests]({{source-root}}/yt/yt/flow/library/python/companion/test/test_http_client.py), which check GET, POST, response bodies, status codes, cookies and redirects.

## Request API {#requests}

`HttpClient.request(method, url, *, headers=None, params=None, data=None, json=None, timeout=None)` returns `HttpResponse`. The convenience methods `get`, `post`, `put`, `patch`, `delete` and `head` accept a URL and the same keyword arguments.

#|
|| **Argument** | **Meaning** ||
|| `headers` | Request headers as a dictionary. ||
|| `params` | Query parameters as a dictionary. ||
|| `data` | Request body passed to the underlying Requests client. ||
|| `json` | Value serialized as a JSON request body. ||
|| `timeout` | A number of seconds, or a pair `(connect_timeout, read_timeout)` in seconds. Without an override, the connect timeout is 10 seconds and the read timeout is 60 seconds. This is not a total request deadline. ||
|#

Pass request-specific values at the call site. Do not mutate the shared client's state from a computation or resource.

## Response and errors {#responses}

`HttpResponse` provides these fields and methods:

#|
|| **Member** | **Result** ||
|| `status_code` | Integer HTTP status code. ||
|| `ok` | Whether the status is not an HTTP error (4xx or 5xx). A redirect response can also be `ok`. ||
|| `url` | Final response URL after any followed redirects. ||
|| `headers` | Response headers as a dictionary. ||
|| `content` | Response body as `bytes`. ||
|| `text` | Decoded response body. ||
|| `json(**kwargs)` | Response body decoded as JSON; keyword arguments go to the underlying decoder. ||
|#

An HTTP error status does not raise an exception: inspect `status_code` or `ok`. Request failures and JSON decoding errors propagate to your code. The wrapper adds no retry loop; choose retry and idempotency behavior for your external service explicitly.

The clients do not retain or automatically resend response cookies. Redirects are disabled by default, so a 3xx response is returned directly. A positive `max_redirect_count` enables following redirects up to the configured limit; exceeding the limit raises a client exception.

## Configuration {#configuration}

The worker node's `companion` block supplies `http_client_config` and `https_client_config`. The worker passes these blocks to the Python process through `YT_FLOW_COMPANION_CONFIG`; Python builds a separate client from each block. See [TCompanionConfig](../generated_docs/all_yson_structs.md#NYT_NFlow_NCompanion_TCompanionConfig) for the worker-side schema.

The Python wrapper reads the following keys from each client block:

#|
|| **Key** | **Python behavior** ||
|| `max_idle_connections` | Connection-pool size. An omitted value or `0` selects `8`; a positive value supplies the pool size. ||
|| `max_redirect_count` | Default `0` disables following redirects. A positive value sets the redirect limit. ||
|#

Other fields of the C++ HTTP/HTTPS configuration are not applied by this Python wrapper. In particular, configure the Python request timeout with the per-request `timeout` argument; C++ timeout fields do not replace it. These pool sizes are not a limit on the number of concurrent requests.

## Availability in tests and embedded code {#availability}

`HttpClient`, `HttpClients` and `HttpResponse` are exported by `yt.yt.flow.library.python.companion`. You can construct `HttpClient()` with defaults or pass a dictionary containing the supported configuration keys.

A runtime context created outside the serving path does not automatically receive clients. Reading `ctx.http_client` or `ctx.https_client` without injection raises `ValueError`. Tests that construct contexts must provide clients explicitly; the [context injection tests]({{source-root}}/yt/yt/flow/library/python/companion/test/test_http_client.py) show this boundary.

## Source and tests {#source-and-tests}

- [HTTP client implementation]({{source-root}}/yt/yt/flow/library/python/companion/http_client.py).
- [Runtime context]({{source-root}}/yt/yt/flow/library/python/companion/context.py) and [batch client injection]({{source-root}}/yt/yt/flow/library/python/companion/service.py).
- [Local HTTP and configuration tests]({{source-root}}/yt/yt/flow/library/python/companion/test/test_http_client.py).
- [Companion HTTP integration tests]({{source-root}}/yt/yt/flow/tests/companion/http_client/python/test/test_http_client_python.py).
