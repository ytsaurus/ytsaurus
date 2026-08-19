## Task-proxy


Release notes for this component.




**Releases:**

{% cut "**0.4.1**" %}

**Release date:** 2026-08-10


**Release page:** [0.4.1](https://github.com/ytsaurus/ytsaurus-task-proxy/releases/tag/release/0.4.1)


**Helm chart:** [0.4.1](https://github.com/orgs/ytsaurus/packages/container/task-proxy-chart/1116607758?tag=0.4.1)


fix build scripts

{% endcut %}


{% cut "**0.4.0**" %}

**Release date:** 2026-08-10


**Release page:** [0.4.0](https://github.com/ytsaurus/ytsaurus-task-proxy/releases/tag/release/0.4.0)


#### Added

- Prometheus metrics for service discovery, YTsaurus requests, and authorization.
- Helm support for `ServiceMonitor` / `VMServiceScrape` and alert rules for Prometheus or VictoriaMetrics.
- Authorization-result caching with configurable TTL, capacity, backend-request concurrency, and proactive refresh.
- A Grafana dashboard for authorization metrics.
- Global task-proxy timeout settings:
   - `timeouts.connectTimeoutSeconds`
   - `timeouts.routeTimeoutSeconds`
   - `timeouts.streamIdleTimeoutSeconds`
- Per-operation `task_proxy` annotation overrides:
   - `route_timeout_seconds`
   - `stream_idle_timeout_seconds`
- Documentation for the `task_proxy` annotation format and timeout overrides.

 ### Changed

- Improved logging and metrics for authorization and discovery infrastructure errors.
- Added HTTP retries for YTsaurus requests.
- Helm now supports an explicit `ytProxy` value; when omitted, it uses the HTTP proxy service in the release namespace.
- Image tags are now optional in the Helm chart configuration.

 ### Migration notes

- For standalone binary deployments, `--namespace` has been replaced with the required `--yt-proxy` flag.
- Helm deployments remain compatible: leave `ytProxy` empty to retain the automatically generated proxy address.

{% endcut %}


{% cut "**0.3.0**" %}

**Release date:** 2026-03-23


**Release page:** [0.3.0](https://github.com/ytsaurus/ytsaurus-task-proxy/releases/tag/release/0.3.0)


**Helm chart:** [0.3.0](https://github.com/orgs/ytsaurus/packages/container/task-proxy-chart/752333866?tag=0.3.0)


Added routing based on `operationID/alias, taskName, service` triple as alternative to hash (8-character hex string, i.e. `645236d8`).

If operation has alias, it could be used in domain: `<operationAlias>-<taskName>-<service>.<baseDomain>`, which is suitable for UIs to open in browser. All you need is to specify operation alias, i.e. for SPYT standalone cluster or direct submit.

It's also supported for headers, you can use old option `x-yt-taskproxy-id: 645236d8` or new one, specifying values from triple with headers `x-yt-taskproxy-operation-id: <operationID>`, `x-yt-taskproxy-task-name: <taskName>`, `x-yt-taskproxy-service: <service>`. If operation has alias, you can use `x-yt-taskproxy-operation-alias: <operationAlias>` instead of operation ID header.

Please note that operation aliases, task names and services should match `[a-z0-9_]{1,30}$` regexp to avoid problems with using triple in domain.

{% endcut %}


{% cut "**0.2.3**" %}

**Release date:** 2026-02-16


**Release page:** [0.2.3](https://github.com/ytsaurus/ytsaurus-task-proxy/releases/tag/release/0.2.3)


**Helm chart:** [0.2.3](https://github.com/orgs/ytsaurus/packages/container/task-proxy-chart/688915103?tag=0.2.3)


change spyt master rest api service name (just `rest` to avoid dashes usage for better static FQDNs)

{% endcut %}


{% cut "**0.2.2**" %}

**Release date:** 2026-02-16


**Release page:** [0.2.2](https://github.com/ytsaurus/ytsaurus-task-proxy/releases/tag/release/0.2.2)


**Helm chart:** [0.2.2](https://github.com/orgs/ytsaurus/packages/container/task-proxy-chart/688652887?tag=0.2.2)


auto-discovery of SPYT standalone cluster's master restAPI service

{% endcut %}


{% cut "**0.2.1**" %}

**Release date:** 2026-02-04


**Release page:** [0.2.1](https://github.com/ytsaurus/ytsaurus-task-proxy/releases/tag/release/0.2.1)


**Helm chart:** [0.2.1](https://github.com/orgs/ytsaurus/packages/container/task-proxy-chart/670837677?tag=0.2.1)


Support header `x-yt-taskproxy-id` as alternative for domain routing

{% endcut %}


{% cut "**0.2.0**" %}

**Release date:** 2026-01-29


**Release page:** [0.2.0](https://github.com/ytsaurus/ytsaurus-task-proxy/releases/tag/release/0.2.0)


**Helm chart:** [0.2.0](https://github.com/orgs/ytsaurus/packages/container/task-proxy-chart/662319261?tag=0.2.0)


Initial version of Ytsaurus task proxy

{% endcut %}

