## Query Tracker

Is published as a docker image.

**Current release:** {{qt-version}} (`ytsaurus/query-tracker:{{qt-version}}-relwithdebinfo`)

**All releases:**

{% cut "**0.0.8**" %}

- Optimized Query Tracker API performance by adding system tables indexes. Issue: [#653](https://github.com/ytsaurus/ytsaurus/issues/653)
- Added support of SystemPython udfs in YQL queries. Issue: [#265](https://github.com/ytsaurus/ytsaurus/issues/265)
- Fixed broken logs compression in YQL agent. Issue: [#623](https://github.com/ytsaurus/ytsaurus/issues/623)
- Optimized simultaneous YQL queries performance.
- Fixed memory leak in YQL Agent.
- Important fix. Fixed YQL queries results corruption in DQ. Issue: [#707](https://github.com/ytsaurus/ytsaurus/issues/707)
- Added DQ support in dual stack networks. Issue: [#744](https://github.com/ytsaurus/ytsaurus/issues/744)

{% endcut %}

{% cut "**0.0.7**" %}

- Important fix. Fixed YQL queries results corruption. Issue: [#707](https://github.com/ytsaurus/ytsaurus/issues/707)
- Fixed YQL DQ launching.
- Fixed bug caused UTF-8 errors in yql-agent logs.
- Fixed multiple deadlocks in yql-agent.
- Added support for SPYT discovery groups.
- Added support for SPYT queries parameters.
- Added everyone-share ACO which can be used to share queries by link.
- Added support of multiple ACOs per query, feature will be available in fresh UI, SDK releases.
- Changed interaction between Query Tracker and Proxies.

{% note info %}

This release is only compatible with proxy version 23.2.1, operator version 0.10.0 and later
https://github.com/ytsaurus/ytsaurus/releases/tag/docker%2Fytsaurus%2F23.2.1
https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.10.0

{% endnote %}

{% endcut %}


{% cut "**0.0.6**" %}

- Fixed authorization in complex cluster-free YQL queries
- Fixed a bug that caused queries with large queries to never complete
- Fixed a bag caused possibility of SQL injection in query tracker
- Reduced the size of query_tracker docker images

**Related issues:**
- [Problems with QT ACOs](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/176)

In case of an error when starting query
```
Access control object "nobody" does not exist
```
You need to run commands by admin
```
yt create access_control_object_namespace --attr '{name=queries}'
yt create access_control_object --attr '{namespace=queries;name=nobody}'
```

{% endcut %}

{% cut "**0.0.5**" %}

- Added access control to queries.
- Added support for the in‑memory DQ engine that accelerates small YQL queries.
- Added execution mode setting to query tracker. This allows to run queries in validate and explain modes.
- Fixed a bug that caused queries to be lost in query_tracker.
- Fixed a bug related to yson parsing in YQL queries.
- Reduced the load on the state dyntables by QT.
- Improved authentication in YQL queries.
- Added authentication in SPYT queries.
- Added reuse of spyt sessions. Speeds up the sequential launch of SPYT queries from a single user.
- Changed the build type of QT images from cmake to ya make.

NB:
- Compatible only with operator version 0.6.0 and later.
- Compatible only with proxies version 23.2 and later.
- Before updating, please read the QT documentation, which contains information about the new query access control.

New related issues:
- [Problems with QT ACOs](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/176)

In case of an error when starting query
```
Access control object "nobody" does not exist
```
You need to run commands by admin
```
yt create access_control_object_namespace --attr '{name=queries}'
yt create access_control_object --attr '{namespace=queries;name=nobody}'
```

{% endcut %}

{% cut "**0.0.4**" %}

- Applied YQL defaults from the documentation
- Fixed a bag in YQL queries that don't use {{product-name}} tables
- Fixed a bag in YQL queries that use aggregate functions
- Supported common UDF functions in YQL

NB: This release is compatible only with the operator 0.5.0 and newer versions.

{% endcut %}

{% cut "**0.0.3**" %}

- Fixed a bug that caused the user transaction to expire before the completion of the yql query on IPv4 only networks.
- System query_tracker tables have been moved to sys bundle

{% endcut %}

{% cut "**0.0.2**" %}

—

{% endcut %}

{% cut "**0.0.1**" %}

- Added authentication, now all requests are run on behalf of the user that initiated them.
- Added support for v3 types in YQL queries.
- Added the ability to set the default cluster to execute YQL queries on.
- Changed the format of presenting YQL query errors.
- Fixed a bug that caused errors during the execution of queries that did not return any result.
- Fixed a bug that caused errors during the execution of queries that extracted data from dynamic tables.
- Fixed a bug that caused memory usage errors. YqlAgent no longer crashes for no reason under the load.

{% endcut %}