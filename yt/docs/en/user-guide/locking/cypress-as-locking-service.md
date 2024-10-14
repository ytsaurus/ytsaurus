# Distributed locks

{% include [Distributed locks](../../_includes/user-guide/locking/cypress-as-locking-service-p1.md) %}

## General information

To use {{product-name}} as a distributed locking service, you need to deploy master servers of a cluster in multiple locations.

There must be at least three locations so that the service can work if one of them is not available. If the master servers are located in five locations, the service can work if any two of them are not available.

{% include [Distributed locks](../../_includes/user-guide/locking/cypress-as-locking-service-p2.md) %}

## Quotas

{% include [Distributed locks](../../_includes/user-guide/locking/cypress-as-locking-service-p3.md) %}
