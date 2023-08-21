{% include [Overview](../../../_includes/user-guide/data-processing/spyt/overview-p1.md) %}

## What is SPYT? { #what-is-spyt }

Spark over {{product-name}} (SPYT) enables a Spark cluster to be started with {{product-name}} computational capacity.  The cluster is started in a [{{product-name}} Vanilla operation](../../../user-guide/data-processing/operations/vanilla.md), then takes a certain amount of resources from the quota and occupies them constantly.  Spark can read [static](../../../user-guide/storage/static-tables.md), as well as [dynamic {{product-name}} tables](../../dynamic-tables/overview.md), perform computations on them, and record the result in the static table.
Current underlying Spark version is 3.2.2.

{% include [Overview](../../../_includes/user-guide/data-processing/spyt/overview-p2.md) %}
