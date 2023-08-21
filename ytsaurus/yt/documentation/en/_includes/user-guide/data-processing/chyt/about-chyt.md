# CHYT

**CHYT powered by ClickHouse** is a technology that enables you to create a cluster of ClickHouse servers directly on {{product-name}} compute nodes.
[ClickHouse](https://clickhouse.com/) is created within a Vanilla operation and works with the data in {{product-name}}. This operation is called a clique (for more information, see [Concepts](../../../../user-guide/data-processing/chyt/general.md#what-is)). Cliques can be public (available to all users) and private (available to a user or a team). A public clique `*ch_public` is a basic public clique running on each {{product-name}} cluster.


## Main advantages { #advantages }

The vast majority of built-in ClickHouse features are available in CHYT. To learn more about various features of ClickHouse, see the [official documentation](https://clickhouse.com/docs/ru/).

Besides that, there are the following advantages:
- You don't need to copy data from {{product-name}} to ClickHouse.
- You can use the computing quota in {{product-name}} for fast computations.
- You can quickly perform computations on {{product-name}} data of small and medium size (up to 1 TB) up to 100 times faster than running MapReduce operations.
- Working with [static](../../../../user-guide/storage/static-tables.md) and [dynamic](../../../../user-guide/dynamic-tables/overview.md) tables is supported.

## Restrictions { #disadvantages }

Processed tables must be [schematized](../../../../user-guide/storage/static-schema.md).




