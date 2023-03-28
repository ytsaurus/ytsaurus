# JOIN in CHYT

This article presents a high-level description of the JOIN structure in CHYT.

## Sorting vs sharding { #sort-vs-shard }

The table storage schema in {{product-name}} is fundamentally different from that in ClickHouse. In {{product-name}}, the basic storage primitive is a [static table](../../../../../user-guide/storage/static-tables.md) with rows in [chunks](../../../../../user-guide/storage/chunks.md) randomly scattered across the cluster. Static tables are extremely inefficient for tasks that require point data reads (since these point reads usually result in Random IO on the hard disk). For quick point reads, {{product-name}} has a more efficient and complex primitive called [dynamic tables](../../../../user-guide/dynamic-tables/overview.md).

However, many batch applications that process a large amount of data in a flow require one way or another working in a model when some of the columns are *key*. {{product-name}} supports the concept of a sorted table — a table schema can show that table rows are sorted by a column prefix. Such columns are called key columns. Such metainformation enables you to efficiently implement, for example, the Sorted Reduce operation which is missing in the original Map-Reduce paradigm. In {{product-name}}, the Reduce range is very wide: in particular, operations like Reduce support foreign tables with `LEFT INNER JOIN` semantics.

In ClickHouse, the basis for distributed data storage is not [sorting](../../../../user-guide/dynamic-tables/sorted-dynamic-tables.md) as in {{product-name}}, but *sharding*. By default, the data is distributed across shards according to a shard key based on the remainder of the sharding expression divided by the total weight of all shards (for more information, see the [ClickHouse documentation](https://clickhouse.yandex/docs/en/operations/table_engines/distributed/)). Besides that, users often control the sharding logic themselves by inserting data into specific hosts.

Both schemas achieve the same goal in different ways:
- In {{product-name}}, sorting ensures the locality of rows with one key value in a single chunk (or in a set of consecutive chunks).
- In ClickHouse, sharding ensures the locality of rows with one sharding expression value on a single machine.

## How does a query work in CHYT? { #query }

The CHYT structure is described in detail In the [Query anatomy](../../../../../user-guide/data-processing/chyt/queries/anatomy.md) section. Any SELECT query is split by the coordinator into portions on the main table and then each portion is processed independently on its own instance.

This execution schema is very similar to that of the Distributed ClickHouse engine, except that in ClickHouse, data is read literally "from underneath" the instance, while in {{product-name}}, chunks are scattered randomly across the cluster machines, which is compensated by a very broad network.

This method is well-suited for single-source threaded queries that do not contain JOIN. However, as soon as the need for joins arises, you have to figure out how to combine related rows from different sources without having to perform random reads.

## JOIN types in ClickHouse { #types }

How can coordination (i.e. load distribution between instances) of JOIN work in a distributed environment? In ClickHouse, the following execution strategies of the `lhs JOIN rhs USING/ON ...` construction are possible:

1. Distributed local JOIN: if the tables are sharded *in the same way*, you can execute JOIN independently on each instance, since it is true that a pair of keys joined by JOIN cannot end up on different machines. Thus, `lhs` and `rhs` on each instance are interpreted as their respective local tables.
2. GLOBAL JOIN: if you use the keyword GLOBAL next to JOIN, you can force the system to do the following. The right-hand side `rhs` argument is executed and serialized on the query coordinator and its serialized representation is sent out along with the query to the instances. The instances are invited to use this representation to retrieve the right-hand side part in their memory. This method is good when `rhs` is relatively small and there are relatively few instances: it is easy to see that if one of these conditions is not met, you can bump into distribution of the table from the coordinator by subqueries over the network (or over the memory on the coordinator). This method *does not require* any additional conditions for the consistency of the storage/sharding schema on the tables.
3. JOIN via subqueries. ClickHouse enables you to surround `lhs` and/or `rhs` with brackets and this significantly affects the execution plan:
   — If you surround `lhs` with brackets, ClickHouse loses any information about the lhs structure, in particular, knowledge about the distributed nature of the left-hand side part is lost. In this situation, the left-hand side part is executed independently on the coordinator, the right-hand side part is executed independently and goes up to the RAM to the hash table, and then JOIN is completely executed only on the coordinator.
   — If you surround `rhs` with brackets, ClickHouse will make a distributed query as if the query just looks like `SELECT lhs`. Then it will send its queries to the instances, leaving `JOIN (rhs)` as it is. Next, each instance will execute `rhs` independently, which can lead to a much heavier load, since each instance will materialize the right-hand side part independently. The latter problem is solved by the protective mechanism that prohibits such behavior by default and results in the `Double-distributed IN/JOIN subqueries is denied` error.

For more information, see the ClickHouse documentation. Relevant links:
- [SELECT Query](https://clickhouse.com/docs/en/sql-reference/statements/select/);
- [distributed_product_mode](https://clickhouse.com/docs/en/operations/settings/settings/#distributed-product-mode).

What does a similar classification look like in CHYT? Points 2 and 3 work exactly the same way: strategy 2 is good if the right-hand side of the table is small. The first option of strategy 3 is suitable if the right-hand table is very large, but you can wait for a long time. Note that CHYT is designed for fast analytical queries and if something requires to join large heterogeneous tables, better use YQL and Map-Reduce. Therefore, we do not recommend this method.

There is a significant difference in strategy 1 in CHYT. You can read about it below.

## Sorted JOIN { #sorted }

Instead of the *Distributed JOIN* logic that uses the identity of the argument *sharding* schema, CHYT naturally produces the *Sorted JOIN* logic that uses the identity of the argument *sorting* schema. For those who understand how the Sorted Reduce operation works, Sorted JOIN works exactly the same way.

To use the Sorted JOIN strategy, use the ordinary `lhs JOIN rhs USING/ON ...` construction, but `lhs` and `rhs` are subject to the following additional restrictions:
- `lhs` and `rhs` must be sorted tables. Let `lhs` be sorted by columns `l1, l2, ..., ln` and `rhs` be sorted by columns `r1, r2, ..., rm`.
- The JOIN condition should look like a set of equalities `l1 = r1`, ..., `lk = rk` for a `k` (the equalities themselves can go in any order). This can be expressed as a set of equalities in the ON clause or as a set of common key columns in the USING clause, but not as a condition in the WHERE clause.

If these conditions are met, you can reuse the coordination logic from the Sorted reduce operation by forming pairs of appropriate ranges from `lhs` and `rhs` and distributing them to instances in subqueries. If this condition is not met, an error will occur and you will have to use either strategy 2 (use GLOBAL JOIN) or the second version of strategy 2 (place the right-hand side part in a subquery).
