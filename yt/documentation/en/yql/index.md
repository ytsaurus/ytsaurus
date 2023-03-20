---
vcsPath: yql/docs_yfm/docs/ru/yql-product/index.md
sourcePath: yql-product/index.md
---
# Introduction

YQL is an SQL-based language of universal declarative queries against data storage and processing systems, as well as an infrastructure to run such queries.

YQL benefits include:
- A powerful graph execution engine that can build MapReduce pipelines with hundreds of nodes and adapt during computation.
- Ability to build complex data processing pipelines using SQL by storing subqueries in variables as chains of dependent queries and transactions.
- Predictable parallel execution of queries of any complexity.
- Efficient implementation of joins, subqueries, and window functions with no restrictions on their topology or nesting.
- Extensive function library.
- Support for custom functions in C++, Python, and JavaScript.
- Automatic execution of small parts of queries on prepared compute instances, bypassing MapReduce operations to reduce latency.

# How to try

YQL предоставляет функциональный веб-интерфейс в котором среди прочего можно:
- писать код запросов;
- запускать и останавливать выполнение запросов;
- просматривать результат выполнения запросов;
- просматривать историю запросов.

![](../../images/yql_interface.png){ .center }
