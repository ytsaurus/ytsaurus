# Introduction

YQL (Yandex Query Language) is a language of universal declarative queries against data storage and processing systems as well as an infrastructure to run such queries.

YQL benefits include:

- A powerful graph execution engine that can build MapReduce pipelines with hundreds of nodes and adapt during computation.
- Ability to build complex data processing pipelines using SQL by storing subqueries in variables as chains of dependent queries and transactions.
- Predictable parallel execution of queries of any complexity.
- Efficient implementation of joins, subqueries, and window functions with no restrictions on their topology or nesting.
- Extensive function library.
- Support for user-defined functions in [C++](udf/cpp.md), [Python](udf/python.md){% if audience == "internal" %}, and [JavaScript](udf/javascript.md){% endif %}.
- Automatic execution of small parts of queries on prepared compute instances, bypassing MapReduce operations to reduce latency.

# How to try

YQL provides a functional web interface where, among other things, you can:
- Write query code.
- Start and stop query execution.
- View query execution results.
- View query history.

![](../../images/yql_interface.png){ .center }
