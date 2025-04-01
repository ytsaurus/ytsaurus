## Current YTsaurus development roadmap

 _As new versions are released, we will update tasks in this roadmap with corresponding versions and add new tasks._
 ***

## Master & Cypress
- [ ] Sequoia: Cypress metadata in Dynamic Tables.
- [ ] S3 Support as a Medium: Introduce S3 support as a storage medium, expanding storage flexibility and options.
- [ ] Support for Apache Parquet/Apache Iceberg file format in S3 as external tables in Cypress.
- [ ] Develop support for time zones in the YTsaurus type system.
- [ ] Delta lake tables.
- [ ] Row Level Security. 

## Scheduler
- [X] Operation access control via ACOs (24.2).
- [ ] Gang operations support for distributed ML workloads (25.1).
- [ ] Jobs Timeline UI for Vanilla operations (25.1).
- [ ] New allocation scheduling strategy for ML workloads (25.2+).
- [ ] Flexible ACL management for pools.
- [ ] Renewal of documentation.
- [ ] Improved fair share distribution algorithm for gang operations.

## MapReduce
- [ ] API for Modifying Operation Parameters: Develop an API that allows for the modification of operation parameters during execution, enhancing flexibility and control over dynamic operations.
- [ ] Shuffle service
- [ ] Inter-Cluster Bandwidth Limiting Mechanism: Develop a mechanism to limit inter-cluster bandwidth during operations to optimize network resource usage and prevent overloads.
- [ ] IO Scheduler: Implement an IO scheduler to optimize input/output operations, improve resource allocation, and enhance overall system performance.
- [ ] Distributed jobs for data processing: this technology is needed to run GPU batch inference jobs in case of large models that don't fit inside one host.
- [ ] Support ARM64 host.
- [ ] Rework sorted chunk pool and make new_sorted_pool default.
- [ ] Accounting compressed data size in data slicing.
- [ ] Adaptive buffer row count in jobs: this is useful for heavy jobs to reduce number of job abortions caused by interuption timeout.

## Queues
- [x] Implement exactly once write semantics for YTsaurus queues
- [x] Refactor queue exports: add retries and rate limiting
- [ ] Kafka proxy
  - [x] Basic kafka functionality via a single kafka proxy
  - [ ] Opportunity to run more than one kafka proxy
  - [ ] Support transactional producers


## Dynamic Tables
- [ ] Bundle Controller: Make our internal dynamic tables resource management tool available in Open Source. Bundle controller allows multi-tennant separation of dynamic tables compute layer (e.g. tablet nodes) between different tennants and provides UI configuration for thread pool sizes and memory categories limitation
- [ ] Bundle diagnostics dashboards: Add several useful diagnostics dashboards to UI in Open Source. With dashboards and diagnostics guide it is much easier to resolve performance issues. 
- [ ] Chaos: Improve stability and performance.
- [ ] Seamless Tablet Migration: Develop and implement a protocol for seamless tablet migration to prevent memory and load imbalances on nodes. The goal is to minimize downtime to a few seconds by maintaining dual copies of tablets with consistent write support during migration. This includes achieving zero-downtime migration, implementing resharding, and devising a strategy for supporting write retries, potentially initiating development in this area.
- [ ] Bulk insert from YQL: YQL requires special type of bulk insert due to nested user transactions. We enhance bulk insert feature to allow inserting directly from YQL.
- [ ] Shared write: Lockless write mode useful for write-only transactions (also known as blind write). Initial support is already available in recent release but we work on performance and expect much lower latencies for reading values written in shared write mode. 
- [ ] Display Dynamic Table Errors in YT Interface: Implement an interface feature to display errors from dynamic tables with a selectable time range. The initial version is almost ready, and in the upcoming semesters, we will evaluate its performance and incorporate new error types.
- [ ] UDF Development: Enable support for YQL UDFs in dynamic tables, allowing users to leverage existing UDFs. The execution engine will utilize WebAssembly technology to ensure adequate isolation for running arbitrary code.
- [ ] Secondary Indexes: Add new types of secondary indices (mostly for ORM use-cases), improve performance and stability
- [ ] Nested structures: We enchance support for nested types in dynamic tables. Our long-term goal is to improve both querying nested values and storage.

## Query Tracker
- [X] Enabled file upload support
- [X] Added execution progress and timeline tracking in UI
- [X] Autocomplete of paths and function names
- [X] Support for SparkSQL in Query Tracker
- [ ] CHYT integration: support for multiple queries
- [ ] Improve search filter for queries
- [ ] Add support for DECLARE construction

## CHYT
- [ ] Improvements in YTsaurus transaction support
- [ ] Update ClickHouse version to 24.8
- [ ] Support for ingestion engines and materialized views
- [ ] Dynamic distribution of secondary queries
- [ ] Metadata-based query optimization
- [ ] Support for clickHouse dictionaries

## YQL
- [X] Support for DQ integration
- [ ] Introduce ClickHouse UDF in Open-Source release
- [ ] Push-down predicates into input query
- [ ] Support explicit runtime cluster specification and input tables from remote clusters
- [ ] Support Cost-Based Optimizer
- [ ] Support block computation based on arrow input
- [ ] Rework query isolation model to improve robustness
- [ ] Add federative queries in YQL (S3, HDFS)

## Flow
- [ ] Flow to be released in Open Source in 2026
- [ ] Exactly once stateful processing: Store both user-generated state and internal flow messages in dynamic tables to provide exactly-once guarantee for processing states in presence of worker failures.
- [ ] Watermarks and timers: Add watermarks and timers to allow trigger-based logic.
- [ ] Automatic load balancing: Automatically reshard and rebalance flow workers to smooth resource utilization and avoid bottlenecks.
- [ ] Message congestion control: Schedule flow messages to avoid congestion and reduce processing lag.
- [ ] Chaos-stored state: Store both internal flow messages and states in chaos replicated dynamic tables to provide strongest availability requirements if necessary.

## CLI & APIs
- [x] Support of https in python and CLI
- [ ] Python SDK: support more types in dataclasses: BigDate, BigTimestamp, Decimal, Json, Yson
- [ ] Python SDK: support using standart dataclasses for table row representation
- [ ] Python SDK: support custom column names for dataclass'es
- [ ] C++ Mapreduce SDK: support working over RPC proxies
- [ ] C++ Mapreduce SDK: better integration with yt/yt/core/logging
- [ ] Go SDK: Explore RPC encryption for enhanced security.
- [ ] Go SDK: Evaluate and potentially implement RPC streaming.
- [ ] Go SDK: Complex Type Interoperability  
- [ ] Go SDK: Integrate Skiff into supported operations.
- [ ] Go SDK: Add capabilities to work with queues.

## Microservices
- [ ] Open Source Release of Resource Usage: Provides visibility into account resource consumption.
- [ ] Open Source Release of Access Log Viewer: Enables viewing of all operations on objects in the UI.
- [ ] Timbertruck: support zstd compression

## Other
- [x] Airflow provider
- [ ] Debezium integration
