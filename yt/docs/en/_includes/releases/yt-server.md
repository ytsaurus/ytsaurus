## YTsaurus server


All main components are released as a docker image.




**Releases:**

{% cut "**25.1.0**" %}

**Release date:** 2025-07-28


_To install YTsaurus Server 25.1.0 [update](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.25.0) the k8s-operator to version 0.25.0._

#### Significant changes

#### API
  - Enhancement of operations/jobs API.
  - Introduced requests memory tracking in HTTP proxies.

#### Compute
  - Implemented permission validation for operations using access control objects.
  - Gang-operations gone production.
  - Introduced PatchSpec API to modify operation spec at runtime.
  - [experimental] Remote copy scheduler now considers inter-cluster bandwidth limits.
  - Introduced job shell for CRI-based job environments.

#### Storage
  - Supported `decimal256` type.
  - Support non-materialized computed column evaluation in static tables.

#### Query language features
  - Added `allow_async_replica` hint to QL table descriptors to enable fallback to async replicas in selects.
  - Timestamp functions in QL now support localtime  and use lookup tables for better performance.
  - QL AVG  function now supports doubles and unsigned integers.
  - Functions list_contains  and list_has_intersection  in QL now support strongly typed composite values and nulls.
  - `EXPLAIN` query now works with replicated tables.

#### Dynamic tables
  - Implemented gradual global compaction ("chunk reincarnation") for dynamic tables.
  - Supported remote copy for dynamic tables with hunk chunks.

#### Default values
 - Changed default read format to scan  for dynamic tables.
 - Enabled versioned remote copy by default.
 - Enabled remote copy of hunk chunks by default.
 - Enabled two-random-choices allocation strategy for write targets by default.

#### Full changelog

#### Scheduler and GPU

##### New Features & Changes:
- Added alert for unrecognized options in the pool tree config, [6b2770b](https://github.com/ytsaurus/ytsaurus/commit/6b2770b65a51766fbd1ad53d81d6b409a2115f00).
- Added support for generating temporary tokens valid for the duration of an operation, stored in the operation’s secure vault, [c91fd05](https://github.com/ytsaurus/ytsaurus/commit/c91fd057421160254f41d6c394673bf654fc358d).
- Added an option to only allow gang operations to start in FIFO pools, [edcac9a](https://github.com/ytsaurus/ytsaurus/commit/edcac9a3e348b18fd03cb82fb9e15e782b1b4a58).
- Implemented permission validation for operations using access control objects, [bd64281](https://github.com/ytsaurus/ytsaurus/commit/bd64281b11d8fef1a8f497c07ee1a0c5372073d9).
- Enhancement of operations/jobs API:
  - Introduced `attributes` and `events` in `list_jobs`  [25a405d0c88](https://github.com/ytsaurus/ytsaurus/commit/25a405d0c88).
  - Access to operations archive and `//sys/operations` is closed by default.
  - Added operation incarnations to archive and supported corresponding filter in API [6601c8dbbb1](https://github.com/ytsaurus/ytsaurus/commit/6601c8dbbb1).
- Added support for managing operations within `manage` right on pool [177380edfed](https://github.com/ytsaurus/ytsaurus/commit/177380edfed). 
- Supported new logic for calculating cpu limit overcommit [d6d1d08a91f](https://github.com/ytsaurus/ytsaurus/commit/d6d1d08a91f).
- Added ability to run GPU checks in isolated volumes and configure them via operation options, [631f328](https://github.com/ytsaurus/ytsaurus/commit/631f3288fa3eb07176c777c93bcf3afddfaf30dc).

##### Fixes & Optimizations:
- Fixed bug when `offloading` and `schedule_in_single_tree` options specified simultaneously [e36e910b718](https://github.com/ytsaurus/ytsaurus/commit/e36e910b718)
- Fixed fair share truncation in FIFO pools for single allocation vanillas [5d8e22b67fa](https://github.com/ytsaurus/ytsaurus/commit/5d8e22b67fa).
- Dropped some scheduler compats and legacy options.

#### Queue Agent

##### New Features & Changes:
- Added `controller_info` field to queue agent Orchid for detecting stuck controller passes and tracking error counters, [74be9b4](https://github.com/ytsaurus/ytsaurus/commit/74be9b4ab44689d56f8110dc2313e1dc7c1e3057).
- Export progress now includes details about the last export iteration, [6ec743a](https://github.com/ytsaurus/ytsaurus/commit/6ec743a21e4f4ed8e917fc17e822875367709ad3).
- Aggregated alert metrics for the queue agent have been added, [d2bf505](https://github.com/ytsaurus/ytsaurus/commit/d2bf505c9b3bafa8f0966cdd0234c4929416c293).

##### Fixes & Optimizations:
- Added retry backoffs to avoid overloading in case of misconfigured exports, [ae4ead4](https://github.com/ytsaurus/ytsaurus/commit/ae4ead4a1e9e5b86bbc9ffde2637488fb1dbe6ff).
- Fixed crash when reusing an export directory after recreating a queue without resetting export progress, [49826ed](https://github.com/ytsaurus/ytsaurus/commit/49826ed8d9872d3d094224e0811335a2647ef7c3).

#### Proxy

##### New Features & Changes:
- Added support for HTTP proxies in the `discover_proxies` handler, [830e543](https://github.com/ytsaurus/ytsaurus/commit/830e543c1bbde6069180bdf451c807ff0175bdb6).
- Preserved original column types (e.g., `timestamp`) in `web_json` responses from `select_rows`, [f4eb42d](https://github.com/ytsaurus/ytsaurus/commit/f4eb42d1b4e8521b8d70d83ec19ef50cb63647c5).
- Enabled signature generation and verification in HTTP proxy using Cypress public keys, [f8d0c7d](https://github.com/ytsaurus/ytsaurus/commit/f8d0c7d4f096d335b7fc5313f360dcab6d47ceb2).
- Introduced memory tracking for light/heavy HTTP requests, [06f7aeb](https://github.com/ytsaurus/ytsaurus/commit/06f7aeb2a484a802b4a401fcfd0c3216c9cdc33f).
- Added `require_sync_replica` parameter to the `push_queue_producer` handler, [1324b1b](https://github.com/ytsaurus/ytsaurus/commit/1324b1bb4aa2ec33249447dcc38257836b9e5ca0).
- Enabled parallel execution of `discover_versions` across components, [f06afce](https://github.com/ytsaurus/ytsaurus/commit/f06afce8d73c34082343b85f577f8ae3edcb114d).
- Added `create_user_if_not_exists` config option to prevent user creation during OAuth authentication (Issue #930), [8470ed6](https://github.com/ytsaurus/ytsaurus/commit/8470ed63a46beb3c483c885d62a195fbfd4ac77f).
- Added `require_password_in_authentication_commands` flag to allow skipping password checks in auth commands, [723db18](https://github.com/ytsaurus/ytsaurus/commit/723db18f80a3ceff00ed67d6a9cb5872b9c1ffda).
- Introduced `cache_key_mode` to control the granularity of credential caching, [df66eb5](https://github.com/ytsaurus/ytsaurus/commit/df66eb5a00ab7d338fb0972f618ad28862a2a440).
- Added `EnableCookies` option to `PartitionTable` request, which returns an opaque cookie for `CreateTablePartitionReader`, [f2f1ce6](https://github.com/ytsaurus/ytsaurus/commit/f2f1ce6039fd84e6cd13341e90db0a834ea1580e).
- Added `CreateTablePartitionReader` method to read a table partition without master requests, [f2f1ce6](https://github.com/ytsaurus/ytsaurus/commit/f2f1ce6039fd84e6cd13341e90db0a834ea1580e).
- Added support for signature validation using Cypress public keys, [12c8532](https://github.com/ytsaurus/ytsaurus/commit/12c85321bfd4fd41f32db7ee356345e0aee9c2b0).
- Added a handler to retrieve job trace events, [8ee855e](https://github.com/ytsaurus/ytsaurus/commit/8ee855ef01ae55a6c500d6ec6029bac3b3c8260c).

##### Fixes & Optimizations:
- Switched to using a dedicated thread pool for HTTP stream compression, [4f61857](https://github.com/ytsaurus/ytsaurus/commit/4f61857cbabea72e403b269f361de5b01bb6d746).
- Added type compatibility checks in the Arrow parser using YT-specific types, [bd0a6ff](https://github.com/ytsaurus/ytsaurus/commit/bd0a6ff376a1c2ec28e10d4ee4d476d70bb8a131).
- Introduced `UploadTransactionPingPeriod` config to correctly handle upload transaction timeouts, [f10f749](https://github.com/ytsaurus/ytsaurus/commit/f10f749527d924fece0e52a0248cf0c58ed8d313).
- Embedded HTTP proxy Arrow writer directly into the Arrow encoder to remove duplication, [99293b6](https://github.com/ytsaurus/ytsaurus/commit/99293b6cf4c9a157209d556bc8e6f128be3832e1).
- Enabled reading of Arrow tables with different column counts in chunk metadata, [6210035](https://github.com/ytsaurus/ytsaurus/commit/6210035056c12e3b078f7a3e01b562030f946192).
- Fixed incorrect data type in driver for `push_queue_producer` input, [ebffb74](https://github.com/ytsaurus/ytsaurus/commit/ebffb746ada4d63b768c7627e9bdee110b686cbd).
- Decimal improvements:
  - Added support for nested `decimal128` and `decimal256` in Arrow.
  - Fixed incorrect fixed-length encoding of `decimal256(n, p)` for small precisions, now variable length like `decimal128`, [58c6c65](https://github.com/ytsaurus/ytsaurus/commit/58c6c6590919a2bfbb88c9f5b833324b86623ead).

#### Dynamic Tables

##### New Features & Changes:
- Added row-cache poisoning to help detect memory errors, [4933fe9](https://github.com/ytsaurus/ytsaurus/commit/4933fe97dd68a40b2c5cdce4d2aa77000fdfb8dc).
- Added `allow_async_replica` hint to QL table descriptors to enable fallback to async replicas in selects, [27ac9b9](https://github.com/ytsaurus/ytsaurus/commit/27ac9b90f5d50dfc63490d86f24fb88bc042e74b).
- Added a method to return freezing or unmounting tables to the mounted state, [cdb2027](https://github.com/ytsaurus/ytsaurus/commit/cdb2027bf1b1de28a9a025a14375da5cd29bcd75).
- Enabled lookups to use the fair-share thread pool, [b19ecb3](https://github.com/ytsaurus/ytsaurus/commit/b19ecb34beae00c66f1c11ad3e6a19681f711087).
- Introduced a protocol that allows writing to tablets during smooth movement, [3ab47cd](https://github.com/ytsaurus/ytsaurus/commit/3ab47cd930accca3d38fac269e67b8ae8862d29b).
- Bundle controller can now manage memory limits for queries, [c6c6de4](https://github.com/ytsaurus/ytsaurus/commit/c6c6de47a8a8f0d64668a6a219e369f5c20a73ed).
- Timestamp functions in QL now support `localtime` and use lookup tables for better performance, [265160e](https://github.com/ytsaurus/ytsaurus/commit/265160e382062e7e19673da27f0242a636b7c51f).
- QL `AVG` function now supports doubles and unsigned integers, [d597f37](https://github.com/ytsaurus/ytsaurus/commit/d597f37096f44c7cc418da5cb986e06ed4377625).
- Functions `list_contains` and `list_has_intersection` in QL now support strongly typed composite values and nulls, [aeb6b24](https://github.com/ytsaurus/ytsaurus/commit/aeb6b24220a07385ab1975b610a6af06a75ed9fc).
- Added profiling counters for `pull_queue` and `pull_queue_consumer` commands on tablet nodes, [6aebfc1](https://github.com/ytsaurus/ytsaurus/commit/6aebfc1a758092864dae6b25177ec4339bc1c2db).
- `EXPLAIN` query now works with replicated tables, [fcb3dba](https://github.com/ytsaurus/ytsaurus/commit/fcb3dbab1a50858856e5aa52977a185620bb4b56).
- Changed default read format to `scan` for dynamic tables, [f2ccc73](https://github.com/ytsaurus/ytsaurus/commit/f2ccc73dbd5b2883d733b16145ae16f55e7fe272).
- Select queries now choose random in-sync replicas even from the same cluster, [efdf083](https://github.com/ytsaurus/ytsaurus/commit/efdf083a7d8cc0e7345f8b7705b87c0ec6a7ee47).
- Added `total_grouped_row_count` to QL statistics, [e37b81f](https://github.com/ytsaurus/ytsaurus/commit/e37b81f5199263dd9d15ef1116cd0be0463f8145).
- Enabled row-cache support for selects, useful for dictionary-style joins, [bba767b](https://github.com/ytsaurus/ytsaurus/commit/bba767b3f8671bddb8f6aed77d0ee25eed16d056).
- Implemented gradual global compaction ("chunk reincarnation") for dynamic tables, [6d405af](https://github.com/ytsaurus/ytsaurus/commit/6d405af0192ab66acf71bccbf38f2f02fda966bc).
- Added support for lookup format with timestamp columns, [180af7c](https://github.com/ytsaurus/ytsaurus/commit/180af7c4b45cacaa66783809c9c7b4759c385348).
- Added `allow_reign_change` parameter to tablet node config for crash-on-reign-change testing, [3754c50](https://github.com/ytsaurus/ytsaurus/commit/3754c50c141d89ef9ce6fe6a40df24e941d900ba).
- Introduced states for secondary indices: `invalid`, `bijective`, `injective`, and `unknown`, [81068e0](https://github.com/ytsaurus/ytsaurus/commit/81068e0145fcb6899cc8e351fcd72398ca55a763).
- Enabled versioned remote copy by default, [0862748](https://github.com/ytsaurus/ytsaurus/commit/08627486d980cc99aa0e8ec14104c7c6fbf43dea).
- Added support for remote copy of erasure-coded hunk chunks, [2b783b0](https://github.com/ytsaurus/ytsaurus/commit/2b783b04de6e5ade76d159ce05e674bb9545b50f).
- `GROUP BY + LIMIT` without `ORDER BY` is now executed in parallel unless certain conditions apply. Affects behavior of `WITH TOTALS`, [c296f24](https://github.com/ytsaurus/ytsaurus/commit/c296f24846653dff7b7e6bc2b3c913e05fa479ce).
- Remote copy for dynamic tables with hunk chunks is now supported (except with compression dictionaries and striped erasure), [34f16d0](https://github.com/ytsaurus/ytsaurus/commit/34f16d0fd4c307134608f27cfb5cc028b3cbc771).
- `dump-snapshot` command now supports `checksum` mode to help debug snapshot mismatches, [5daa913](https://github.com/ytsaurus/ytsaurus/commit/5daa91303d7210689ecb117614b908523bd429af).

##### Fixes & Optimizations:
- Fixed float precision issues in scan reads by serializing floats as doubles, [0b78e7a](https://github.com/ytsaurus/ytsaurus/commit/0b78e7ad5b4a4ec48ece1f92501172bbeff8ec6d).
- Fixed incorrect timezone behavior in `timestamp_floor_*_localtime` functions, [17f3dd1](https://github.com/ytsaurus/ytsaurus/commit/17f3dd1debf37d5b2a0222cf3d906b40e523ae12).
- Switched from logical to physical chunk count in ordered dynamic tablet chunk lists, [2efe013](https://github.com/ytsaurus/ytsaurus/commit/2efe013a9ba51a105c3ce490b6cffdcdcc415c36).
- Use lookup joins automatically when the left subplan is selective, [4b8b207](https://github.com/ytsaurus/ytsaurus/commit/4b8b207a6ea179d480bc05f5478a226d31c3da99).
- Fixed segfaults when reading tables with nested columns, [45cb542](https://github.com/ytsaurus/ytsaurus/commit/45cb542f8c883e7d559fee880065ddc5abde2e34).
- Bundle controller now skips faulty bundles instead of blocking progress, [dc149b2](https://github.com/ytsaurus/ytsaurus/commit/dc149b27843635d16d2416f09a21ffba8b4de702).

#### MapReduce

##### New Features & Changes:
- Added various job splitter options to operation specs, [e2998a4](https://github.com/ytsaurus/ytsaurus/commit/e2998a41e1794bfc139709f1df6cfd282cf82e33).
- Preserved job cookies during gang operation restarts and allowed restarting already completed jobs on incarnation switch, [e3f7655](https://github.com/ytsaurus/ytsaurus/commit/e3f7655698256ea6c257171d17ba8af7f130a299).
- Added operation incarnation controller that restarts all jobs if one terminates (useful for distributed ML), [fb9c7d3](https://github.com/ytsaurus/ytsaurus/commit/fb9c7d3d97563cb4e7d8d80270fe1be4739606d3).
- Introduced job shell for CRI-based job environments, [6b18f2f](https://github.com/ytsaurus/ytsaurus/commit/6b18f2f3d836bc63e6e89d44dcce328a5bc2d958).
- Allowed dynamic updates to `job_count` for vanilla tasks, [3a5cfef](https://github.com/ytsaurus/ytsaurus/commit/3a5cfef7f0a2a87cd5b48637d03467dc49f82f91).
- Introduced `PatchSpec` API to modify operation spec at runtime (initially supports `max_failed_job_count`), [3a5cfef](https://github.com/ytsaurus/ytsaurus/commit/3a5cfef7f0a2a87cd5b48637d03467dc49f82f91).
- `RemoteCopy` operations now always copy key system attributes (`compression_codec`, `erasure_codec`, `optimize_for`) even if `copy_attributes` is false, [25be378](https://github.com/ytsaurus/ytsaurus/commit/25be3785e95e49e1639ccef1ff49fc7077f1e1c7).
- Remote copy scheduler now considers inter-cluster bandwidth limits, [e0af4fd](https://github.com/ytsaurus/ytsaurus/commit/e0af4fdbdea4fcab2561b803364f153078195223).
- Controller agents now always fetch schemas from secondary cells using schema IDs, [af00687](https://github.com/ytsaurus/ytsaurus/commit/af0068716ea05530aade3474f15e59c7b2ab9d16).

##### Fixes & Optimizations:
- Fixed a bug where teleportation of a single chunk in unordered pool could fail, [897ffff](https://github.com/ytsaurus/ytsaurus/commit/897ffff6c1ed4f7d21d52b3fb456ff8fa8b7023b).
- Returned specific error instead of generic `Job failed by external request`, [703aae5](https://github.com/ytsaurus/ytsaurus/commit/703aae58eb1b2a515c2aa9e49d4e9ae63bc3a39e).

#### Master Server

##### New Features & Changes:
- Added metrics for read/write request rate limits and request queue size per user, [0052341](https://github.com/ytsaurus/ytsaurus/commit/0052341c563cc3c9eb2a3bdca11aaf7d36eabce4).
- Prevented prerequisite paths from differing from execution paths, [50156f0](https://github.com/ytsaurus/ytsaurus/commit/50156f0aa5382824a725ab27d3d1b2dc6210293b).
- Support opaqueness for `@schema` attribute, [4f2c6ad](https://github.com/ytsaurus/ytsaurus/commit/4f2c6ad11f2bc951642546e33b790705911cee3d).
- Allowed master cell removal without downtime of other components (except masters), [16bd5ba](https://github.com/ytsaurus/ytsaurus/commit/16bd5baf39e018085f42d8776dffefe16e648d27).
- Introduced a new pipeline for cross-cell copy operations, [e7eea1e](https://github.com/ytsaurus/ytsaurus/commit/e7eea1ed49fb49d12d64639161e33943a4e06fa4).
- Added `TSnapshotLoadContextGuard` to provide access to readonly mode during snapshot loading, [ee94027](https://github.com/ytsaurus/ytsaurus/commit/ee9402746c38adf4b23d5166cc4f41fdc01c4f20).
- Added validation of meta aggregated writer chunk extensions during master merge jobs, [de86b64](https://github.com/ytsaurus/ytsaurus/commit/de86b64ca13ed72993e83e5e6bccedcb25fff0ef).
- Reduced `TTableNode` memory usage by optimizing enums and chunk requisition fields, [90226b0](https://github.com/ytsaurus/ytsaurus/commit/90226b0af60dffa15565e7f4642c0e0482038c96).
- Always enabled the local read executor, [521af69](https://github.com/ytsaurus/ytsaurus/commit/521af696a57c69c7b19855b310dacae1fce7e6a9).
- Reworked `@hunk_primary_medium` and `@hunk_media` attributes:
  - They are now nullable.
  - Hunk chunks can be placed on a different medium than table chunks.
  - Replication is respected by the chunk replicator and initial writer.
  - Setting and clearing behavior is now well-defined, [508e000](https://github.com/ytsaurus/ytsaurus/commit/508e000430a6637a030c1575c7963efe1e37723a).
- Added `snapshot-dump-scope-filter` option to limit output during master snapshot dumps, [8a73474](https://github.com/ytsaurus/ytsaurus/commit/8a734741b3b893596562e8b0968a82983679ea41).
- Enabled two-random-choices allocation strategy for write targets by default, [1a153fb](https://github.com/ytsaurus/ytsaurus/commit/1a153fb75b539f97bb3534cbd2a24b5405c4c91c).
- Introduced Sequoia response keeper for tracking responses, [f886fa1](https://github.com/ytsaurus/ytsaurus/commit/f886fa13b8efa2c0959587a6e0bc6f5a5541a846).
- Added `checksum` mode to `dump-snapshot` CLI command for debugging snapshot mismatches, [5daa913](https://github.com/ytsaurus/ytsaurus/commit/5daa91303d7210689ecb117614b908523bd429af).
- Enabled non-materialized computed column evaluation in static tables, [221672d](https://github.com/ytsaurus/ytsaurus/commit/221672d07a7b81987beff311ec22319d631a8278).

##### Fixes & Optimizations:
- Fixed validation error when prerequisite and execution paths differ, [76ceba0](https://github.com/ytsaurus/ytsaurus/commit/76ceba0c03c5fce8bc042dcb3726a3997a2153d5).
- Prevented users from creating tables that are indices to themselves, [f384a9f](https://github.com/ytsaurus/ytsaurus/commit/f384a9f9ea286f4cc4b018498bca6596664c0c85).
- Fixed chunk replicator to correctly handle medium-specific replication overrides, [2e0ca64](https://github.com/ytsaurus/ytsaurus/commit/2e0ca6446ad66eeba31e4c3a2b91d56bf11c0293).
- Fixed chunk requisition updates during merging, [29cc496](https://github.com/ytsaurus/ytsaurus/commit/29cc4968ed660253c6b8167993a67684a716eb9f).
- Fixed duplicate validation issue on paths, [3beea00](https://github.com/ytsaurus/ytsaurus/commit/3beea00256a370a74e5a9c1cb4b56e437a4159ad).
- Fixed master statistics check on cell removal, [2e93998](https://github.com/ytsaurus/ytsaurus/commit/2e9399802d02177cc0cd82069da18e09f5ade73a).
- Fixed handling of replication override in chunk replicator (duplicate fix), [8c711f8](https://github.com/ytsaurus/ytsaurus/commit/8c711f8b07d3f3ee55b7b4fb503b736072b66cdb).
- Fixed group ID for built-in `admins` group, [f687614](https://github.com/ytsaurus/ytsaurus/commit/f687614331b358448d59af9438dfb7398a4909e0).
- Fixed crash caused by null pointer dereference in `HydraCreateForeignObject`, [8ba1cab](https://github.com/ytsaurus/ytsaurus/commit/8ba1cabb0ccf6c958374ea2fe4948762609a1823).
- Removed legacy ZooKeeper shard code, [c0482a7](https://github.com/ytsaurus/ytsaurus/commit/c0482a74746e10d16900608dfdf7c4455a81a751).
- Fixed reliability status updates for exec nodes, [4e9becf](https://github.com/ytsaurus/ytsaurus/commit/4e9becf2dc2992ad8a714dcdcf404f1ba40758bf).
- Fixed race between transaction commit and exported object cleanup, [5b72aad](https://github.com/ytsaurus/ytsaurus/commit/5b72aad7cafe1f0b44dd76acced1a52ecd4e7264).
- Eliminated false alerts caused by harmless exported object races, [8a80023](https://github.com/ytsaurus/ytsaurus/commit/8a80023a50661a42abba895aacda70d118adf845).
- Fixed crash when setting a YSON dictionary with duplicate keys into a custom attribute, [867354b](https://github.com/ytsaurus/ytsaurus/commit/867354bffa92a4a9991d360c770e1219c4c12f81).
- Fixed row comparison logic in shallow merge validation, [404a790](https://github.com/ytsaurus/ytsaurus/commit/404a790b962ee26f1a4c2085d5bfc8b223ff6199).
- Cleared pending container restarts to avoid reinitialization issues, [1f0d9f7](https://github.com/ytsaurus/ytsaurus/commit/1f0d9f7bccc8faae5302f68e32589f1d1a963068).
- Fixed crash when reading `@local_scan_flags` attribute, [d8743cb](https://github.com/ytsaurus/ytsaurus/commit/d8743cbae113dc99a3c53612f253e45c8eab4b08).
- Fixed non-deterministic error messages caused by unordered YSON fields, [d907ada](https://github.com/ytsaurus/ytsaurus/commit/d907ada9984b6b711e4d5dd02d36d9e333df5dbb).
- Fixed compatibility issue with imaginary chunk locations, [f591951](https://github.com/ytsaurus/ytsaurus/commit/f591951182e9555c7ca58173ec14a75e7b6d41a7).

#### Other

##### New Features & Changes:
- Added memory usage tracking for logging, [f0351fa](https://github.com/ytsaurus/ytsaurus/commit/f0351fa2aa2278c7ac804c93074e91d70e724138).
- Added memory usage tracking for chunk replica cache, [aa5053d](https://github.com/ytsaurus/ytsaurus/commit/aa5053d5b243a3356d95388fb51094e2ed43ea68).
- Added memory usage tracking for sensors, [8396ecd](https://github.com/ytsaurus/ytsaurus/commit/8396ecddde795abc6b52642c07fbe56b5b76581d).
- Introduced support for running multiple daemons in a single `ytserver` process (multidaemon mode), [2986da8](https://github.com/ytsaurus/ytsaurus/commit/2986da8386705b72d1f2de8b4a6a9de21b4c05ea).
- Added sanitization of monitoring labels, [5bebc7a](https://github.com/ytsaurus/ytsaurus/commit/5bebc7aabdc9439f49e63b8a9104e8256a273940).
- Added support for `decimal256` type (up to 76 digits precision), including Skiff format as `int256`, [implicit].
- Added `message_level_overrides` option to logging config for fine-grained runtime control, [4e13e2a](https://github.com/ytsaurus/ytsaurus/commit/4e13e2a32db1cb46272330c131c1c4ca3f50994d).

##### Fixes & Optimizations:
- Fixed a bug where queued heavy RPC requests could incorrectly reuse storage (e.g. trace context) from unrelated requests, [6bd19a0](https://github.com/ytsaurus/ytsaurus/commit/6bd19a014d512d4d5f2bcac54ae2087f65cce3f9).
- Improved tracking of row cache memory usage, [122fd89](https://github.com/ytsaurus/ytsaurus/commit/122fd89ce22e6eb1f6cb48672463a0c264af9069).
- Histogram counters now use 64-bit values, [94fe6d3](https://github.com/ytsaurus/ytsaurus/commit/94fe6d36acebe32f2609ff1b7be7547dbeeaa446).
- Fixed propagation of `TLargeColumnarStatisticsExt` in MAW, [bee2ba3](https://github.com/ytsaurus/ytsaurus/commit/bee2ba31007789d05a6799a7e38add72dab6a8b7).
- Fixed handling of unexpected state in data node heartbeat logic, [2e16721](https://github.com/ytsaurus/ytsaurus/commit/2e16721f4f2aec16089ee6eae580e8bca03347ba).
- Properly handled peer disconnects in the NBD server, [b1f2acf](https://github.com/ytsaurus/ytsaurus/commit/b1f2acf7363a9e5dfc123f2cdf60483d3e9a37cb).
- NBD queue now uses a dedicated thread pool, configurable via dynamic config, [5e9c7fe](https://github.com/ytsaurus/ytsaurus/commit/5e9c7fe3167aa81403b0b4f8ced3cafd0ad544a2).



{% endcut %}


{% cut "**24.2.1**" %}

**Release date:** 2025-07-28


_To install YTsaurus Server 24.2.1 [update](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.22.0) the k8s-operator to version 0.22.0._

#### Proxy
Features:
- Support HTTP proxies in `discover_proxies` handler, [0dc02db](https://github.com/ytsaurus/ytsaurus/commit/0dc02db399a7e3e6255f8716102add5bf404bd39).
- Support full result table in YQL queries result, [2d7b0d3](https://github.com/ytsaurus/ytsaurus/commit/2d7b0d3e761361ff3157e7a36465ee79478ab3c7).

Fixes:
- Set attribute `treat_as_queue_producer=%true` during `queue_producer` creation, [1ee68e1](https://github.com/ytsaurus/ytsaurus/commit/1ee68e1fa7409d125605ba4047f655532f42f6ee).
- Fix `PartitionTables` handler for ordered mode, [100bdc4]( https://github.com/ytsaurus/ytsaurus/commit/100bdc425c787902212c041822ef9557ebb7b932).

#### Master
Fixes:
- Fix a crash on `GetIteratorOrCrash` in `TChunkMerger::HydraFinalizeChunkMergeSessions`, [5eda095]( https://github.com/ytsaurus/ytsaurus/commit/5eda0952c2a3453d853a83c24b16b3a5d7f31d49).
- Fix for Object Service hang ups, [31e2dfd](https://github.com/ytsaurus/ytsaurus/commit/31e2dfd7dbe7ad001f11d30fa1c59bc7a7a9ca21).
- Fix a data race on cache cookies in Object Service, [1d79d8d](https://github.com/ytsaurus/ytsaurus/commit/1d79d8d37d81438389a5280c595e8254cfc6f678).

#### Queue Agent
Features:
- Add a flag to select queue export implementation, [3ec2e69](https://github.com/ytsaurus/ytsaurus/commit/3ec2e6945cf0af0f64d16e94ee5aa66e902abbf1).
Fixes:

Fixes:
- Fix `init_queue_agent_state` script to handle restart cases properly, [ae43add](https://github.com/ytsaurus/ytsaurus/commit/ae43add3ecd560c1f49fc41b40a2b0bd6b1f402f).
- Fix potential data loss in case of multiple exports per queue due to incorrect merging of queue export progresses, [c44e13b](https://github.com/ytsaurus/ytsaurus/commit/c44e13b5271ff91c5860c91448fbd704924ddf35).

#### Tablet Balancer
Fixes:
- Fix deviation recalculation during parameterized merge right index search, [b7df500](https://github.com/ytsaurus/ytsaurus/commit/b7df5009926f4d0386fa924902da46e71e63bb54)

#### Other
Feature:
- Support `ytprof` for all components.

{% endcut %}


{% cut "**24.2.0**" %}

**Release date:** 2025-03-19


_To install YTsaurus Server 24.2.0 [update](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.22.0) the k8s-operator to version 0.22.0._

---
#### Known Issue
- Expanding a cluster with new master cells is temporarily disabled due to a bug. This issue will be resolved in the upcoming 25.1 version.

---

#### Scheduler and GPU
New Features & Changes:
- Added support for ACO-based access control in operations.
- Introduced `get_job_trace` method in the jobs API.
- Added an option to fail an operation if started in a non-existent pool.
- Enriched configuration options for offloading operations to pool trees.

Fixes & Optimizations:
- Fixed scheduling issues with new allocations immediately after operation suspension.
- Optimized fair share updates within the control thread.

#### Queue Agent
New Features & Changes:
- Queue exports are now considered when trimming replicated and chaos replicated table queues.
- Introduced sum aggregation for lag metrics in consumer partitions.
- Refactor queue exports: add retries and rate limiting.

Fixes & Optimizations:
- Fixed a possible queue controller suspension by adding a timeout to `GetOrderedTabletSafeTrimRowCount` requests.
- Corrected lock options when acquiring a shared lock for the export directory.
- Resolved expiration issues of clients for chaos queues and consumers when the cluster connection changes.

#### Proxy
New Features & Changes:
- Supported YAML format for structured data. See details in the RFC: [YAML-format support](https://github.com/ytsaurus/ytsaurus/wiki/%5BRFC%5D-YAML-format-support).
- Introduced create_user_if_not_exists config flag to disable automatic user creation during OAuth authentication. [Issue](https://github.com/ytsaurus/ytsaurus/issues/930).
- Added `cache_key_mode` parameter for controlling credential caching granularity.
- Implemented a new handler for retrieving job trace events.

Fixes & Optimizations:
- The `discover_proxies` handler now returns an error when the proxy type is `http`.
- Fixed a heap buffer overflow in the Arrow parser.
- If insufficient memory is available to handle RPC responses, a retryable `Unavailable` error will now be returned instead of the non-retryable `MemoryPressure` error.
- Optimized the `concatenate` method.

#### Kafka proxy
Introduce a new component: the Kafka Proxy. This MVP version enables integration with YTsaurus queues using the Kafka protocol. It currently supports a minimal API for writing to queues and reading with load balancing through consumer groups.

#### Dynamic Tables
New Features & Changes:
- Introduced Versioned Remote Copy for tables with hunks.

Fixes & Optimizations:
- Fixed issues with secondary indices in multi-cell clusters (especially large clusters).
- Improved stability and performance of chaos replication.
  
#### MapReduce
New Features & Changes:
- Disallowed cluster_connection in remote copy operations.
- Introduced single-chunk teleportation for auto-merge operations.
- Merging of tables with compatible (not necessarily matching) schemas is supported.
- Refactored code in preparation for gang operation introduction.
- Refactored code to enable job-level allocation reuse.
- Improved per-job directory logging in job-proxy mode.

Fixes & Optimizations:
- Optimized job resource acquisition in exec nodes.
- Fixed cases of lost metrics in exec nodes.

#### Master Server
New Features & Changes:
- Added an option to fetch input/output table schemas from an external cell using a schema ID.
- Deprecated the list node type; its creation is now forbidden.
- Introduced a new write target allocation strategy using the “two random choices” algorithm.
- Implemented an automatic mechanism to disable replication to data nodes in failing data centers. This can be configured in `//sys/@config/chunk_manager/data_center_failure_detector`.
- Introduced pessimistic validation for resource usage increases when changing the primary medium.
- Forbidden certain erasure codecs in remote copy operations.
- Added node groups attribute for node

Fixes & Optimizations:
- Fixed a race condition between transaction coordinator commit and cell unref for exported objects, [8d6721a](https://github.com/ytsaurus/ytsaurus/commit/8d6721a16bb6a1bc26c9f0d1dc5506f32635e6b6).
- Fixed manual Cypress node merging for Scheduler transactions, [f87a2ad](https://github.com/ytsaurus/ytsaurus/commit/f87a2ad466c2352be9ba7bfee6e7d93796a9eb6a).
- Fixed master crash when setting a YSON dictionary with duplicate keys in a custom attribute, [0cfad80](https://github.com/ytsaurus/ytsaurus/commit/0cfad80f415c23233ca748e345cd9af91169f4c3).
- Fixed row comparison in shallow merge validation to prevent job failures, [3c282d4](https://github.com/ytsaurus/ytsaurus/commit/3c282d4e9f50aa00d861b7a6f1ca388fea18e51d).
- Fixed a crash when reading `@local_scan_flags` attribute, [5b4c954](https://github.com/ytsaurus/ytsaurus/commit/5b4c954c09ac6e1adc55aa6a5d7baff8f894fb61).
- Fixed non-deterministic YSON struct field loading that could cause a “state hashes differ” alert due to inconsistent error messages when multiple required fields were missing, [0553e21](https://github.com/ytsaurus/ytsaurus/commit/0553e2182a0df502592abdd1fcd8ac3c6afd64ad).
- Fixed an issue where nodes held by transactions interfered with cleanup triggered by `expiration_time` attribute.
- Fixed a bug that caused account metrics to break when adding a new account.
- Fixed a bug in attribute-based access control that caused the first entry to always be evaluated.
- Fixed an issue where Hydra followers could become indefinitely stuck after a lost mutation.
- Limited chunk list count per chunk merger session to prevent master overload.
- Fixed an incorrect check for the state of a node during the removal process.
- Improved incremental heartbeat reporting to prevent chunks from getting stuck in the destroyed queue.
- Optimized chunk merger by reducing unnecessary requisition updates.



{% endcut %}


{% cut "**24.1.0**" %}

**Release date:** 2024-11-07


_To install YTsaurus Server 24.1.0 [update](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases) the k8s-operator to version 0.17.0._

#### Scheduler and GPU
Features and changes:
- Support prioritization of pools during strong guarantee adjustment due to insufficient total cluster resources.
- Support prioritization of operations during module assignment stage of the GPU scheduling algorithm.
- Support job resource demand restrictions per pool tree.
- Add custom TTL for jobs in operation archive.
- Add user job trace collection in Trace Event Format.
- Refactor exec node config and Orchid.
- Logically separate jobs and allocations.
- Add configurable input data buffer size in jobs for more efficient interrupts.

Fixes and optimizations:
- Fix exec node heartbeat tracing throughout scheduler and controller agents.
- Optimize general allocation scheduling algorithm and fair share computation.
- Optimize scheduler <-> CA and exec node <-> CA heartbeats processing.

#### Queue Agent
Features:
- Treat static queue export the same way as vital consumer during queues trimming, so not exported rows will not be trimmed.
- Add functionality for banning queue agent instances via cypress attribute.
- Take cumulative data weight and timestamp from consumer meta for consumer metrics.


Fixes:
- Fix bug in handling of queues/consumers with invalid attributes (e.g. `auto_trim_config`).
- Fix alerts visibility from `@queue_status` attribute.
- Do not ignore consumers higher than queue size.
- Rename `write_registration_table_mapping` -> `write_replicated_table_mapping` in dynamic config.
- Take shared lock instead of exclusive lock on static export destination directories.

#### Proxy
Features:
- Implement queue producer handlers for exactly once pushing in queues (`PushQueueProducer`, `CreateQueueProducerSession`).
- Add `queue_consumer` and `queue_producer` object type handler, so they can be created without explicitly schema specification. Example: `yt create queue_consumer <path>`.
- Support retries of cross cell copying.
- Add float and date types in Arrow format.
- Add memory tracking for `read_table` requests.
- Drop heavy requests if there is no more memory.
- Send `bytes_out` and `bytes_in` metrics during request execution.
- Store `cumulative_data_weight` and `timestamp` in consumer meta.
- Rename `PullConsumer` -> `PullQueueConsumer` and `AdvanceConsumer` -> `AdvanceQueueConsumer`. Old handlers continue to exists for now for backward compatibility reasons.

CHYT:
- Add authorization via X-ClickHouse-Key HTTP-header.
- Add sticky query distribution based on session id/sticky cookie.
- Add a new "/chyt" http handler for chyt queries ("/query" handler is deprecated but still works for backward compatibility).
- Add ability to allocate a separate port for the new http handler to support queries without a custom URL path.
- The clique alias may be specified via "chyt.clique_alias" or "user" parameters (only for new handlers).
- Make HTTP GET requests read-only for compatibility with ClickHouse  (only for new handlers).

Fixes:
- Fill dictionary encoding index type in Arrow format.
- Fix null, void and optional composite columns in Arrow format.
- Fix `yt.memory.heap_usage` metrics.

#### Dynamic Tables
Features:
- Secondary Indexes: basic, partial, list, and unique indexes.
- Optimize queries which group and order by same keys.
- Balance tablets using load factor (requires standalone tablet balancer).
- Shared write lock - write to same row from different transactions without blocking.
- Rpc proxy client balancer based on power of two choices algorithm.
- Compression dictionary for Hunks and Hash index.
  
#### MapReduce
Features:
- Support input tables from remote clusters in operations.
- Improve control over how data is split into jobs for ML training applications.
- Support read by latest timestamp in MapReduce operations over dynamic tables.
- Disclose less configuration information to a potential attacker.

Fixes:
- Fix teleportation of a single chunk in an unordered pool.
- Fix agent disconnect on removal of an account.
- Fix the inference of intermediate schemas for inputs with column filters.
- Fix controller agent crash on incompatible user statistic paths.

Optimizations:
- Add JobInputCache: in-memory cache on exe nodes, storing data read by multiple jobs running on the same node.

#### Master Server

Features:
- Tablet cells Hydra persistence data is now primarily stored at the new location `//sys/hydra_persistence` by default. The duality with the previous location (`//sys/tablet_cells`) will be resolved in the future releases.
- Support inheritance of `@chunk_merger_mode` after copy into directory with set `@chunk_merger_mode`.
- Add backoff rescheduling for nodes merged by chunk merger in case of a transient failure to merge them.
- Add an option to use the two random choices algorithm when allocating write targets.
- Add the add-maintenance command to CLI.
- Support intra-cell cross-shard link nodes.
- Propagate transaction user to transaction replicas for the sake of proper accounting of the cpu time spent committing or aborting them.
- Propagate knowledge of new master cells dynamically to other cluster components and shorten downtime when adding new master cells.

Optimizations:
- Reduce master server memory footprint by reducing the size of table nodes.
- Speed up removal jobs on data nodes.
- Move exec node tracker service away from automaton thread.
- Non-data nodes are now disposed immediately (instead of per location disposal) and independently from data-nodes.
- Offload invoking transaction replication requests from automaton thread.

Fixes:
- Fix nullptr dereferencing in resolution of queue agent and yql agent attributes.
- Respect medium override in IO engine on node restart.
- Fix rebalancing mode in table's chunk tree after merging branched tables.
- Fix sanitizing hostnames in errors for cellar nodes.
- Fix losing trace context for some callbacks and rpc calls.
- Fix persistence of `@last_seen_time` attribute for users.
- Fix handling unknown chunk meta extensions by meta aggregating writer.
- Fix nodes crashing on heartbeat retries when masters are down for a long time.
- Fix table statistics being inconsistent between native and external cells after copying the table mid statistics update.
- Fix logical request weight being accidentally dropped in proxying chunk service.
- Fix a crash that occasionally occurred when exporting a chunk.
- Fix tablet cell lease transactions getting stuck sometimes.
- Native client retries are now more reliable.
- Fix primary cell chunk hosting for multicell.
- Fix crash related to starting incumbency epoch until recovery is complete.
- Restart elections if changelog store for a voting peer is locked in read-only (Hydra fix for tablet nodes).
- Fix crashing on missing schema when importing a chunk.
- Fix an epoch restart-related crash in expiration tracker.
- In master cell directory, alert on an unknown cell role instead of crashing.

#### Misc
Features:
- Add ability to redirect stdout to stderr in user jobs (`redirect_stdout_to_stderr` option in operation spec).
- Add dynamic table log writer.

{% endcut %}


{% cut "**23.2.1**" %}

**Release date:** 2024-07-31


#### Scheduler and GPU
Features:
  * Disable writing `//sys/scheduler/event_log` by default.
  * Add lightweight running operations.

Fixes:
  * Various optimizations in scheduler
  * Improve total resource usage and limits profiling.
  * Do not account job preparation time in GPU statistics.

#### Queue Agent
Fixes:
  * Normalize cluster name in queue consumer registration.

#### Proxy
Features:
  * RPC proxy API for Query Tracker.
  * Changed format and added metadata for issued user tokens.
  * Support rotating TLS certificates for HTTP proxies.
  * Compatibility with recent Query Tracker release.

Fixes:
  * Do not retry on Read-Only response error.
  * Fix standalone authentication token revokation.
  * Fix per-user memory tracking (propagate allocation tags to child context).
  * Fix arrow format for optional types.

#### Dynamic Tables
Features:
  * Shared write locks.
  * Increased maximum number of key columns to 128.
  * Implemented array join in YT QL.

Fixes:
  * Cap replica lag time for tables that are rarely written to.
  * Fix possible journal record loss during journal session abort.
  * Fix in backup manager.
  * Fix some bugs in chaos dynamic table replication.
  
#### MapReduce
Features:
  * Combined per-locaiton throttlers limiting total in+out bandwidth.
  * Options in operation spec to force memory limits on user job containers.
  * Use codegen comparator in SimpleSort & PartitionSort if possible.

Fixes:
  * Better profiling tags for job proxy metrics.
  * Fixes for remote copy with erasure repair.
  * Fix any_to_composite converter when multiple schemas have similarly named composite columns.
  * Fixes for partition_table API method.
  * Fixes in new live preview.
  * Do not fail jobs with supervisor communication failures.
  * Multiple retries added in CRI executor/docker image integration.
  * Cleaned up job memory statistics collection, renamed some statistics.

#### Master Server
Features:
  * Parallelize and offload virtual map reads.
  * Emergency flag to disable attribute-based access control.
  * Improved performance of transaction commit/abort.
  * Enable snapshot loading by default.

Fixes:
  * Fixes and optimizations for Sequoia chunk replica management.
  * Fix multiple possible master crashes.
  * Fixes for master update with read-only availability.
  * Fixes for jammed incremental hearbeats and lost replica update on disabled locations.
  * Fix per-account sensors on new account creation.

#### Misc
Features:
  * Config exposure via orchid became optional.
  * Support some c-ares options in YT config.
  * Support IP addresses in RPC TLS certificate verification.

Fixes:
   * Fix connection counter leak in http server.
   * Track and limit memory used by queued RPC requests.
   * Better memory tracking for RPC connection buffers.
   * Fix address resolver configuration.


{% endcut %}


{% cut "**23.2.0**" %}

**Release date:** 2024-02-29


#### Scheduler

Many internal changes driven by developing new scheduling mechanics that separate jobs from resource allocations at exec nodes. These changes include modification of the protocol of interaction between schedulers, controller agents and exec nodes, and adding tons of new logic for introducing allocations in exec nodes, controller agents and schedulers.

List of significant changes and fixes: 
  - Optimize performance of scheduler's Control and NodeShard threads.
  - Optimize performance of the core scheduling algorithm by considering only a subset of operations in most node heartbeats.
  - Optimize operation launch time overhead by not creating debug transaction if neither stderr or core table have been specified.
  - Add priority scheduling for pools with resource guarantees.
  - Consider disk usage in job preemption algorithm.
  - Add operation module assignment preemption in GPU segments scheduling algorithm.
  - Add fixes for GPU scheduling algorithms.
  - Add node heartbeat throttling by scheduling complexity.
  - Add concurrent schedule job exec duration throttling.
  - Reuse job monitoring descriptors within a single operation.
  - Support monitoring descriptors in map operations.
  - Support filtering jobs with monitoring descriptors in `list_jobs` command.
  - Fix displaying jobs which disappear due to a node failure as running and "stale" in UI.
  - Improve ephemeral subpools configuration.
  - Hide user tokens in scheduler and job proxy logs.
  - Support configurable max capacity for pipes between job proxy and user job.

#### Queue Agent

Aside small improvements, the most significant features include the ability to configure periodic exports of partitioned data from queues into  static tables and the support for using replicated and chaos dynamic tables as queues and consumers.

Features: 
- Support chaos replicated tables as queues and consumers.
- Support snapshot exports from queues into static tables.
- Support queues and consumers that are symbolic links for other queues and consumers.
- Support trimming of rows in queues by lifetime duration.
- Support for registering and unregistering of consumer to queue from different cluster.

Fixes:
- Trim queues by its `object_id`, not by `path`.
- Fix metrics of read rows data weight via consumer.
- Fix handling frozen tablets in queue.

#### Proxy
Features:
- Add ability to call `pull_consumer` without specifying `offset`, it will be taken from `consumer` table.
- Add `advance_consumer` handler for queues.
- Early implementation of `arrow` format to read/write static tables.
- Support type conversions for inner fields in complex types.
- Add new per user memory usage monitoring sensors in RPC proxies.
- Use ACO for RPC proxies permission management.
- Introduce TCP Proxies for SPYT.
- Support of OAuth authorization.

Fixes:
- Fix returning requested system columns in `web_json` format.


#### Dynamic Tables
Features:
- DynTables Query language improvments:
    - New range inferrer.
    - Add various SQL operators (<>, string length, ||, yson_length, argmin, argmax, coalesce).
- Add backups for tables with hunks.
- New fair share threadpool for select operator and network.
- Add partial key filtering for range selects.
- Add overload controller.
- Distribute load among rpc proxies more evenly.
- Add per-table size metrics.
- Store heavy chunk meta in blocks.


#### MapReduce

Features:
- RemoteСopy now supports cypress file objects, in addition to tables.
- Add support for per job experiments.
- Early implementation of CRI (container runtime interface) job environment & support for external docker images.
- New live preview for MapReduce output tables.
- Add support for arrow as an input format for MapReduce.
- Support GPU resource in exec-nodes and schedulers.

Enhancements:
- Improve memory tracking in data nodes (master jobs, blob write sessions, p2p tracking).
- Rework memory acccounting in controller agents.

#### Master Server

Noticeable/Potentially Breaking Changes:
  - Read requests are now processed in a multithreaded manner by default.
  - Read-only mode now persists between restarts. `yt-admin master-exit-read-only` command should be used to leave it.
  - `list_node` type has been deprecated. Users are advised to use `map_node`s or `document`s instead.
  - `ChunkService::ExecuteBatch` RPC call has been deprecated and split into individual calls. Batching chunk service has been superseded by proxying chunk service.
  - New transaction object types: `system_transaction`, `nested_system_transaction`. Support for transaction actions in regular Cypress transactions is now deprecated.
  - Version 2 of the Hydra library is now enabled by default. Version 1 is officially deprecated.

Features:
  - It is now possible to update master-servers with no read downtime via leaving non-voting peers to serve read requests while the main quorum is under maintenance.
  - A data node can now be marked as pending restart, hinting the replicator to ignore its absence for a set amount of time to avoid needless replication bursts.
  - The `add_maintenance` command now supports HTTP- and RPC-proxies.
  - Attribute-based access control: a user may now be annotated with a set of tags, while an access-control entry (ACE) may be annotated with a tag filter.

Optimizations & Fixes:
  - Response keeper is now persistent. No warm-up period is required before a peer may begin leading.
  - Chunk metadata now include schemas. This opens up a way to a number of significant optimizations.
  - Data node heartbeat size has been reduced.
  - Chunks and chunk lists are now loaded from snapshot in parallel.
  - Fixed excessive memory consumption in multicell configurations.
  - Accounting code has been improved to properly handle unlimited quotas and avoid negative master memory usage.

Additionally, advancements have been made in the Sequoia project dedicated to scaling master server by offloading certain parts of its state to dynamic tables. (This is far from being production-ready yet.)

#### Misc

Enhancements:
- Add rpc server config dynamization.
- Add support for peer alternative hostname for Bus TLS.
- Properly handle Content-Encoding in monitoring web-server.
- Bring back "host" attribute to errors.
- Add support for --version option in ytserver binaries.
- Add additional metainformation in yson/json server log format (fiberId, traceId, sourceFile).


{% endcut %}

