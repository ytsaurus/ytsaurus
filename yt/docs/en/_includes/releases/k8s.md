## Kubernetes operator


Is released as helm charts on [Github Packages](https://github.com/ytsaurus/ytsaurus-k8s-operator/pkgs/container/ytop-chart).




**Releases:**

{% cut "**0.22.0**" %}

**Release date:** 2025-03-07


## Features
* Update to YTsaurus 24.2 is supported

## Minor
* Add lost CA bundle and TLS secrets VolumeMounts for jobs container by @imakunin in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/449
* Add bus client configuration by @imakunin in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/450

## Experimental
* Add multiple update selectors by @wilwell in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/383
* Add blocked components column to kubectl output by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/459

## New Contributors
* @imakunin made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/449

**Full Changelog**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.21.0...release/0.22.0

{% endcut %}


{% cut "**0.21.0**" %}

**Release date:** 2025-02-10


## Features
* Support the ability to deploy a Kafka proxy by @savnadya in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/407

## Minor
* Add config for kind with audit log by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/441

## Bugfix
* Preserve object finalizers by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/440
* Set quota and min_disk_space for locations by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/445
* Fix zero port if no monitoring port configured by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/447


{% endcut %}


{% cut "**0.20.0**" %}

**Release date:** 2025-01-20


## Minor
* Support not creating non-existing users by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/416
* Added DNSConfig into Instance and YTsaurusSpec by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/420
* Enable real chunks job by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/412
* Add log_manager_template for job proxy by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/428

## Release notes
This release makes yt operator compatible with ytsaurus 24.2.
Update to this version will launch job for setting correct enable_real_chunks_value values in cypress and exec nodes will be updated with a new config.

**Full Changelog**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.19.0...release/0.20.0

{% endcut %}


{% cut "**0.19.0**" %}

**Release date:** 2025-01-09


## Minor
* Configure yqla mrjob syslibs by @Krisha11 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/409
## Bugfix
* Add yqla update job by @Krisha11 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/387

**Full Changelog**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.18.1...release/0.19.0

{% endcut %}


{% cut "**0.18.1**" %}

**Release date:** 2024-12-13


## Minor
* more validation by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/393
## Bugfix
* Fix updates for named cluster components @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/401

**Full Changelog**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.18.0...release/0.18.1

{% endcut %}


{% cut "**0.18.0**" %}

**Release date:** 2024-11-26


## Warning
This release has known bug, which broke update for YTsaurus components with non-empty names (names can be set for data/tablet/exec nodes) and roles (can be set for proxies).
The bug was fixed in [0.18.1](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.18.1).

## Features
* Implemented RemoteTabletNodes api by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/372

## Minor
* Update sample config for cluster with TLS by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/369
* Remove DataNodes from StatelesOnly update by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/371
* Added namespacedScope value to the helm chart by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/376
* Upgrade crd-ref-docs by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/379
* Add observed generation for remote nodes by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/382
* Support different controller families in strawberry configuration by @dmi-feo in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/355
* kata-compat: mount TLS-related files to a separate directory by @kruftik in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/388
* Support OAuth login transformations by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/397
* Add diff for static config update case by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/398

## Bugfix
* Fix observed generation by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/373
* Fix YQL agent dynamic config creation by @savnadya in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/377
* Fix logging in chyt_controller by @dmi-feo in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/370
* Fix strawberry container name by @dmi-feo in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/375
* Use expected instance count as default for minimal ready count by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/395

## New Contributors
* @dmi-feo made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/370

**Full Changelog**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.17.0...release/0.18.0

{% endcut %}


{% cut "**0.17.0**" %}

**Release date:** 2024-10-11


## Minor
* Separate CHYT init options into makeDefault and createPublicClique by @achulkov2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/347
## Bugfix
* Fix queue agent init script usage for 24.* by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/356


{% endcut %}


{% cut "**0.16.2**" %}

**Release date:** 2024-09-13


## Bugfix
* Fix strawberry controller image for 2nd job by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/345


{% endcut %}


{% cut "**0.16.1**" %}

**Release date:** 2024-09-13


## Warning
This release has a bug if Strawberry components is enabled.
Use 0.16.2 instead.

## Bugfix
* Revert job image override for UI/strawberry by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/344 — the bug was introduced in 0.16.0


{% endcut %}


{% cut "**0.16.0**" %}

**Release date:** 2024-09-12


## Warning
This release has a bug for a configuration where UI or Strawberry components are enabled and some of their images were overridden (k8s init jobs will fail for such components).
Use 0.16.2 instead.

## Minor
* Add observedGeneration field to the YtsaurusStatus by @wilwell in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/333
* Set statistics for job low cpu usage alerts by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/335
* Add nodeSelector for UI and Strawberry by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/338
* Init job creates from InstanceSpec image if specified by @wilwell in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/336
* Add tolerations and nodeselectors to jobs by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/342

## New Contributors
* @wilwell made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/333


{% endcut %}


{% cut "**0.15.0**" %}

**Release date:** 2024-09-04


## Backward incompatible changes
1. Component pod labels were refactored in #326 and changes are:
- `app.kubernetes.io/instance` was removed
- `app.kubernetes.io/name` was Ytsaurus before, now it contains component type
- `app.kubernetes.io/managed-by` is `"ytsaurus-k8s-operator"` instead of `"Ytsaurus-k8s-operator"`

2. Deprecated `chyt` field in the main [YTsaurus spec](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/docs/api.md#ytsaurusspec) was removed, use `strawberry` field with the same schema instead.

## Minor
* Added tolerations for Strawberry by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/328
* Refactor label names for components by @achulkov2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/326

## Experimental
* RemoteDataNodes by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/330


{% endcut %}


{% cut "**0.14.0**" %}

**Release date:** 2024-08-22


## Backward incompatible changes
Before this release `StrawberryController` was unconditionally configured with `{address_resolver={enable_ipv4=%true;enable_ipv6=%true}}` in its static config. From now on it respects common `useIpv6` and `useIpv4` fields, which can be set in the [YtsaurusSpec](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/docs/api.md#ytsaurusspec).
If for some reason it is required to have configuration different from
```yaml
useIpv6: true
useIpv4: true
```
for the main Ytsaurus spec and at the same time `enable_ipv4=%true;enable_ipv6=%true` for the `StrawberryController`, it is possible to achieve that by using `configOverrides` ConfigMap with
```yaml
data:
    strawberry-controller.yson: |
    {
      controllers = {
        chyt = {
          address_resolver = {
            enable_ipv4 = %true;
            enable_ipv6 = %true;
          };
        };
      };
    }
```

## Minor
* Add no more than one ytsaurus spec per namespace validation by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/305
* Add strategy, nodeSelector, affinity, tolerations by @sgburtsev in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/321
* Add forceTcp and keepSocket options by @leo-astorsky in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/324

## Bugfixes
* Fix empty volumes array in sample config by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/318

## New Contributors
* @leo-astorsky made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/324


{% endcut %}


{% cut "**0.13.1**" %}

**Release date:** 2024-07-30


### Bugfixes
* Revert deprecation of useInsecureCookies in #310 by @sgburtsev in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/317

The field `useInsecureCookies` was deprecated in the previous release in a not backwards compatible way, this release fixes it. It is now possible to configure the secureness of UI cookies (via the `useInsecureCookies` field) and the secureness of UI and HTTP proxy interaction (via the `secure` field) independently.


{% endcut %}


{% cut "**0.13.0**" %}

**Release date:** 2024-07-23


## Features
* Add per-component terminationGracePeriodSeconds by @koct9i in https://github.com/ytsaurus/yt-k8s-operator/pull/304
* Added externalProxy parameter for UI by @sgburtsev in https://github.com/ytsaurus/yt-k8s-operator/pull/308
* Size as Quantity in LogRotationPolicy by @sgburtsev in https://github.com/ytsaurus/yt-k8s-operator/pull/309
* Use `secure` instead of `useInsecureCookies`, pass caBundle to UI by @sgburtsev in https://github.com/ytsaurus/yt-k8s-operator/pull/310

## Minor
* Add all YTsaurus CRD into category "ytsaurus-all" "yt-all" by @koct9i in https://github.com/ytsaurus/yt-k8s-operator/pull/311

## Bugfixes
* Operator should detect configOverrides updates by @l0kix2 in https://github.com/ytsaurus/yt-k8s-operator/pull/314



{% endcut %}


{% cut "**0.12.0**" %}

**Release date:** 2024-06-28


## Features
* More options for store locations. by @sgburtsev in https://github.com/ytsaurus/yt-k8s-operator/pull/294
  * data nodes upper limit for `low_watermark` increased from 5 to 25Gib;
  * data nodes' `trash_cleanup_watermark` will be set equal to the `lowWatermark` value from spec
  * `max_trash_ttl`can be configured in spec
* Add support for directDownload to UI Spec by @kozubaeff in https://github.com/ytsaurus/yt-k8s-operator/pull/257
  * `directDownload` for UI can be configured in the spec now. If omitted or set to `true`, UI will have current default behaviour (use proxies for download), if set to `false` — UI backend will be used for downloading.

## New Contributors
* @sgburtsev made their first contribution in https://github.com/ytsaurus/yt-k8s-operator/pull/294


{% endcut %}


{% cut "**0.11.0**" %}

**Release date:** 2024-06-27


### Features
* SetHostnameAsFQDN option is added to all components. Default is true by @qurname2 in https://github.com/ytsaurus/yt-k8s-operator/pull/302
* Add per-component option hostNetwork by @koct9i in https://github.com/ytsaurus/yt-k8s-operator/pull/287

### Minor
* Add option for per location disk space quota by @koct9i in https://github.com/ytsaurus/yt-k8s-operator/pull/279
* Add into exec node pods environment variables for CRI tools by @koct9i in https://github.com/ytsaurus/yt-k8s-operator/pull/283
* Add per-instance-group podLabels and podAnnotations by @koct9i in https://github.com/ytsaurus/yt-k8s-operator/pull/289
* Sort status conditions for better readability by @koct9i in https://github.com/ytsaurus/yt-k8s-operator/pull/290
* Add init containers for exec node by @koct9i in https://github.com/ytsaurus/yt-k8s-operator/pull/288
* Add loglevel "warning" by @koct9i in https://github.com/ytsaurus/yt-k8s-operator/pull/292
* Remove mutating/defaulter webhooks by @koct9i in https://github.com/ytsaurus/yt-k8s-operator/pull/296

### Bugfixes
* fix exec node resource calculation on non-isolated CRI-powered job environment by @kruftik in https://github.com/ytsaurus/yt-k8s-operator/pull/277



{% endcut %}


{% cut "**0.10.0**" %}

**Release date:** 2024-06-07


### Features
### Minor
 - Add everyone-share QT ACO by @Krisha11 in #272
 - Add channel in qt config by @Krisha11 in #273
 - Add option for per location disk space quota #279
### Bugfixes
- Fix exec node resource calculation on non-isolated CRI-powered job environment #277



{% endcut %}


{% cut "**0.9.1**" %}

**Release date:** 2024-05-30


### Features
### Minor
 - Add 'physical_host' to cypress_annotations for CMS and UI сompatibility #252
 - added WATCH_NAMESPACE env and LeaderElectionNamespace #168
 - Add configuration for solomon exporter: specify host and some instance tags #258
 - Add sidecars support to primary masters containers #259
 - Add option for containerd registry config path #264
### Bugfixes
 - Fix CRI job environment for remote exec nodes #261


{% endcut %}


{% cut "**0.9.0**" %}

**Release date:** 2024-04-23


### Features
- Add experimental (behaviour may change) UpdateSelector field #211 to be able to update components separately
### Minor
- Enable TmpFS when possible #235
- Disable disk quota for slot locations #236
- Forward docker image environment variables to user job #248
### Bugfixes
- Fix flag doNotSetUserId #243


{% endcut %}


{% cut "**0.8.0**" %}

**Release date:** 2024-04-12


### Features
### Minor
- Increased default value for MaxSnapshotCountToKeep and MaxChangelogCountToKeep
- Tune default bundle replication factor #210
- Set EnableServiceLinks=false for all pods #218
### Bugfixes
- Fix authentication configuration for RPC Proxy #207
- Job script updated on restart #224
- Use secure random and base64 for tokens #202
- Fix running jobs with custom docker_image when default job image is not set #217

{% endcut %}


{% cut "**0.7.0**" %}

**Release date:** 2024-04-04


### Features
  * Add Remote exec nodes support #75
  * Add MasterCaches support #122
  * Enable TLS certificate auto-update for http proxies #167
  * CRI containerd job environment #105

### Minor
  * Support RuntimeClassName in InstanceSpec
  * Configurable monitoring port #146
  * Not triggering full update for data nodes update
  * Add ALLOW_PASSWORD_AUTH to UI #162
  * Readiness checks for strawberry & UI
  * Medium is called domestic medium now #88
  * Tune tablet changelog/snapshot initial replication factor according to data node count #185
  * Generate markdown API docs
  * Rename operations archive #116
  * Configure cluster to use jupyt #149
  * Fix QT ACOs creation on cluster update #176
  * Set ACLs for QT ACOs, add everyone-use ACO #181
  * Enable rpc proxy in job proxy #197
  * Add yqla token file in container #140

### Bugfixes
  * Replace YQL Agent default monitoring port 10029 -> 10019


{% endcut %}


{% cut "**0.6.0**" %}

**Release date:** 2024-02-26


### Features
- Added support for updating masters of 23.2 versions
- Added the ability to bind masters to the set of nodes by node hostnames.
- Added the ability to configure the number of stored snapshots and changelogs in master spec
- Added the ability for users to create access control objects
- Added support for volume mount with mountPropagation = Bidirectional mode in execNodes
- Added access control object namespace "queries" and object "nobody". They are necessary for query_tracker versions 0.0.5 and higher.
- Added support for the new Cliques CHYT UI.
- Added the creation of a group for admins (admins).
- Added readiness probes to component statefulset specs

### Fixes
- Improved ACLs on master schemas
- Master and scheduler init jobs do not overwrite existing dynamic configs anymore.

### Tests
- Added flow to run tests on Github resources
- Added e2e to check that updating from 23.1 to 23.2 works
- Added config generator tests for all components
- Added respect KIND_CLUSTER_NAME env variable in e2e tests
- Supported local k8s port forwarding in e2e

### Backward Incompatible Changes
- `exec_agent` was renamed to `exec_node` in exec node config, if your specs have `configOverrides` please rename fields accordingly.



{% endcut %}


{% cut "**0.5.0**" %}

**Release date:** 2023-11-29


**Features**
- Added `minReadyInstanceCount` into Ytsaurus components which allows not to wait when all pods are ready.
- Support queue agent.
- Added postprocessing of generated static configs.
- Introduced separate UseIPv4 option to allow dualstack configurations.
- Support masters in host network mode.
- Added spyt engine in query tracker by default.
- Enabled both ipv4 and ipv6 by default in chyt controllers.
- Default CHYT clique creates as tracked instead of untracked.
- Don't run full update check if full update is not enabled (`enable_full_update` flag in spec).
- Update cluster algorithm was improved. If full update is needed for already running components and new components was added, operator will run new components at first, and only then start full update. Previously such reconfiguration was not supported.
- Added optional TLS support for native-rpc connections.
- Added possibility to configure job proxy loggers.
- Changed how node resource limits are calculated from `resourceLimits` and `resourceRequests`.
- Enabled debug logs of YTsaurus go client for controller pod.
- Supported dualstack clusters in YQL agent.
- Supported new config format of YQL agent.
- Supported `NodePort` specification for HTTP proxy (http, https), UI (http) and RPC proxy (rpc port). For TCP proxy NodePorts are used implicitly when NodePort service is chosen. Port range size and minPort are now customizable.

**Fixes**
- Fixed YQL agents on ipv6-only clusters.
- Fixed deadlock in case when UI deployment is manually deleted.

**Tests**
- e2e tests were fixed.
- Added e2e test for operator version compat.

{% endcut %}


{% cut "**0.4.1**" %}

**Release date:** 2023-10-03


**Features**
- Support per-instance-group config override
- Support TLS for RPC proxies

**Bug fixes**
- Fixed an error during creation of default `CHYT` clique (`ch_public`).

{% endcut %}


{% cut "**0.4.0**" %}

**Release date:** 2023-09-26


**Features**

- The operations archive will be updated when the scheduler image changes.
- Ability to specify different images for different components.
- Cluster update without full downtime for stateless components was supported.
- Updating of static component configs if necessary was supported.
- Improved SPYT controller. Added initialization status (`ReleaseStatus`).
- Added CHYT controller and the ability to load several different versions on one YTsaurus cluster.
- Added the ability to specify the log format (`yson`, `json` or `plain_text`), as well as the ability to enable recording of structured logs.
- Added more diagnostics about deploying cluster components in the `Ytsaurus` status.
- Added the ability to disable the launch of a full update (`enableFullUpdate` field in `Ytsaurus` spec).
- The `chyt` spec field was renamed to `strawberry`. For backward compatibility, it remains in `crd`, but it is recommended to rename it.
- The size of `description` in `crd` is now limited to 80 characters, greatly reducing the size of `crd`.
- `Query Tracker` status tables are now automatically migrated when it is updated.
- Added the ability to set privileged mode for `exec node` containers.
- Added `TCP proxy`.
- Added more spec validation: checks that the paths in the locations belong to one of the volumes, and also checks that for each specified component there are all the components necessary for its successful work.
- `strawberry controller` and `ui` can also be updated.
- Added the ability to deploy `http-proxy` with TLS.
- Odin service address for the UI can be specified in spec.
- Added the ability to configure `tags` and `rack` for nodes.
- Supported OAuth service configuration in the spec.
- Added the ability to pass additional environment variables to the UI, as well as set the theme and environment (`testing`, `production`, etc.) for the UI.
- Data node location media are created automatically during the initial deployment of the cluster.



{% endcut %}


{% cut "**0.3.1**" %}

**Release date:** 2023-08-14


**Features**

- Added the ability to configure automatic log rotation.
- `toleration` and `nodeSelector` can be specified in instance specs of components.
- Types of generated objects are specified in controller configuration, so operator respond to modifications of generated objects by reconciling.
- Config maps store data in text form instead of binary, so that you can view the contents of configs through `kubectl describe configmap <configmap-name>`.
- Added calculation and setting of `disk_usage_watermark` and `disk_quota` for exec node.
- Added a SPYT controller and the ability to load the necessary for SPYT into Cypress using a separate resource, which allows you to have several versions of SPYT on one cluster.

**Bug fixes**

- Fixed an error in the naming of the `medium_name` field in static configs.



{% endcut %}

