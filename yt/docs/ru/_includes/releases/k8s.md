## Kubernetes operator


Is released as helm charts on [Github Packages](https://github.com/ytsaurus/ytsaurus-k8s-operator/pkgs/container/ytop-chart).




**Releases:**

{% cut "**v0.31.0**" %}

**Release date:** 2026-03-12


#### What's Changed
* API: split api into separate module by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/675
* Implemented ImageHeater component by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/693
* Implemented imageHeater as step 0 during cluster creation by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/698
* helm: cleanup env in values and set GOMAXPROCS=1 by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/706
* Add client config into master containers by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/715
* Implemented rollingUpdate logic for hp/rp-proxies by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/708
* Prepared Data-nodes for rack-by-rack rollingUpdate by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/720
* Prepapred execNodes for rollingUpdate by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/729
* Add option to limits count of instance groups updated concurrently by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/722
* feat(ui): add urls field to UISpec for custom cluster icons by @masterbpro in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/726
* Add cluster states "Preparing" and "UpdateBlocked" by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/727
* Set minimum required Kubernetes to 1.29+, Helm to 3.18+ by @Copilot in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/732

#### Fixes
* spyt: fix ca root bundle env in init job by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/686
* Refactor and fix component update by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/690
* Fixed bug in ImageHeater with second run by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/696
* Spyt crd add flag offline by @Intention-man in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/687
* Cleanup and document cluster update options by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/699
* Update SPYT sample manifest with current image and new fields by @Intention-man in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/701
* delete flag from sample by @Intention-man in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/702
* Remove stale files by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/695
* Add pprof bind address by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/694
* Refactor ysaurus update state machine by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/707
* Add NodePort service type to rpc proxies in local cluster sample by @DanilSabirov in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/719
* Use golang builder from mirror.gcr.io/library/golang by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/712
* Setup hydra uploader and timbertruck users only when needed by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/717
* Cleanup logic of which component need update by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/710
* Fix Ytsaurus update stuck waiting for the `WaitingForTabletCellsRemoved` condition by @aapurii in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/714
* Rework component status constructors by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/721
* Add EnableAnchorProfiling option by @Gufran in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/650
* Fix ComponentStatusBlocked calls by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/723
* Created dispatchComponentUpdate helper for rolling update options by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/724
* Cleanup image heater by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/703
* Add into sample configs links to related documentation by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/731
* Fix waiting image heater at initialization by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/734
* Fix EnableAnchorProfiling by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/735
* Fix default node selector and tolerations in image heater by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/736

#### Testing
* test/e2e: sync delete namespace and logs events by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/683
* test/r8r: censor hashes by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/691
* test/r8r: fix hash censore for object lists by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/697
* General cleanup and r8r test for remote nodes, chyt, spyt by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/692
* Convert update_flow_steps_test.go to Ginkgo/Gomega by @Copilot in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/700
* Refactor YTsaurus test images by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/635
* Do not recover panic in tests by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/709
* test/e2e: enable mTLS for 25.2 by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/711
* Disable ginkgo parallelism for DEBUG=1 by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/716
* Fix kube-rbac-proxy in compat test by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/725

#### New Contributors
* @Copilot made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/700
* @DanilSabirov made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/719
* @aapurii made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/714
* @masterbpro made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/726

**Full Changelog**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/v0.30.0...v0.31.0

{% endcut %}


{% cut "**v0.30.0**" %}

**Release date:** 2026-02-02


#### What's Changed
* Remove kube-rbac-proxy by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/637
* Only create default pool if not present by @Gufran in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/646
* Set @cluster_name in init job by @Gufran in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/645
* Set data and tablet node resources in config by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/619
* Add permission allow everyone use for ch_public by @Gufran in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/647
* Add option items for FileObjectReference by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/654
* Extract json-schema from CRD by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/657
* config/samples: add yaml-language-server annotations by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/659
* Ability to specify any available version of spark in SPYT CRD for k8s by @Intention-man in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/658
* Update CA root bundle setup for Java by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/667
* Add feature flag to secure cluster connections by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/621
* Fix pod annotations by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/673
* Auto generating dyn config for query tracker with spark and spyt versions by @Intention-man in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/665
* Add feature to lock YT spec to operator version by @Gufran in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/653
* Refactor and fix common pod options by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/671
* Update list of YQL agent instances by cypress patch by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/680
* Switch release tag to standard golang scheme by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/681

#### Update strategies (work in progress)
* Adding masters to the new rolling update mode by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/630
* Validate update plan consistency by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/603
* Added ytop_strategy_on_delete_waiting_time_seconds prometheus metric by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/641
* Fixed bug in scheduler's UpdatePreCheck by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/660
* Improved arePodsUpdatedToNewRevision for onDelete logic by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/661
* Added config checksum and pod-annotation, updated onDelete e2e by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/652
* Added ca, ds and msc into new update strategy by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/662
* Changed return value for some component with ComponentStatusReadyAfter by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/663
* Added http and rpc proxies into new update strategy by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/666
* Added tnd,end and dnd into new bulkMode update strategy by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/669
* Save generations and hashes for detecting required updates by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/580

#### Testing
* test/e2e: capture and pass test spec context into reporters by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/639
* test/e2e: fix getOperatorMetricsURL by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/643
* test/e2e: fix checkClusterHealth by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/644
* test/e2e: define node resources by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/599
* Adjust golangci rules by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/649
* test: default timeout 1h by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/668
* Cleanup config map management in BuildCypressPatch by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/676
* test/e2e: use maintenance flag instead of unholy master clones by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/677
* Switch devel version to 0.0.0-devel by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/678

#### New Contributors
* @Intention-man made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/658

**Full Changelog**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.29.0...v0.30.0

{% endcut %}


{% cut "**0.29.0**" %}

**Release date:** 2025-12-19


#### New Features
* Add tablet balancer to operator by @ifsmirnov in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/634
* Add CA Root Bundle by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/617
* Support for HTTPS-only API by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/596
* Handle errors in config generation and overrides by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/633

#### Minor / Fixes
* Rename binary name of OffshoreDataGateway by @pavel-bash in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/622
* components/yql_agent: fix CA Root Bundle environment by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/623
* Add forgotten prometheus_cluster_role.yaml for prometheus integration by @Kontakter in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/626
* test/e2e: move chyt object into yt builder by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/627
* Add workaround for bug in CRI-O seccomp profile in privileged pods by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/631
* test/e2e: relabel and regroup testcases by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/636

#### Update strategies (work in progress)
* Added bulkUpdate mode of the updatePlan. Created preChecks interface,… by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/604
* Added onDelete rollingUpdate mode for Scheduler by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/625
* test/e2e: use better method to wait sts switch to onDelete by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/632

**Full Changelog**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.28.0...release/0.29.0

{% endcut %}


{% cut "**0.28.0**" %}

**Release date:** 2025-12-09


#### What's Changed
* Use reduced version of core/v1 Volume spec by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/571
* Set user job memory limit in exec node config by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/584
* Zero out job container resource requests without dedicated job resources by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/616

#### New Features
* Add OffshoreDataGateway component by @pavel-bash in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/567
* Adding customization of the solomon_exporter key by @kmalov in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/576
* Track operator version by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/579
* Make CRDs install optional by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/589
* Add resource selection by operator instance label by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/590
* Add NVIDIA container runtime for CRI-O by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/586

#### Minor and test fixes
* Revert "test: skip case broken for macos" by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/532
* Add YTOP_ENABLE_E2E for enabling e2e in test runs by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/565
* test/e2e: check TLS CHYT/YQL/QueryTracker for various versions by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/563
* Fix: Sidecar crashes during initialization lead to pods not being able to initialize [other implementation] by @ilyaibraev in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/570
* controllers: cleanup component manager by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/545
* Update generated files for OffshoreDataGateway by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/583
* Update README.md by @AMRivkin in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/585
* Update images to latest versions in cluster_v1_local.yaml by @savnadya in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/592
* Update container images in cluster_v1_demo.yaml by @savnadya in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/591
* [timbertruck] Initialization improvements by @ilyaibraev in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/594
* test/e2e: update ytsaurus images by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/595
* Validating `structuredLoggers` settings when `timbertruck` is enabled by @ilyaibraev in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/597
* Cleaup and fix operator compat test by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/593
* Added handleUpdatingClusterState helper for qa,qt,yqla components by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/601
* Fix CRI-O sidecar configmap mount by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/600
* test/e2e: fix http api tests by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/605
* Fix make install by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/602
* test/e2e: increase yt client log level to info by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/608
* test/e2e: incrase http api request timeouts by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/611
* Split fillJobEnvironmentCRI out of fillJobEnvironment by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/614
* test/r8r: add mtls https cri by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/612
* test/webhooks: cleanup by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/613
* Cleanup CA bundle components by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/615
* test/e2e: fix usage of spec-wide context by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/618
* Revert "Added handleUpdatingClusterState helper for qa,qt,yqla compon… by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/609
* test/e2e: check query tracker update by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/620

#### New Contributors
* @pavel-bash made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/567

**Full Changelog**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.27.0...release/0.28.0

{% endcut %}


{% cut "**0.27.0**" %}

**Release date:** 2025-09-22


#### Major changes
* Add support for CRI-O by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/525
* Introduce cypress patches by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/531
* Nvidia GPU support: nvidia-container-runtime & entrypoint for jobs container with GpuAgent by @futujaos in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/547
* Add discovery servers into master config by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/549
* Fix master update pipeline by @savnadya in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/552
* Support CHYT's server in http-proxies by @epsilond1 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/553
* strawberry: add option for logging into stderr by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/555
* Add options for reconciler concurrency by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/560
* Add bundle controller to operator by @ifsmirnov in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/562

#### Minor changes
* Use golang 1.24 by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/538
* Fix go tool execution from other directories by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/539
* Upgrade tools by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/540
* update canonized files by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/541
* add missing canondata files by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/542
* Get or guess cluster domain only once at start by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/543
* Fix cypress patch updating logic by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/546
* Update sample configs by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/548
* Upgrade ginkgo to v2.25.3 by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/551
* test/e2e: check map operation by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/550
* test/e2e: check native transport certificates rotation by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/554
* Add timbertruck canon test by @ilyaibraev in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/557
* Update dependencies by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/558
* Drop legacy operation spec option by @Kontakter in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/559
* By default run upto 1 reconciles concurrently by @savnadya in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/561

#### New Contributors
* @epsilond1 made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/553
* @Kontakter made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/559
* @ifsmirnov made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/562

**Full Changelog**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.26.0...release/0.27.0

{% endcut %}


{% cut "**0.26.0**" %}

**Release date:** 2025-08-20


#### Features
* Introduced a new Timbertruck component to delivery logs to cypress by @ilyaibraev in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/509
* Added cypress proxies by @koloshmet in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/519

#### Minor
* Pass metadata env vars into server pod containers by @Gufran in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/520
* Include default environment in CRI sidecar by @Gufran in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/522
* Add CRI service metrics by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/523
* fix ENV name typo by @kruftik in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/528
* Upgrade to golang 1.23.12 by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/529
* Update cluster_v1_local.yaml by @ogorbacheva in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/463
* Allow native transport TLS without mTLS by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/537


#### New Contributors
* @Gufran made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/520
* @koloshmet made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/519
* @ogorbacheva made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/463

**Full Changelog**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.25.0...release/0.26.0

{% endcut %}


{% cut "**0.25.0**" %}

**Release date:** 2025-07-23


#### Features
* Support for [YTsaurus Server 25.1.0](https://github.com/ytsaurus/ytsaurus/releases/tag/docker%2Fytsaurus%2F25.1.0)
* Native RPC mTLS by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/493
* Add ability to override http proxy ports by @imakunin in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/505
* Support new exe node config format by @k-pogorelov in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/511
* Support HydraPersistenceUploader sidecar by @ilyaibraev in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/489

#### Minor
* Sync exec node if containerd config changes by @sanchosancho in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/480
* Fix atomic status update in update plan logic by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/496
* Fix non-idempotent CreateUser by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/515


#### New Contributors
* @sanchosancho made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/480
* @kirillgrachoff made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/492
* @ilyaibraev made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/489
* @k-pogorelov made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/511

**Full Changelog**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.24.0...release/0.25.0

{% endcut %}


{% cut "**0.24.0**" %}

**Release date:** 2025-05-20


#### Minor
* Rerun init queue agent script during QA update by @savnadya in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/479

**Full Changelog**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.23.1...release/0.24.0

{% endcut %}


{% cut "**0.23.1**" %}

**Release date:** 2025-04-04


#### Minor
* Revert "Disable stockpile by default" in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/477

The change introduced in 0.23.0 was reverted due to the fact it ends up in updating all components of all existing clusters on the operator update, but change is not important. We'll consider making it opt-in in the next releases.


{% endcut %}


{% cut "**0.23.0**" %}

**Release date:** 2025-04-02


#### Minor
* Fix bug in blocked components column by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/464
* Deleted the `stderr` logger for JobProxy. by @imakunin in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/465
* Set init quota for user-defined mediums by @futujaos in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/466
* Disable stockpile by default by @imakunin in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/467
* tablet node tag filter for bundles in bootstrap by @futujaos in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/468
* Create ROADMAP.md by @AMRivkin in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/473
* Configure yqla mrjob syslibs V2 by @Krisha11 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/475

#### Release notes
* `configureMrJobSystemLibs` field was removed https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/475 and now system libs for YQL agent are being added unconditionally.
* stockpile/thread_count is set to zero in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/467 to remove non-relevant warnings in logs, as a downside it will trigger all components update.

#### New Contributors
* @futujaos made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/466
* @AMRivkin made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/473

**Full Changelog**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.22.0...release/0.23.0

{% endcut %}


{% cut "**0.22.0**" %}

**Release date:** 2025-03-07


#### Features
* Update to YTsaurus 24.2 is supported

#### Minor
* Add lost CA bundle and TLS secrets VolumeMounts for jobs container by @imakunin in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/449
* Add bus client configuration by @imakunin in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/450

#### Experimental
* Add multiple update selectors by @wilwell in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/383
* Add blocked components column to kubectl output by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/459

#### New Contributors
* @imakunin made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/449

**Full Changelog**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.21.0...release/0.22.0

{% endcut %}


{% cut "**0.21.0**" %}

**Release date:** 2025-02-10


#### Features
* Support the ability to deploy a Kafka proxy by @savnadya in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/407

#### Minor
* Add config for kind with audit log by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/441

#### Bugfix
* Preserve object finalizers by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/440
* Set quota and min_disk_space for locations by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/445
* Fix zero port if no monitoring port configured by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/447


{% endcut %}


{% cut "**0.20.0**" %}

**Release date:** 2025-01-20


#### Minor
* Support not creating non-existing users by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/416
* Added DNSConfig into Instance and YTsaurusSpec by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/420
* Enable real chunks job by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/412
* Add log_manager_template for job proxy by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/428

#### Release notes
This release makes yt operator compatible with ytsaurus 24.2. 
Update to this version will launch job for setting correct enable_real_chunks_value values in cypress and exec nodes will be updated with a new config.

**Full Changelog**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.19.0...release/0.20.0

{% endcut %}


{% cut "**0.19.0**" %}

**Release date:** 2025-01-09


#### Minor
* Configure yqla mrjob syslibs by @Krisha11 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/409
#### Bugfix
* Add yqla update job by @Krisha11 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/387

**Full Changelog**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.18.1...release/0.19.0

{% endcut %}


{% cut "**0.18.1**" %}

**Release date:** 2024-12-13


#### Minor
* more validation by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/393
#### Bugfix
* Fix updates for named cluster components @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/401

**Full Changelog**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.18.0...release/0.18.1

{% endcut %}


{% cut "**0.18.0**" %}

**Release date:** 2024-11-26


#### Warning
This release has known bug, which broke update for YTsaurus components with non-empty names (names can be set for data/tablet/exec nodes) and roles (can be set for proxies).
The bug was fixed in [0.18.1](https://github.com/ytsaurus/ytsaurus-k8s-operator/releases/tag/release%2F0.18.1).

#### Features
* Implemented RemoteTabletNodes api by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/372

#### Minor
* Update sample config for cluster with TLS by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/369
* Remove DataNodes from StatelesOnly update by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/371
* Added namespacedScope value to the helm chart by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/376
* Upgrade crd-ref-docs by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/379
* Add observed generation for remote nodes by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/382
* Support different controller families in strawberry configuration by @dmi-feo in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/355
* kata-compat: mount TLS-related files to a separate directory by @kruftik in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/388
* Support OAuth login transformations by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/397
* Add diff for static config update case by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/398

#### Bugfix
* Fix observed generation by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/373
* Fix YQL agent dynamic config creation by @savnadya in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/377
* Fix logging in chyt_controller by @dmi-feo in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/370
* Fix strawberry container name by @dmi-feo in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/375
* Use expected instance count as default for minimal ready count by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/395

#### New Contributors
* @dmi-feo made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/370

**Full Changelog**: https://github.com/ytsaurus/ytsaurus-k8s-operator/compare/release/0.17.0...release/0.18.0

{% endcut %}


{% cut "**0.17.0**" %}

**Release date:** 2024-10-11


#### Minor
* Separate CHYT init options into makeDefault and createPublicClique by @achulkov2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/347
#### Bugfix
* Fix queue agent init script usage for 24.* by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/356


{% endcut %}


{% cut "**0.16.2**" %}

**Release date:** 2024-09-13


#### Bugfix
* Fix strawberry controller image for 2nd job by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/345


{% endcut %}


{% cut "**0.16.1**" %}

**Release date:** 2024-09-13


#### Warning
This release has a bug if Strawberry components is enabled.
Use 0.16.2 instead.

#### Bugfix
* Revert job image override for UI/strawberry by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/344 — the bug was introduced in 0.16.0


{% endcut %}


{% cut "**0.16.0**" %}

**Release date:** 2024-09-12


#### Warning
This release has a bug for a configuration where UI or Strawberry components are enabled and some of their images were overridden (k8s init jobs will fail for such components).
Use 0.16.2 instead.

#### Minor
* Add observedGeneration field to the YtsaurusStatus by @wilwell in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/333
* Set statistics for job low cpu usage alerts by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/335
* Add nodeSelector for UI and Strawberry by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/338
* Init job creates from InstanceSpec image if specified by @wilwell in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/336
* Add tolerations and nodeselectors to jobs by @l0kix2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/342

#### New Contributors
* @wilwell made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/333


{% endcut %}


{% cut "**0.15.0**" %}

**Release date:** 2024-09-04


#### Backward incompatible changes
1. Component pod labels were refactored in #326 and changes are:
- `app.kubernetes.io/instance` was removed
- `app.kubernetes.io/name` was Ytsaurus before, now it contains component type
- `app.kubernetes.io/managed-by` is `"ytsaurus-k8s-operator"` instead of `"Ytsaurus-k8s-operator"`

2. Deprecated `chyt` field in the main [YTsaurus spec](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/docs/api.md#ytsaurusspec) was removed, use `strawberry` field with the same schema instead.

#### Minor
* Added tolerations for Strawberry by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/328
* Refactor label names for components by @achulkov2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/326

#### Experimental
* RemoteDataNodes by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/330


{% endcut %}


{% cut "**0.14.0**" %}

**Release date:** 2024-08-22


#### Backward incompatible changes
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

#### Minor
* Add no more than one ytsaurus spec per namespace validation by @qurname2 in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/305
* Add strategy, nodeSelector, affinity, tolerations by @sgburtsev in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/321
* Add forceTcp and keepSocket options by @leo-astorsky in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/324

#### Bugfixes
* Fix empty volumes array in sample config by @koct9i in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/318

#### New Contributors
* @leo-astorsky made their first contribution in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/324


{% endcut %}


{% cut "**0.13.1**" %}

**Release date:** 2024-07-30


#### Bugfixes
* Revert deprecation of useInsecureCookies in #310 by @sgburtsev in https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/317

The field `useInsecureCookies` was deprecated in the previous release in a not backwards compatible way, this release fixes it. It is now possible to configure the secureness of UI cookies (via the `useInsecureCookies` field) and the secureness of UI and HTTP proxy interaction (via the `secure` field) independently.


{% endcut %}


{% cut "**0.13.0**" %}

**Release date:** 2024-07-23


#### Features
* Add per-component terminationGracePeriodSeconds by @koct9i in https://github.com/ytsaurus/yt-k8s-operator/pull/304
* Added externalProxy parameter for UI by @sgburtsev in https://github.com/ytsaurus/yt-k8s-operator/pull/308
* Size as Quantity in LogRotationPolicy by @sgburtsev in https://github.com/ytsaurus/yt-k8s-operator/pull/309
* Use `secure` instead of `useInsecureCookies`, pass caBundle to UI by @sgburtsev in https://github.com/ytsaurus/yt-k8s-operator/pull/310

#### Minor 
* Add all YTsaurus CRD into category "ytsaurus-all" "yt-all" by @koct9i in https://github.com/ytsaurus/yt-k8s-operator/pull/311

#### Bugfixes
* Operator should detect configOverrides updates by @l0kix2 in https://github.com/ytsaurus/yt-k8s-operator/pull/314



{% endcut %}


{% cut "**0.12.0**" %}

**Release date:** 2024-06-28


#### Features
* More options for store locations. by @sgburtsev in https://github.com/ytsaurus/yt-k8s-operator/pull/294
  * data nodes upper limit for `low_watermark` increased from 5 to 25Gib;
  * data nodes' `trash_cleanup_watermark` will be set equal to the `lowWatermark` value from spec
  * `max_trash_ttl`can be configured in spec
* Add support for directDownload to UI Spec by @kozubaeff in https://github.com/ytsaurus/yt-k8s-operator/pull/257
  * `directDownload` for UI can be configured in the spec now. If omitted or set to `true`, UI will have current default behaviour (use proxies for download), if set to `false` — UI backend will be used for downloading.

#### New Contributors
* @sgburtsev made their first contribution in https://github.com/ytsaurus/yt-k8s-operator/pull/294


{% endcut %}


{% cut "**0.11.0**" %}

**Release date:** 2024-06-27


#### Features
* SetHostnameAsFQDN option is added to all components. Default is true by @qurname2 in https://github.com/ytsaurus/yt-k8s-operator/pull/302
* Add per-component option hostNetwork by @koct9i in https://github.com/ytsaurus/yt-k8s-operator/pull/287

#### Minor 
* Add option for per location disk space quota by @koct9i in https://github.com/ytsaurus/yt-k8s-operator/pull/279
* Add into exec node pods environment variables for CRI tools by @koct9i in https://github.com/ytsaurus/yt-k8s-operator/pull/283
* Add per-instance-group podLabels and podAnnotations by @koct9i in https://github.com/ytsaurus/yt-k8s-operator/pull/289
* Sort status conditions for better readability by @koct9i in https://github.com/ytsaurus/yt-k8s-operator/pull/290
* Add init containers for exec node by @koct9i in https://github.com/ytsaurus/yt-k8s-operator/pull/288
* Add loglevel "warning" by @koct9i in https://github.com/ytsaurus/yt-k8s-operator/pull/292
* Remove mutating/defaulter webhooks by @koct9i in https://github.com/ytsaurus/yt-k8s-operator/pull/296

#### Bugfixes
* fix exec node resource calculation on non-isolated CRI-powered job environment by @kruftik in https://github.com/ytsaurus/yt-k8s-operator/pull/277



{% endcut %}


{% cut "**0.10.0**" %}

**Release date:** 2024-06-07


#### Features
#### Minor 
 - Add everyone-share QT ACO by @Krisha11 in #272
 - Add channel in qt config by @Krisha11 in #273 
 - Add option for per location disk space quota #279
#### Bugfixes
- Fix exec node resource calculation on non-isolated CRI-powered job environment #277



{% endcut %}


{% cut "**0.9.1**" %}

**Release date:** 2024-05-30


#### Features
#### Minor 
 - Add 'physical_host' to cypress_annotations for CMS and UI сompatibility #252
 - added WATCH_NAMESPACE env and LeaderElectionNamespace #168
 - Add configuration for solomon exporter: specify host and some instance tags #258
 - Add sidecars support to primary masters containers #259 
 - Add option for containerd registry config path #264
#### Bugfixes
 - Fix CRI job environment for remote exec nodes #261


{% endcut %}


{% cut "**0.9.0**" %}

**Release date:** 2024-04-23


#### Features
- Add experimental (behaviour may change) UpdateSelector field #211 to be able to update components separately
#### Minor 
- Enable TmpFS when possible #235
- Disable disk quota for slot locations #236
- Forward docker image environment variables to user job #248
#### Bugfixes
- Fix flag doNotSetUserId #243


{% endcut %}


{% cut "**0.8.0**" %}

**Release date:** 2024-04-12


#### Features
#### Minor 
- Increased default value for MaxSnapshotCountToKeep and MaxChangelogCountToKeep
- Tune default bundle replication factor #210
- Set EnableServiceLinks=false for all pods #218
#### Bugfixes
- Fix authentication configuration for RPC Proxy #207 
- Job script updated on restart #224
- Use secure random and base64 for tokens #202
- Fix running jobs with custom docker_image when default job image is not set #217

{% endcut %}


{% cut "**0.7.0**" %}

**Release date:** 2024-04-04


#### Features
  * Add Remote exec nodes support #75
  * Add MasterCaches support #122
  * Enable TLS certificate auto-update for http proxies #167
  * CRI containerd job environment #105

#### Minor 
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

#### Bugfixes
  * Replace YQL Agent default monitoring port 10029 -> 10019


{% endcut %}


{% cut "**0.6.0**" %}

**Release date:** 2024-02-26


#### Features
- Added support for updating masters of 23.2 versions
- Added the ability to bind masters to the set of nodes by node hostnames.
- Added the ability to configure the number of stored snapshots and changelogs in master spec
- Added the ability for users to create access control objects
- Added support for volume mount with mountPropagation = Bidirectional mode in execNodes
- Added access control object namespace "queries" and object "nobody". They are necessary for query_tracker versions 0.0.5 and higher.
- Added support for the new Cliques CHYT UI.
- Added the creation of a group for admins (admins).
- Added readiness probes to component statefulset specs

#### Fixes
- Improved ACLs on master schemas
- Master and scheduler init jobs do not overwrite existing dynamic configs anymore.

#### Tests
- Added flow to run tests on Github resources
- Added e2e to check that updating from 23.1 to 23.2 works
- Added config generator tests for all components
- Added respect KIND_CLUSTER_NAME env variable in e2e tests
- Supported local k8s port forwarding in e2e

#### Backward Incompatible Changes
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
- Data node location mediums are created automatically during the initial deployment of the cluster.



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

