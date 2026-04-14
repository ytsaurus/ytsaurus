## UI


Is released as a docker image.




**Releases:**

{% cut "**3.7.0**" %}

**Release date:** 2026-03-19


#### [3.7.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v3.6.1...ui-v3.7.0) (2026-03-19)


#### Features

* **Queries:** multiple full result [YTFRONT-5615] ([d1adf53](https://github.com/ytsaurus/ytsaurus-ui/commit/d1adf532b6d2bb7ab67f852c6a308fb4161d73c4))


#### Bug Fixes

* **ACL/EditInheritance:** disable 'Confirm' button until a field is changed [YTFRONT-5562] ([e9a3c20](https://github.com/ytsaurus/ytsaurus-ui/commit/e9a3c208caedb27f05fc09a6c25edaf3dc82996d))
* **Components/Proxies:** add filtering of 'Role' choices [YTFRONT-] ([54c2f78](https://github.com/ytsaurus/ytsaurus-ui/commit/54c2f783107d82b8bbc60a8f5bab73cf0cec1c92))
* **Flows:** use 'name' field for computations [YTFRONT-5604] ([a5c2bc3](https://github.com/ytsaurus/ytsaurus-ui/commit/a5c2bc390f22b55ebea1d3324498fdb4fe07c25e))
* **Navigation/ChaosReplicatedTable:** use 'alter_replication_card' command to switch automatic replication [YTFRONT-5287] ([b911633](https://github.com/ytsaurus/ytsaurus-ui/commit/b91163362a0b8d28c0b5951bcf0b5a443f733b0b))
* **Monitoring:** fix dashboard autoupdate [YTFRONT-5614] ([5414ee4](https://github.com/ytsaurus/ytsaurus-ui/commit/5414ee418dee9d456a25d1a86cab93b69dde1d98))
* **Navigation:** metadata in empty tables [YTFRONT-5638] ([4c15602](https://github.com/ytsaurus/ytsaurus-ui/commit/4c15602d12b4442f9b7e89a38666d8e3798ade5c))
* **Navigation/TopRow:** a path with '\/' should not be marked as a symlink [YTFRONT-5182] ([b302c3b](https://github.com/ytsaurus/ytsaurus-ui/commit/b302c3b046c3c9e60c8210147c644c26f9bb00f3))
* **Operation/Details:** layer paths with attributes should be properly displayed [YTFRONT-5268] ([2e76f34](https://github.com/ytsaurus/ytsaurus-ui/commit/2e76f345296ce4772282da363fc38f1c7e0de0b3))
* **Queries/Progress:** fix operation url [YTFRONT-5622] ([73e6b3d](https://github.com/ytsaurus/ytsaurus-ui/commit/73e6b3d5c37d830b86da26c5c2e9e92ce53e259f))
* **Scheduling:** use utf8.decode for title of operations [YTFRONT-5597] ([dbb222c](https://github.com/ytsaurus/ytsaurus-ui/commit/dbb222cbdeba9a96f28470e19b7ab9adf84f34df))
* **System:** 'nvme' word should be in UPPERCASE [YTFRONT-5534] ([dac156c](https://github.com/ytsaurus/ytsaurus-ui/commit/dac156c67387192986dbc6d76785ae034783db4d))
* **System:** copy address to clipboard [YTFRONT-5129] ([0182d77](https://github.com/ytsaurus/ytsaurus-ui/commit/0182d7774600e32a59e1c3ac8d55ab8d5142056c))


{% endcut %}


{% cut "**3.6.0**" %}

**Release date:** 2026-02-18


#### [3.6.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v3.5.1...ui-v3.6.0) (2026-02-18)


#### Features

* **Accounts/DetailedUsage:** requests should  be sent through nodejs BFF [YTFRONT-5366] ([b240437](https://github.com/ytsaurus/ytsaurus-ui/commit/b24043752ea0c2abd983de23574c11e72805449c))
* **Flow:** add flows as a separate page [YTFRONT-5241] ([74ae966](https://github.com/ytsaurus/ytsaurus-ui/commit/74ae96625e06a16283934898792ab49ae17533ff))
* **Flow:** show messages for all tabs [YTFRONT-5244] ([3552691](https://github.com/ytsaurus/ytsaurus-ui/commit/35526919d1834dbd04216e4f06ce7a663fb4dfbd))
* **Navigation/AccessLog:** requests should be sent through nodejs BFF [YTFRONT-5300] ([15e6e39](https://github.com/ytsaurus/ytsaurus-ui/commit/15e6e3924f551d6e4eabf6cff4a9e7db7d1ab3fc))
* **Queries/Progress:** new graph design [YTFRONT-5468] ([1daca1f](https://github.com/ytsaurus/ytsaurus-ui/commit/1daca1fed95a6ae0445ba8bf88a46a4f3251a661))


#### Bug Fixes

* **Flow/Computation:** use highlight_cpu_usage, highlight_memory_usage [YTFRONT-5115] ([129c231](https://github.com/ytsaurus/ytsaurus-ui/commit/129c231daa03faf638384ef4bc180ada90d72b5d))
* **Flow/Messages:** add 'markdown_text' support [YTFRONT-5255] ([0f0a96f](https://github.com/ytsaurus/ytsaurus-ui/commit/0f0a96fc2de1045e35f03668051789b880ac58e1))
* **Flow/Messages:** expand first message when messages.length == 1 [YTFRONT-5237] ([36de213](https://github.com/ytsaurus/ytsaurus-ui/commit/36de2130a929a794f0cb3feaf437fb6b98cdba3f))
* **Flow/Monitoring:** forward pipeline_path [YTFRONT-4488] ([f84673a](https://github.com/ytsaurus/ytsaurus-ui/commit/f84673a30d596c1fa8723e854c7a3a314631c046))
* **FlowMessages:** add 'markdown_text' support [YTFRONT-5255] ([757df50](https://github.com/ytsaurus/ytsaurus-ui/commit/757df50e8886df26ee1c83cd56f38074c7859297))
* **Navigation/Flow:** better computation [YTFRONT-5115] ([3f413ec](https://github.com/ytsaurus/ytsaurus-ui/commit/3f413ec710c1f5545254c2ca679ac3d80b918dae))
* **Queries/Tutorials:** support old qt tutorials [YTFRONT-5509] ([25de4f7](https://github.com/ytsaurus/ytsaurus-ui/commit/25de4f733bbb10fec4380579895a66a6d80af18c))
* **Queries:** yql version in new query [YTFRONT-5522] ([ff5c03a](https://github.com/ytsaurus/ytsaurus-ui/commit/ff5c03aa4b8670f69e632dff5d11ac053acc2379))
* **Redirect:** corrected the useBeta value check [YTFRONT-5543] ([199b0d3](https://github.com/ytsaurus/ytsaurus-ui/commit/199b0d3c0550bca9f1c7bd9e4033bed2587c5113))

{% endcut %}


{% cut "**3.5.1**" %}

**Release date:** 2026-02-02



#### [3.5.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v3.5.0...ui-v3.5.1) (2026-02-02)


#### Bug Fixes

* better default docsBaseurl [YTSAURUSSUP-2262] ([d0bcc3e](https://github.com/ytsaurus/ytsaurus-ui/commit/d0bcc3e75ddb7ec9ac611854db6bf3c071b482b2))
* **Components/Node:** fix a mispring [YTFRONT-5511] ([cb3390f](https://github.com/ytsaurus/ytsaurus-ui/commit/cb3390fb61ecc6c571a3d0a843a298bc5bdd39e4))
* **Components/Versions:** do not use autofocus for 'Host' filter [YTSAURUSSUP-2262] ([306338c](https://github.com/ytsaurus/ytsaurus-ui/commit/306338c3f0cf536f19d8b8b018bbc8747625282a))
* **Dashboard:** enabled new dashbaord by default [YTSAURUSSUP-2262] ([b09cec1](https://github.com/ytsaurus/ytsaurus-ui/commit/b09cec1fe138caef82685d475952d5eee8b0704e))
* **DownloadManager:** crypto problem [YTFRONT-5474] ([418a4b2](https://github.com/ytsaurus/ytsaurus-ui/commit/418a4b27554c69cb24cbdee86b370ac300dafe08))
* **Navigation/MapNode:** add 'Settings' button with 'Group nodes by type' [YTSAURUSSUP-2262] ([0e5c599](https://github.com/ytsaurus/ytsaurus-ui/commit/0e5c599df27fc04e25cf8e930c4e8e913b9a7d7a))
* **oatuh:** fix maxAge of YT_OAUTH_REFRESH_TOEKEN_NAME [YTFRONT-5504] ([d289da8](https://github.com/ytsaurus/ytsaurus-ui/commit/d289da8396e5949389b2ece6d3ddb25ff1bc172e))
* **Operation/JobsTimeline:** increase jobs count [YTFRONT-5484] ([6553fc6](https://github.com/ytsaurus/ytsaurus-ui/commit/6553fc6acecf9abb0e43fdf23685b09dd1be5613))
* **Scheduling:** better sorting for 'Pool/Operation' [YTFRONT-5469] ([02d9800](https://github.com/ytsaurus/ytsaurus-ui/commit/02d9800fabddb50497f1dc4481c3d5be635fc11b))
* **Scheduling:** fit breadcrums to visible area [YTFRONT-5506] ([5f47b66](https://github.com/ytsaurus/ytsaurus-ui/commit/5f47b663bb8738ce6c860bf2c4240e0892c99a1b))
* **server:** do not use cacheable-lookup [YTSAURUSSUP-2262] ([03f0751](https://github.com/ytsaurus/ytsaurus-ui/commit/03f07512b7a97fa924f3223b951fb4ab24180dd4))
* **styles:** resolve SASS deprecation warnings ([448e6f2](https://github.com/ytsaurus/ytsaurus-ui/commit/448e6f2acb9d648b1f4b5c56b67efa539d045d77))


#### Features

* **Accounts:** add Prometheus dashboard inegration [YTFRONT-4388] ([a3c85e0](https://github.com/ytsaurus/ytsaurus-ui/commit/a3c85e024be4f4726f64377bf4e0c6ef65d2856d))
* **Bundles:** add Prometheus dashboard integration [YTFRONT-4388] ([2ce6c39](https://github.com/ytsaurus/ytsaurus-ui/commit/2ce6c39409b46d265fc2c3eb1fcedce85734dfa9))
* **CHYT:** add Prometheus dashboard integration [YTFRONT-4388] ([2019484](https://github.com/ytsaurus/ytsaurus-ui/commit/201948411996d07db5155c0f2ad30f7c5ac8446d))
* **Job:** add Prometheus dashboard integration [YTFRONT-4388] ([d905243](https://github.com/ytsaurus/ytsaurus-ui/commit/d905243d4446fa7b79dd1f2cfb60f8e2fec80790))
* **Monitoring:** add Prometheus dashboards integration [YTFRONT-4388] ([7a1ce2a](https://github.com/ytsaurus/ytsaurus-ui/commit/7a1ce2a6285ee33ec32fe6c74f689f1a3f8b33d4))
* **Navigation/Consumer:** add Prometheus dashboard integration [YTFRONT-4388] ([873c644](https://github.com/ytsaurus/ytsaurus-ui/commit/873c644daa6d7f732ae92bef52d5ca4637e09951))
* **Navigation/Flow:** add Prometheus integration [YTFOTN-4388] ([eace5ae](https://github.com/ytsaurus/ytsaurus-ui/commit/eace5aecc96a9210036cf60b2bd20bfe6e5f10f3))
* **Navigation/Queue:** add Prometheus dashboard integration [YTFRONT-4388] ([c10369d](https://github.com/ytsaurus/ytsaurus-ui/commit/c10369dd2a2a8d4ff26d7ce77661e3cd8cdcabb7))
* **Navigation/Metadata:** operation link [YTFRONT-4994] ([33a1d26](https://github.com/ytsaurus/ytsaurus-ui/commit/33a1d26d095221c1fa47964a3ce8866fa831e1a6))
* **Navigation:** duplicate names [YTFRONT-5458] ([7bec172](https://github.com/ytsaurus/ytsaurus-ui/commit/7bec172450a2789a52602f4c8681351852c97b24))
* **Navigation:** long text in error [YTFRONT-5477] ([3341ef7](https://github.com/ytsaurus/ytsaurus-ui/commit/3341ef77390bffef156802e9be5f0469dcb53477))
* **Navigation:** support Ctrl in breadcrumbs [YTFRONT-5465] ([8600951](https://github.com/ytsaurus/ytsaurus-ui/commit/8600951431332c8028bc3d642fc3b699375c264d))
* **Operation:** add Prometheus dashboard integration [YTFRONT-4388] ([c615ccb](https://github.com/ytsaurus/ytsaurus-ui/commit/c615ccb37248c84c260bea5a99e8de7b647bed4f))
* **Operations:** limit format [YTFRONT-5386] ([7e3940a](https://github.com/ytsaurus/ytsaurus-ui/commit/7e3940a5a7dd8d07537acf431e77f8c0ffe7b084))
* **Queries/Progress:** old graph height [YTFRONT-5500] ([3283db8](https://github.com/ytsaurus/ytsaurus-ui/commit/3283db81b52693df0f03439def60011931647c30))
* **Queries:** display only supported engines [YTFRONT-5502] ([a2d9e16](https://github.com/ytsaurus/ytsaurus-ui/commit/a2d9e16339131b3a8a933c3d055d311c1adbd560))
* **Plan:** migrate to gravity timeline [YTFRONT-5215] ([57cb5f4](https://github.com/ytsaurus/ytsaurus-ui/commit/57cb5f4dfe5301c96d854b2a3ad316d487d9e239))
* **Scheduling:** wrong pending time [YTFRONT-5439] ([8959c61](https://github.com/ytsaurus/ytsaurus-ui/commit/8959c611822821ba49d7bcd855d90c5475ae0e78))
* **System:** add Prometheus dashboard integration [YTFRONT-4388] ([66da56d](https://github.com/ytsaurus/ytsaurus-ui/commit/66da56d9d0d0ef864f9493eaa271386180f26c2e))
* **Users/EditModal:** another form if crypto not supported [YTFRONT-5474] ([6c9eee8](https://github.com/ytsaurus/ytsaurus-ui/commit/6c9eee83a677ffe9b26fb5fb08e39068070ec984))
* **UTF8:** graph node details info [YTFRONT-5246] ([d83815d](https://github.com/ytsaurus/ytsaurus-ui/commit/d83815d16397988fd5c4dace4c26416e3bc3ff00))


{% endcut %}


{% cut "**3.4.1**" %}

**Release date:** 2025-12-23


#### [3.4.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v3.4.0...ui-v3.4.1) (2025-12-23)


#### Features

* **AiChat:** add agents by engine [YTFRONT-5381] ([32b26ff](https://github.com/ytsaurus/ytsaurus-ui/commit/32b26ff1e3cce2860f889a1c2a67743d4aabaabc))
* **interface-helpers:** add format.NumberWithSuffix ([0080c7d](https://github.com/ytsaurus/ytsaurus-ui/commit/0080c7d5caa0a3345233e20eb0d8cdac10481a54))


#### Bug Fixes

* **Components:** long role name [YTFRONT-5390] ([aad281b](https://github.com/ytsaurus/ytsaurus-ui/commit/aad281b17634285ebf9e22afac448a68a0515ec9))
* **Navigation/Breadcrumbs:** breadcrumbs should be truncated by view-port [YTFRONT-5421] ([d005496](https://github.com/ytsaurus/ytsaurus-ui/commit/d005496874a9dbe0b7b598bdbde80bb04a030d64))
* **Scheduling/Table:** minor fix for scrolling [YTFRONT-5134] ([b4857f6](https://github.com/ytsaurus/ytsaurus-ui/commit/b4857f638c41b01edcf8fbce56901143ccf5f6c1))
* **Scheduling:** add redirect for removed 'Details' tab [YTFRONT-5134] ([84282c1](https://github.com/ytsaurus/ytsaurus-ui/commit/84282c1e2e63707909f88caba1bb79b47f7cdb0e))
* **Scheduling:** bring back 'Static configuration' block [YTFRONT-5423] ([5bfa023](https://github.com/ytsaurus/ytsaurus-ui/commit/5bfa02392181193f119418931a0a33255f3893c0))
* **Statistics:** better value formatting for YTChartKitPie [YTFRONT-5304] ([a7ca6cf](https://github.com/ytsaurus/ytsaurus-ui/commit/a7ca6cfc5d93956cdbf33ba393f4f894622e6dcf))

{% endcut %}


{% cut "**3.3.1**" %}

**Release date:** 2025-12-12


#### [3.3.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v3.3.0...ui-v3.3.1) (2025-12-12)

#### ⚠ BREAKING CHANGES

* upgrade @gravity-ui/uikit to version 7 [YTFRONT-4917]

#### Features

* add new cluster theme 'electricviolet' [YTFRONT-5318] ([4d30f9e](https://github.com/ytsaurus/ytsaurus-ui/commit/4d30f9e15dec0dd2ec1ca73c7d033cd4b4157ecc))
* **Queries:** ru locale [YTFRONT-5069] ([1aa195c](https://github.com/ytsaurus/ytsaurus-ui/commit/1aa195c9d832afa39e0c3e8cc9930197c7f680a5))
* **Scheduling:** rework the page [YTFRONT-5134] ([e289ac4](https://github.com/ytsaurus/ytsaurus-ui/commit/e289ac4e28a754c0f9695327fdfe0a487df5c80d))
* **Queries/Tutorials:** add tutorials pagination [YTFRONT-5344] ([29dcc79](https://github.com/ytsaurus/ytsaurus-ui/commit/29dcc7956e6fac3c6781b422b0d8d1d77e1c990c))
* **AiChat:** add ai chat [YTFRONT-5048] ([ca57375](https://github.com/ytsaurus/ytsaurus-ui/commit/ca57375da7103565df0f79ece2e9e22a057c8774))
* add disableHeavyProxies option to clusters-config [YTFRONT-5176] ([df70d88](https://github.com/ytsaurus/ytsaurus-ui/commit/df70d883cc053696523829ef509d7748e6688cb8))
* **Operation/Details/Specification:** add cpu_limit/gpu_limit/memory_limit for tasks [YTFRONT-5145] ([939fead](https://github.com/ytsaurus/ytsaurus-ui/commit/939fead0fdf534a8530f365e8220a1c1efb1e798))
* upgrade @gravity-ui/uikit to version 7 [YTFRONT-4917] ([29c4362](https://github.com/ytsaurus/ytsaurus-ui/commit/29c4362ed5ae9ce10d9b7a964784c804f6de3b0b))
* upgrade react-redux 7-&gt;9 ([76ca6fe](https://github.com/ytsaurus/ytsaurus-ui/commit/76ca6fefccd64186c1bfb01baa8503ea8d2d8638))

#### Bug Fixes

* **Settings:** hide the developer tab for users [YTFRONT-5393] ([8402b68](https://github.com/ytsaurus/ytsaurus-ui/commit/8402b68a2a7808f4302462e868c06082da55ee2e))
* **Maintenance:** top-row should be visible after click on 'Proceed to cluster anyway' [YTFRONT-5320] ([98d5ebe](https://github.com/ytsaurus/ytsaurus-ui/commit/98d5ebe5ce863c764a863a3fa9d8798c0f5f8d0e))
* **Navigation/Flow:** better z-index for dialogs [YTFRONT-5401] ([61add6c](https://github.com/ytsaurus/ytsaurus-ui/commit/61add6c107607b393601ea819e13c63605dca28d))
* **Queries:** take stage into alterQuery [YTFRONT-5394] ([cfee11f](https://github.com/ytsaurus/ytsaurus-ui/commit/cfee11f4d1e80a63c47fcbf0da1df74c190f4407))
* **UIFactory:** add 'hidden' fields for results of UIFactory.getSchedulingExtraTabs(...) [YTFRONT-5271] ([f9c9c67](https://github.com/ytsaurus/ytsaurus-ui/commit/f9c9c670803a509507b513a329a6c13bbd01f085))
* **JobLogs:** use cluster config [YTFRONT-5348] ([df75e2d](https://github.com/ytsaurus/ytsaurus-ui/commit/df75e2d35d619a31fb2151e0466f29f8dead1b15))
* **Queries/History:** date filter wrong format [YTFRONT-5344] ([72eb7a2](https://github.com/ytsaurus/ytsaurus-ui/commit/72eb7a271b9725e07a80410a6a91c4dfa11d9cc7))
* **Queries/History:** refresh list problem [YTFRONT-5344] ([9a0450b](https://github.com/ytsaurus/ytsaurus-ui/commit/9a0450b5ee559b5551120a3d33d5a7e46bbd7709))
* **Queries/Tutorials:** problem with URL when clicking on element [YTFRONT-5344] ([f485257](https://github.com/ytsaurus/ytsaurus-ui/commit/f4852576b96320d90d6a2bfe12f997dce92f4753))
* **Scheduling:** returned filtering when selecting a pool tree [YTFRONT-5363] ([67a7c4d](https://github.com/ytsaurus/ytsaurus-ui/commit/67a7c4d0165f2b3ebe6e3bb82258cbf34ab81f64))
* **Account/Editor:** do not allow to remove accounts with not empty resource usage [YTFRONT-5320] ([e2ca472](https://github.com/ytsaurus/ytsaurus-ui/commit/e2ca472c4c24acf20dad64c7ffe34a6b101ff9ec))
* **ACL:** renamd 'Edit ACL' to 'Add ACL' [YTFRONT-5314] ([14f7ba7](https://github.com/ytsaurus/ytsaurus-ui/commit/14f7ba7914bd6417d149d4e5924ba25cde743df5))
* **Navigation:** correct operation reference in metadata [YTFRONT-5193] ([a09f5ae](https://github.com/ytsaurus/ytsaurus-ui/commit/a09f5aea16e9d1921727ab6f43f201bd3dbf5c3b))
* **Navigation:** editing tablet cell bundle for replicated tables [YTFRONT-5350] ([2757be5](https://github.com/ytsaurus/ytsaurus-ui/commit/2757be5902145931d15464d55b46a6b6453014c0))
* **Operation/Jobs:** description filter [YTFRONT-5254] ([d14b6e1](https://github.com/ytsaurus/ytsaurus-ui/commit/d14b6e1490f93fe735b8891faa49ad5dfe3167ed))
* **Queries/Navigation:** loss of state when executing request [YTFRONT-5252] ([dfa972c](https://github.com/ytsaurus/ytsaurus-ui/commit/dfa972ca2d3107eaf0c39349a19ed7190590a1d6))
* **Queries/Navigation:** rerender when running query [YTFRONT-5252] ([d2585bf](https://github.com/ytsaurus/ytsaurus-ui/commit/d2585bfe3166271fb4d0482372887d072cb549f7))
* **Queries/Tutorials:** wrong items in list [YTFRONT-5302] ([dc4a980](https://github.com/ytsaurus/ytsaurus-ui/commit/dc4a980b41ba59eeef2f1b9eec0b36c118da5399))
* **Queries:** load yql version correctly [YTFRONT-5349] ([419e729](https://github.com/ytsaurus/ytsaurus-ui/commit/419e72948285311922276399f628ea26cd0faae4))
* **Queries:** make settings correct type [YTFRONT-5351] ([376c943](https://github.com/ytsaurus/ytsaurus-ui/commit/376c9437b8180a921b40d8690f4aaaffa06e4870))
* **Dashboard2/WidgetSettings:** key should rely on a data [YTFRONT-5154] ([d6a2fa9](https://github.com/ytsaurus/ytsaurus-ui/commit/d6a2fa991edcf11a4af761824d5d10ef80ba2ca3))
* **Dashboard2:** operation and queries widgets limits [YTFRONT-5154] ([662662d](https://github.com/ytsaurus/ytsaurus-ui/commit/662662d6f8552bb3e0ef5e0d45e252faab8e3911))
* **Dashboard2:** provide better errors per item in Accounts/Pools/Services widgets [YTFRONT-5154] ([d6d4b4b](https://github.com/ytsaurus/ytsaurus-ui/commit/d6d4b4bb6e949b78f69dffbe9645c92d063ba510))
* **Dashboards2:** caching while copying cluster settings from previous cluster[YTFRONT-5154] ([d9fb327](https://github.com/ytsaurus/ytsaurus-ui/commit/d9fb327b6341d0bd4c7d4e5fafc3cacfb03a648c))
* **Navigation/Link:** render error [YTFRONT-5293] ([f976a39](https://github.com/ytsaurus/ytsaurus-ui/commit/f976a39c3f3861bea10b6551e578cf06f1750bcb))
* **Queries/List:** state filter error [YTFRONT-5310] ([6a5c876](https://github.com/ytsaurus/ytsaurus-ui/commit/6a5c8765937e53a9edb32c3bae0cb7f297880ea1))

{% endcut %}


{% cut "**2.7.0**" %}

**Release date:** 2025-11-11


#### [2.7.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v2.6.0...ui-v2.7.0) (2025-11-11)


#### Features

* **Components/CypressProxies:** add cypress proxies tab [YTFRONT-5036] ([4e91046](https://github.com/ytsaurus/ytsaurus-ui/commit/4e91046d5ab84ade0511b7c4426c9060728098e8))
* **Operation/Logs:** support logs tabs for operations and logs [YTFRONT-5191] ([b088313](https://github.com/ytsaurus/ytsaurus-ui/commit/b0883136a4fc649b0c478d6b8dee0dc0b38e26c4))
* **System/CypressProxies:** add cypress proxies section [YTFRONT-5036] ([249dba8](https://github.com/ytsaurus/ytsaurus-ui/commit/249dba86ab4d92bba40dbe762c8215cea401df18))
* **Queries:** share query with active tab [YTFRONT-5001] ([9ea4fcb](https://github.com/ytsaurus/ytsaurus-ui/commit/9ea4fcba2c443463a26504825fba6cc6c2ea771e))
* **Dashboard2:** internalization for Dashboard page [YTFRONT-3400] ([fdef408](https://github.com/ytsaurus/ytsaurus-ui/commit/fdef4081b15178d9b5255730905c1bec5cc35f0c))
* **Operation/Timeline:** show incarnations [YTFRONT-4980] ([7f3209d](https://github.com/ytsaurus/ytsaurus-ui/commit/7f3209d98cadd8fd15fb6a16a2919c32a7c16f84))
* **Queries:** add yql versions [YTFRONT-5098] ([1762ef8](https://github.com/ytsaurus/ytsaurus-ui/commit/1762ef8234c7f84c602201f8ac62796a1e95ffb9))
* **Settings:** stage setting is available for users [YTFRONT-5261] ([3ad3eda](https://github.com/ytsaurus/ytsaurus-ui/commit/3ad3eda4c444c0ba67eae3e303966de3f337c903))
* **Operation/Jobs:** add monitoring descriptor filter [YTFRONT-5254] ([5b7bc64](https://github.com/ytsaurus/ytsaurus-ui/commit/5b7bc6456494bff6e0a5451123d8a17a07c80b43))
* **Operation/JobsMonitoring:** new jobs monitor [YTFRONT-5053] ([c9d5051](https://github.com/ytsaurus/ytsaurus-ui/commit/c9d5051d755c681a96c1ee1c2e7c8b52efe164ce))


#### Bug Fixes

* **Queries:** engines info problem [YTFRONT-5286] ([5253699](https://github.com/ytsaurus/ytsaurus-ui/commit/525369988fbd3c60e08bda2b1d62e4318f94e6bb))
* **Operations/JobsMonitoring:** full list of descriptors [YTFRONT-5192] ([8a3c244](https://github.com/ytsaurus/ytsaurus-ui/commit/8a3c24477092f8a8af1f2b74d8c33cdfb10a53f2))
* **UTF8:** truncated cell encoding [YTFRONT-5206] ([69f44f7](https://github.com/ytsaurus/ytsaurus-ui/commit/69f44f78f9c7d941d0ec13729ebfa03b88399903))
* **UTF8:** truncated modal [YTFRONT-5202] ([a64ede7](https://github.com/ytsaurus/ytsaurus-ui/commit/a64ede7cc6ba11635e6a4c523e594583d1f09f29))
* **Navigation/Description:** use '&' to suppress links resolution [YTFRONT-5232] ([7c72895](https://github.com/ytsaurus/ytsaurus-ui/commit/7c72895e0d947bfa68852bf77ef7d18c26b58616))
* **Navigation/NavigationError/RequestPermission:** allow requests for 'portal_exit' [YTFRONT-5233] ([0e54e1a](https://github.com/ytsaurus/ytsaurus-ui/commit/0e54e1ac69f24f7e96624d3248d972a8ae6ce5aa))
* **Navigation/Table:** use 'Allow raw string' when 'YQL V3 Types' [YTFRONT-5226] ([481aed0](https://github.com/ytsaurus/ytsaurus-ui/commit/481aed0c55a3cb8f018ebbb94df510fb10045691))
* **Operations/JobsMonitoring:** display of limit exceeding [YTFRONT-5231] ([6d6a597](https://github.com/ytsaurus/ytsaurus-ui/commit/6d6a5972ebb9568ef576a0343276e7412213a722))
* **Queries/Chart:** date in axis [YTFRONT-5105] ([52378f1](https://github.com/ytsaurus/ytsaurus-ui/commit/52378f1bc7f09c4591faefc92d1e00fb688d649d))
* **Queries:** tutorials tab [YTFRONT-5240] ([02ad5e2](https://github.com/ytsaurus/ytsaurus-ui/commit/02ad5e26ca88745b61bbb44b93f00fcab6ab1a95))
* **Queries:** wrong clique selector size [YTFRONT-5227] ([db610ec](https://github.com/ytsaurus/ytsaurus-ui/commit/db610ec8fcd8a22296a4cb864a11412e2465b7ac))
* **Queries:** graph when switching tabs [YTFRONT-5263] ([0d649e4](https://github.com/ytsaurus/ytsaurus-ui/commit/0d649e48d555cada46be9d4b62d7d1530dc584c1))
* **Chyt/ACL:** display specific set of permissions [YTFRONT-5275] ([1022307](https://github.com/ytsaurus/ytsaurus-ui/commit/1022307a0804eaa979c21c78067926f63b16e548))
* **ClustersMenu:** place GPU group after 'Auxiliary MRs' [YTFRONT-5282] ([d7f387a](https://github.com/ytsaurus/ytsaurus-ui/commit/d7f387ab3f0ca93c09138c13e44b7be85b6d1674))
* **Navigation/Attributes:** compression codec [YTFRONT-5200] ([1062551](https://github.com/ytsaurus/ytsaurus-ui/commit/1062551b150fcb2baf3923ac1c967f581ac34512))
* **Navigation/Description:** do not close edit mode in case of saving error [YTFRONT-5273] ([a3b06cf](https://github.com/ytsaurus/ytsaurus-ui/commit/a3b06cf13d438aad436babdcbf88a19986fb9677))
* **Navigation/Table:** apply 'Allow raw strings' only for string-types [YTFRONT-5226] ([4358b69](https://github.com/ytsaurus/ytsaurus-ui/commit/4358b69a04ea901e5569e0e84a549569ab52a0b4))
* **Operation/Incarnations:** expand by default first incarnation [YTFRONT-5278] ([e55f832](https://github.com/ytsaurus/ytsaurus-ui/commit/e55f8321b7c159aa6de7f9624f8a1b191e78d88c))
* **Queries:** queries list update problem [YTFRONT-5260] ([1a8708f](https://github.com/ytsaurus/ytsaurus-ui/commit/1a8708fe6bc6f5025cf998ceefead8d37f6d6791))
* **UTF8:** operation title encoding problem [YTFRONT-5269] ([0a7ebb2](https://github.com/ytsaurus/ytsaurus-ui/commit/0a7ebb2f43b39bba062986d4a16aff4cda4234bd))

{% endcut %}


{% cut "**2.1.0**" %}

**Release date:** 2025-09-18


#### [2.1.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v2.0.0...ui-v2.1.0) (2025-09-18)

#### ⚠ BREAKING CHANGES

* **interface-helpers:** move format.DateTime to ui/src/common/hammer/format [YTFRONT-5171]

#### Features

* **yql:** add syntax highlighting for new YQL keywords ([ab6ff5a](https://github.com/ytsaurus/ytsaurus-ui/commit/ab6ff5af05eb06512ce183ebd9cce5d214c03555))
* **Navigation/Flow:** add 'force'-flag for static spec editing [YTFRONT-5185] ([26a75b6](https://github.com/ytsaurus/ytsaurus-ui/commit/26a75b63fdab09fe416e7372629cb33a7acf7307))
* **OperationDetail/Incarnations:** add incarnations tab [YTFRONT-5119] ([7f6264c](https://github.com/ytsaurus/ytsaurus-ui/commit/7f6264ca9e4fde6df1f37305910c88d90313cc2a))
* **Queries:** add secrets button [YTFRONT-5162] ([8b6a834](https://github.com/ytsaurus/ytsaurus-ui/commit/8b6a8342a8e2e1b3c7137ad5fc987e587ed270d5))
* **Navigation/ReplicatedTable:** add column for content_type [YTFRONT-4964] ([c9b16f3](https://github.com/ytsaurus/ytsaurus-ui/commit/c9b16f3763f01b0b3a67bdf46b8e09ded849c08c))
* update @gravity-ui/chartkit v5.22.4 [YTFRONT-5116] ([e2b0be8](https://github.com/ytsaurus/ytsaurus-ui/commit/e2b0be8a721a14777784cdb6df0be86af6b715b5))
* **Bundles:** remove bundle balancer [YTFRONT-5160] ([7b5f141](https://github.com/ytsaurus/ytsaurus-ui/commit/7b5f141e9b7f3498049e2af55f8b3682c7560c77))
* **Navigation/Table:** inline preview for `audio`, `image` tags [YTFRONT-5022] ([b84ed7d](https://github.com/ytsaurus/ytsaurus-ui/commit/b84ed7d9e3c96e6a1f55e43945c8e4e5d808e373))
* **Operations/Runtime:** add absolute values [YTFRONT-5120] ([a986bd4](https://github.com/ytsaurus/ytsaurus-ui/commit/a986bd4796c42983ec522ffeedfb935b217dfef8))
* **Operations/Runtime:** add tooltips [YTFRONT-5120] ([a26b6e4](https://github.com/ytsaurus/ytsaurus-ui/commit/a26b6e41625c5963e3c604d8e76948657b77f4e8))
* **Queries/Result:** inline preview for `image`,`audio` tags  [YTFRONT-5022] ([0c400d7](https://github.com/ytsaurus/ytsaurus-ui/commit/0c400d73bd42396c758edf1c187604ea033a318e))
* **System:** add decimal value for cell-tag [YTFRONT-4939] ([f3d53bb](https://github.com/ytsaurus/ytsaurus-ui/commit/f3d53bb94dc515ba6dafd3d0198fd299c34e2111))
* **Dashboard2/PoolsWidget:** add favourite/custom list setting [YTFRONT-3400] ([989c148](https://github.com/ytsaurus/ytsaurus-ui/commit/989c1480c82e05f3ca4decc46f1258457078c70f))
* **Dashboard2/ServicesWidget:** add favourite/custom list setting [YTFRONT-3400] ([127d2c9](https://github.com/ytsaurus/ytsaurus-ui/commit/127d2c9e80238a0c8dae5b6552f205ca03c939cc))
* **Dashboard2:** add items count to widget header [YTFRONT-3400] ([040d913](https://github.com/ytsaurus/ytsaurus-ui/commit/040d913f22525a919695bff2725053feb6ba3bf9))
* **Groups:** replace idm column to external system [YTFRONT-5113] ([75d3473](https://github.com/ytsaurus/ytsaurus-ui/commit/75d347388cb2d5526ceb6df8df3916f5d8daef5f))
* **Monaco:** new colors for dark and light contrast theme [YTFRONT-5060] ([a49bf9d](https://github.com/ytsaurus/ytsaurus-ui/commit/a49bf9d3318d36432da64cdd2a07dae5148784b5))
* **Navigation/CreateTable:** add new aggregate type [YTFRONT-5153] ([7e0ee63](https://github.com/ytsaurus/ytsaurus-ui/commit/7e0ee63ca4ae849e55d483709f229ad346c13aee))
* **Navigation/YqlWidget:** qt operation link [YTFRONT-4994] ([8b9688c](https://github.com/ytsaurus/ytsaurus-ui/commit/8b9688c3e000716b1143cdd597b01d97944d9073))
* **Queries/List:** add additional list filters [YTFRONT-5058] ([fc5ad97](https://github.com/ytsaurus/ytsaurus-ui/commit/fc5ad976051beb0a6b7299e1f9cdda80ce4c7286))
* **Queries/List:** infinite query list [YTFRONT-5060] ([9a58558](https://github.com/ytsaurus/ytsaurus-ui/commit/9a5855808e5eb4292bf4b700df70158c44ad1a30))
* **Queries:** filter engines by selected cluster [YTFRONT-4852] ([7a0daaf](https://github.com/ytsaurus/ytsaurus-ui/commit/7a0daaf443d186a54c2f4cb8e7f24f9646ceb425))
* **Queries:** qt menu redesign [YTFRONT-4852] ([04a5b2c](https://github.com/ytsaurus/ytsaurus-ui/commit/04a5b2c5387a5aec06cad5c90f9c4eadabc5af25))
* **Users:** replace idm column to external system [YTFRONT-5113] ([dc25858](https://github.com/ytsaurus/ytsaurus-ui/commit/dc258587276e1c06e6917b95e827e569f1852e61))


#### Bug Fixes

* minor fix to publish new release ([e90a68b](https://github.com/ytsaurus/ytsaurus-ui/commit/e90a68bbb5f165acb4c9e9b0237a3978227c01f5))
* **Navigation/ACL:** fix a mispring [YTFRONT-5166] ([85f1c06](https://github.com/ytsaurus/ytsaurus-ui/commit/85f1c066d0f762ddc02bfafd46676792823e7635))
* **Operation/Details:** edit button should be always visible [YTFRONT-5164] ([658a55a](https://github.com/ytsaurus/ytsaurus-ui/commit/658a55a28fb5b1a04f43038395ebea62715f3e39))
* **Operation/Jobs:** do not allow to collapse 'Id/Address' column [YTFRONT-5171] ([0fb04a9](https://github.com/ytsaurus/ytsaurus-ui/commit/0fb04a912c4d4a255a9efe161aa94962d920a736))
* **UTF8:** encoding table schema [YTFRONT-5161] ([ded51be](https://github.com/ytsaurus/ytsaurus-ui/commit/ded51be9fbe5c26ed78caa69d6bde785bcf09510))
* **Accounts/DetailedUsage:** use all fileds of row for '/get-versioned-resource-usage' [YTFRONT-5187] ([a8e73d6](https://github.com/ytsaurus/ytsaurus-ui/commit/a8e73d6b57c8a939b84d83162b3a188c2b03b65f))
* **Acl:** added "vital" param to requestPermissions call, because it is required for "Register queue consumer (vital)" permission ([c7e96a2](https://github.com/ytsaurus/ytsaurus-ui/commit/c7e96a2ec55af73616dc6e8aea906cf4293cf4ce))
* **ACL:** get rid of 'mode: keep-missing-fields' [YTFRONT-5039] ([ea90bfa](https://github.com/ytsaurus/ytsaurus-ui/commit/ea90bfa3a927f168f3bf6da7cc4f1af684a37d32))
* **Operation:** utf8 error in description [YTFRONT-4982] ([f60f965](https://github.com/ytsaurus/ytsaurus-ui/commit/f60f965ddc6fd4c8d4756cc3fd66b12849edc2e6))
* **Navigation/Table:** use getInitialSettingsData from reducers [YTFRONT-5137] ([a94a549](https://github.com/ytsaurus/ytsaurus-ui/commit/a94a54986e2b409adbdce0440d7f902a2dcf84ad))
* **Queries/Result:** result tab priority [YTFRONT-5122] ([6d83a4b](https://github.com/ytsaurus/ytsaurus-ui/commit/6d83a4be1b9cff4291312250a8c4817c1b041bc0))
* **Scheduling:** use 'estimated_guarantee_resources' instead of 'promised_fair_share_resources' [YTFRONT-4015] ([4031d70](https://github.com/ytsaurus/ytsaurus-ui/commit/4031d70961dc27b9697ef5d7170d4b8b427afe22))
* **Accounts/DetailedUsage:** move navigation link from path to button [YTFRONT-4896] ([27ebab3](https://github.com/ytsaurus/ytsaurus-ui/commit/27ebab38598675470b198f9491d43b088f1df3b8))
* **Accounts:** typo in the graphic [YTFRONT-5151] ([3a1975c](https://github.com/ytsaurus/ytsaurus-ui/commit/3a1975c72a1999dcbeb59f22275ac86e96de4a8a))
* **ColumnSelector:** decode columns with non-english names [YTFRONT-4873] ([d9a552d](https://github.com/ytsaurus/ytsaurus-ui/commit/d9a552d7d7d801066d2a38abc6d665c86057f420))
* **Dashboard2/Accounts:** make mediums readable [YTFRONT-3400] ([4432bea](https://github.com/ytsaurus/ytsaurus-ui/commit/4432bea9fe9b44b913b65f41cd4ad8f19034b51d))
* **Dashboard2/Accounts:** merge column controls [YTFRONT-3400] ([d25f743](https://github.com/ytsaurus/ytsaurus-ui/commit/d25f743c3cc43d66bded37cb6cfeb83c0938dde4))
* **Dashboard2:** add limits for queries and operations [YTFRONT-3400] ([cd35267](https://github.com/ytsaurus/ytsaurus-ui/commit/cd35267539ec48251e27a72220960bf7ee510eee))
* **Modal:** close modal when pressing esc [YTFRONT-2533] ([72ddde9](https://github.com/ytsaurus/ytsaurus-ui/commit/72ddde9d60dbcce5c3a476896aa81b2e27b3480b))
* **Navigation:** fix cancellation ([cfa5d83](https://github.com/ytsaurus/ytsaurus-ui/commit/cfa5d836e6acb247a54cc19842b15706e073489f))
* **Queries/List:** incorrect order of elements when starting a query [YTFRONT-4770] ([f3a59a3](https://github.com/ytsaurus/ytsaurus-ui/commit/f3a59a3b907ef8e860d83d96fe5a1feec613cb82))
* **Queries/Navigation:** correct element icons [YTFRONT-5099] ([7e64c6d](https://github.com/ytsaurus/ytsaurus-ui/commit/7e64c6da3096eb4efb8ddffbcb6a0febb81727d3))
* **Queries/Result:** result tab priority [YTFRONT-5122] ([153313f](https://github.com/ytsaurus/ytsaurus-ui/commit/153313f7ecfbb92aa07a2d6f8bf2ab50e63d664e))
* **Queries/Share:** changed dependency on list query [YTFRONT-4770] ([bbb62f7](https://github.com/ytsaurus/ytsaurus-ui/commit/bbb62f79708b60ce3ff1745fdf5fee0623a18c32))

#### Code Refactoring

* **interface-helpers:** move format.DateTime to ui/src/common/hammer/format [YTFRONT-5171] ([53721f4](https://github.com/ytsaurus/ytsaurus-ui/commit/53721f4c0e9c81d41070d5ebc0240e5402be0427))

#### Dependencies

* The following workspace dependencies were updated
  * devDependencies
    * @ytsaurus/interface-helpers bumped from ^0.3.0 to ^1.0.0

{% endcut %}


{% cut "**1.98.0**" %}

**Release date:** 2025-07-01


#### [1.98.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.97.0...ui-v1.98.0) (2025-07-01)

#### Features

* **Accounts:** add custom accounts usage base url [YTFRONT-4927] ([510587e](https://github.com/ytsaurus/ytsaurus-ui/commit/510587e7da13a6990e949316f1d4a74147e874ba))
* **Consumer:** add register buttons for queues and consumers [YTFRONT-4869] ([f9d0a84](https://github.com/ytsaurus/ytsaurus-ui/commit/f9d0a843905362050fa80ddbdffeb4f5056d078f))
* **Dialog:** add filter for services select control [YTFRONT-5007] ([51aab9b](https://github.com/ytsaurus/ytsaurus-ui/commit/51aab9b249b07cedcf7039dbb13b9235c3036cd1))
* **Dialog/TimeDuration:** forbid all symbols except numbers and latin chars [YTFRONT-4995] ([380c4ab](https://github.com/ytsaurus/ytsaurus-ui/commit/380c4ab0ad8aa531c8acf3647d397301012d668c))
* **Dialog/AccountsMultiple:** add items deleting labels [YTFRONT-3400] ([44e34aa](https://github.com/ytsaurus/ytsaurus-ui/commit/44e34aa32bf1bf7222ab9f7fa1206cf978cc21d1))
* **Navigation:** add icons for queue producers and consumers [YTFRONT-4981] ([401465d](https://github.com/ytsaurus/ytsaurus-ui/commit/401465d2250ba6e893ac9b5bca20a09067869e35))
* **Navigation/Description:** add external description option [YTFRONT-4680] ([dbfe196](https://github.com/ytsaurus/ytsaurus-ui/commit/dbfe196174aad542e6616acf5677f92edb064644))
* **Navigation/Description:** display loaded description [YTFRONT-5015] ([76fc3d3](https://github.com/ytsaurus/ytsaurus-ui/commit/76fc3d358db8719f350d9969d40eb575bf2836c9))
* **Navigation/TabletErorrs:** make error message button and expand first error [YTFRONT-4740] ([865f759](https://github.com/ytsaurus/ytsaurus-ui/commit/865f7598ba0ce86e9b5e8e3cb30c21f76b2aa431))
* **Navigation/TabletErrors:** remove useless url params on unmount [YTFRONT-4740] ([f755d62](https://github.com/ytsaurus/ytsaurus-ui/commit/f755d62e6853b8a6764f3938bef348fc51bcde80))
* **Navigation/TabletErrors:** small upgrades [YTFRONT-4740] ([e9b459b](https://github.com/ytsaurus/ytsaurus-ui/commit/e9b459bbdd146ba359d45f7f59a6a90b6b272fd1))
* **Navigation/MapNode:** Support uploading files through the UI [[#1173](https://github.com/ytsaurus/ytsaurus-ui/issues/1173)] ([8acf29e](https://github.com/ytsaurus/ytsaurus-ui/commit/8acf29e863e427f247653706dd2d66119a91ca51))
* **Operation/Details:** error.job_id attributes should be displayed as a link [YTFRONT-3916] ([fd38edf](https://github.com/ytsaurus/ytsaurus-ui/commit/fd38edf1e6b11407fbd81fef146d69e2ed8f9a97))
* **Operation/Jobs:** add 'With interruption info' attribute support [YTFRONT-4810] ([02cf8ea](https://github.com/ytsaurus/ytsaurus-ui/commit/02cf8ea5c667c413f57579ce33e57c3134c75ab1))
* **Queries/Navigation:** add open new tab button [YTFRONT-4985] ([4bb9786](https://github.com/ytsaurus/ytsaurus-ui/commit/4bb978649a4a956913dd4d585c9a399ab95fdf9b))
* **Queries:** add multi chart [YTFRONT-4999] ([d760704](https://github.com/ytsaurus/ytsaurus-ui/commit/d760704cdfa14914c019a339b872a30528a94044))
* **Queue/Exports:** move ms to tooltips [YTFRONT-4995] ([77010c4](https://github.com/ytsaurus/ytsaurus-ui/commit/77010c4c59d5361421b8845b72369eba66dc7d67))
* **Queries/Monaco:** scroll editor to selected line [YTFRONT-4915] ([9a747aa](https://github.com/ytsaurus/ytsaurus-ui/commit/9a747aae7241ad742988c54b1b645b4c3b93d1ba))
* **Scheduling:** scroll into ref operation view [YTFRONT-4941] ([237e207](https://github.com/ytsaurus/ytsaurus-ui/commit/237e207a04b68bf311f52d6ad3eb326ada718ff2))
* **Scheduling/Overview:** allow to customize columns [YTFRONT-4402] ([06d92bc](https://github.com/ytsaurus/ytsaurus-ui/commit/06d92bca76d61f26938b253b15f2a69e23a9ad18))
* **TabletError:** add testing api option [YTFRONT-4740] ([9bd661d](https://github.com/ytsaurus/ytsaurus-ui/commit/9bd661dbd3e5c6ddced004a27622914f4c055e10))
* **VCS:** remember user last choice [YTFRONT-4504] ([d253b09](https://github.com/ytsaurus/ytsaurus-ui/commit/d253b096391acb94700069dc09b6c8d00145cd3a))
* **UIFactory:** extend the UIFactory for external descriptions [YTFRONT-4680] ([7201467](https://github.com/ytsaurus/ytsaurus-ui/commit/7201467d87cba61c0fd5ca172fe30032d16be5eb))
* **UIFactory:** create analytics factory ([9e47bdb](https://github.com/ytsaurus/ytsaurus-ui/commit/9e47bdb9629b3603fce6ac0ec5a1f32107aa69b7))
* **UIFactory/Query:** add chat [YTFRONT-4813] ([757de1f](https://github.com/ytsaurus/ytsaurus-ui/commit/757de1fbbe45a0381be13e70e1ba8efe0530f1be))

#### Bug Fixes

* **Accounts:** use recoursive_resource_usage for 'Aggregation' [YTFRONT-5024] ([a15f1a9](https://github.com/ytsaurus/ytsaurus-ui/commit/a15f1a906b6e0dc1ba6319998502f1aabb540d34))
* **BundleEditorDialog:** minor fixes [YTFRONT-4947] ([7a7432a](https://github.com/ytsaurus/ytsaurus-ui/commit/7a7432a4644e809a24a75f177a64dbd26506f81a))
* **CreateTableModal:** rename 'Queue' =&gt; 'Queue table' [YTFRONT-4953] ([50b46b1](https://github.com/ytsaurus/ytsaurus-ui/commit/50b46b1b59327100a3b1bf986eb5b3260978ac5f))
* **Dashboard2:** config editting [YTFRONT-3400] ([1d3cea1](https://github.com/ytsaurus/ytsaurus-ui/commit/1d3cea14ed3d82858a143c28015a64ed11fddbff))
* **Dashboard:** autohieght, typos, extra contol in operations settings, navigation fallback [YTFRONT-3400] ([3a55fc1](https://github.com/ytsaurus/ytsaurus-ui/commit/3a55fc1bbb1a3229c6d48d6fdbc7a30dd4395b70))
* **Job:** host should be clickable [YTFRONT-4958] ([fd55fa5](https://github.com/ytsaurus/ytsaurus-ui/commit/fd55fa507546711be886d1be9bfa65682bb94508))
* **Dialog/AccountsMultiple:** gaps [YTFRONT-3400] ([0d2213f](https://github.com/ytsaurus/ytsaurus-ui/commit/0d2213f06d879fe5a15d533adfa589ad72f7b5dc))
* **Navigation/Consumers:** icon position in button [YTFRONT-4869] ([f59d4c5](https://github.com/ytsaurus/ytsaurus-ui/commit/f59d4c599a28c9e71a476db1bf7e8fd75c6e630d))
* **Navigation/Queue:** add 'mapping in proggress' message [YTFRONT-4954] ([0b14083](https://github.com/ytsaurus/ytsaurus-ui/commit/0b1408368e944b0cfb8bca71023d17c2a08e9bdf))
* **Navigation/Tablets:** arrow click [YTFRONT-5030] ([044d0ae](https://github.com/ytsaurus/ytsaurus-ui/commit/044d0ae66d7fa10970ee46b9186cb770a4f83e33))
* **Navigation/AccessLog:** add 'Recursive' checkbox filter [YTFRONT-5023] ([3e96cca](https://github.com/ytsaurus/ytsaurus-ui/commit/3e96cca6aac21c08008e2b9f281d0bdcbbf65ebe))
* **Navigation/Queue:** add consumer creation [YTFRONT-4869] ([69c23f9](https://github.com/ytsaurus/ytsaurus-ui/commit/69c23f9e4b1cba38cf2aea3cc803cdd64eb937b9))
* **Navigation/UploadFileManager:** reset state after closing and shift menu item ([b7ac155](https://github.com/ytsaurus/ytsaurus-ui/commit/b7ac155abd41fd6dfedbfebb4b7a8923aa22be7b))
* **Operations:** dont show special status instead of suspended [YTFRONT-5035] ([f305463](https://github.com/ytsaurus/ytsaurus-ui/commit/f305463f731c89afbd5e0b5ac5253303f1fb2940))
* **Operations:** jobs timeline [YTFRONT-4695] ([6f0296e](https://github.com/ytsaurus/ytsaurus-ui/commit/6f0296e1de33b764a2ae56f481ce33ffd23cba06))
* **Operation:** do not display 'Data flow' for 'vanilla' [YTFRONT-4959] ([50c8a1f](https://github.com/ytsaurus/ytsaurus-ui/commit/50c8a1f6a01198fc652e91680b33d09c488a9d37))
* **Operation:** do not show 'total job wall time'/'total cpu time spent' for 'vanilla' [YTFRONT-4960] ([4d9dec9](https://github.com/ytsaurus/ytsaurus-ui/commit/4d9dec9b02b87807aba8cebeff9c43756f8577c7))
* **Operations:** add performance analysis system link [YTFRONT-4924] ([1bd2b69](https://github.com/ytsaurus/ytsaurus-ui/commit/1bd2b69ca71b8538504ce33b469d9c905e784410))
* **Operations:** change runtime meta data [YTFRONT-4940] ([db63474](https://github.com/ytsaurus/ytsaurus-ui/commit/db63474a5e160f2fb2207f9cb61c0c4d3c4f3cf7))
* **Operation/Details/Tasks:** add 'Hide empty' filter to "Aborted statistics" dialog [YTFRONT-5012] ([4a55020](https://github.com/ytsaurus/ytsaurus-ui/commit/4a550206c57eab90ef5456f7b8e5e8e88639e9c8))
* **Operation/LivePreview:** fix for 'n.map is not a function ...' [YTFRONT-5021] ([71fda06](https://github.com/ytsaurus/ytsaurus-ui/commit/71fda06115b2375d389ff33b847243ccb27b8de8))
* **Operation/Details:** set default limit for visible count of env/layers [YTFRONT-4962] ([29a1217](https://github.com/ytsaurus/ytsaurus-ui/commit/29a12172d24a478aa943b52248c030a06f76ae23))
* **Operations/Detail:** special status tooltip [YTFRONT-4943] ([7405792](https://github.com/ytsaurus/ytsaurus-ui/commit/7405792875fe57cff6a4cfd30199f480f96262ba))
* **Operations/Detail:** add special statuses for gpu vanilla operations [YTFRONT-4943] ([a4a6a40](https://github.com/ytsaurus/ytsaurus-ui/commit/a4a6a40796058c658cedf200700788612763f81f))
* **Queries/Chart:** yql datetime in chart [YTFRONT-4937] ([e3d692b](https://github.com/ytsaurus/ytsaurus-ui/commit/e3d692b0036786aace2d3fc9d87eb9f0a38565ee))
* **Queue/Exports:** export duration validator [YTFRONT-4995] ([462525c](https://github.com/ytsaurus/ytsaurus-ui/commit/462525c7da3a959794f6feac7dc0724ba2efd44d))
* **Scheduling:** display 'Automatically calculated' only for pools [YTFRONT-3812] ([a2b80d3](https://github.com/ytsaurus/ytsaurus-ui/commit/a2b80d3abb37c5565ffe0faf43143cb1d7d85dec))
* **Scheduling:** use fifo_index as sort column for FIFO-pool by default [YTFRONT-4942] ([422ffe7](https://github.com/ytsaurus/ytsaurus-ui/commit/422ffe7f50afdcbb169fe971a7502fd14f72281f))
* **Timeline:** from to error [YTFRONT-5011] ([b58e037](https://github.com/ytsaurus/ytsaurus-ui/commit/b58e0376e88ff9af6d0afceb6debbec5a16a3ccf))


{% endcut %}


{% cut "**1.91.3**" %}

**Release date:** 2025-05-21


#### [1.91.3](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.91.2...ui-v1.91.3) (2025-05-21)

#### Features

* **Accounts:** path string to link [YTFRONT-4896] ([a514bbe](https://github.com/ytsaurus/ytsaurus-ui/commit/a514bbec1287e561dac0bca4aa8c2c44d06bb85e))
* **Bundles:** add query memory limit [YTFRONT-4651] ([eca2997](https://github.com/ytsaurus/ytsaurus-ui/commit/eca29978c3962cf40a593bec32865390fee0dc37))
* **Bundles:** bit/s to B/S [YTFRONT-4523] ([34f08aa](https://github.com/ytsaurus/ytsaurus-ui/commit/34f08aa3d5a8b93a79e007400eae64adcd010ac2))
* **Download:** copy to clipboard [YTFRONT-4895] ([623201e](https://github.com/ytsaurus/ytsaurus-ui/commit/623201e8b19a4a8e1327bf96eb3455b51683b019))
* **Monaco:** vim mode[YTFRONT-4807] ([00b5cb7](https://github.com/ytsaurus/ytsaurus-ui/commit/00b5cb701d3af062dfb6d4dcea40683bc3b98340))
* **Navigation/Queue:** add features for queues exports [YTFRONT-4482] ([58baccb](https://github.com/ytsaurus/ytsaurus-ui/commit/58baccb4374a0f6282d331654bf0e53ce825acd2))
* **Navigation/SymLinks:** add target_path button [YTFRONT-4224] ([8072c53](https://github.com/ytsaurus/ytsaurus-ui/commit/8072c53b4022fd84d97d8af2a347a6f3b0e0313d))
* **Operations/List:** apply transactions to 'in'/'out' tables [YTFRONT-3134] ([db581dd](https://github.com/ytsaurus/ytsaurus-ui/commit/db581dd0274e3d099d07f368be575aeeec6a2167))
* **Queries:** save user engine choice [YTFRONT-4816] ([6865bde](https://github.com/ytsaurus/ytsaurus-ui/commit/6865bdeaf77ff8fca429ec4df51eb4f86160145c))
* **Queries/Graph:** increased visibility of the number of tasks [YTFRONT-4812] ([d393feb](https://github.com/ytsaurus/ytsaurus-ui/commit/d393feb2c5d62e43f7ff6bff135206d4896cbd25))
* **Queries:** open navigation on path click [YTFRONT-4802] ([2630b4c](https://github.com/ytsaurus/ytsaurus-ui/commit/2630b4c29aadc9b5c1559cec69bd29a6bf0aa3a0))
* **Queries:** redirect confirm modal [YTFRONT-4809] ([78da341](https://github.com/ytsaurus/ytsaurus-ui/commit/78da34127592eec1afb81f1c6d43e0e8a2bdd238))
* **Queries:** show status in chyt clique selector [YTFRONT-4825] ([3c5065a](https://github.com/ytsaurus/ytsaurus-ui/commit/3c5065ac85050b9f23c501c264a27f4ec9c9732e))
* **Scheduling:** add icon for pools with effective_lightweight_operations_enabled [YTFRONT-4275] ([395c512](https://github.com/ytsaurus/ytsaurus-ui/commit/395c5122c06c0255fe69a0756e70f3ca3564d003))
* update @ytsaurus/javascript-wrapper ([a953b63](https://github.com/ytsaurus/ytsaurus-ui/commit/a953b6370921e691013b74a6909906a64d2201f4))


#### Bug Fixes

* **Account:** normalize timestamp [YTFRONT-4913] ([76b38e1](https://github.com/ytsaurus/ytsaurus-ui/commit/76b38e17eb8574668ea5d4c00ec5b5434482d850))
* **ACL:** incorrect 'Inherit ACL' value [YTFRONT-4718] ([65d1f3e](https://github.com/ytsaurus/ytsaurus-ui/commit/65d1f3ecc97f7739390db398def3bdd93e22664f))
* **ChaosReplicatedTable:** use `alter_table_replica` to control `enable_replicated_table_tracker` [YTFRONT-4796] ([6983439](https://github.com/ytsaurus/ytsaurus-ui/commit/6983439c34cc6f9536824c3aa7f5125bfdfc3ab5))
* **DataTableYT:** table's Attributes should be properly scrollable when QT-side-panel is opened [YTFRONT-4900] ([9f64661](https://github.com/ytsaurus/ytsaurus-ui/commit/9f6466109d8935d2c231a4c858f5e477720dbe99))
* **Dialog:** update event on clean [YTFRONT-4829] ([9d8ab15](https://github.com/ytsaurus/ytsaurus-ui/commit/9d8ab15fbbf33c3aacc2cb090e87501f91337b86))
* **Navigation:** change title from tables [YTFRONT-4822] ([ddfa239](https://github.com/ytsaurus/ytsaurus-ui/commit/ddfa239e6becbfc492f0f53c25535c1e1d9972ad))
* **Navigation/AccessLog:** show destination path [YTFRONT-4341] ([18ac142](https://github.com/ytsaurus/ytsaurus-ui/commit/18ac142f2b21902ff90dace3284e478a0704879e))
* **Navigation/DownloadManager:** provide more details in error toaster [YTFRONT-4790] ([25c35ad](https://github.com/ytsaurus/ytsaurus-ui/commit/25c35ad41998c185929ff4553494754363f525d4))
* **Navigation/DynamicTable:** do not hide key columns in dyn tables [YTFRONT-4826] ([edc5133](https://github.com/ytsaurus/ytsaurus-ui/commit/edc51336f5978c4cea160e36bc6419646f3dde0b))
* **Navigation/MapNode:** add ordered table icon ([f872cde](https://github.com/ytsaurus/ytsaurus-ui/commit/f872cdec0d8fe80f1cc1786aedd9b64d0d13e353))
* **Navigation/MapNode/TableSortModal:** column encoding [YTFRONT-4873] ([6a4ab81](https://github.com/ytsaurus/ytsaurus-ui/commit/6a4ab8113e0e44ffdfbe9a34b89f64df4a98858b))
* **Navigation/Metadata:** monaco vim mode [YTFRONT-4908] ([10d4f4b](https://github.com/ytsaurus/ytsaurus-ui/commit/10d4f4bc333a7ae50befbbd4ffc7318cdab160dc))
* **Navigaion/NavigationError:** n.map is not a function [YTFRONT-4858] ([ff5e6c6](https://github.com/ytsaurus/ytsaurus-ui/commit/ff5e6c6bb21e9ccbd78978ca9dc6d581282feee8))
* **Navigation/Table:** fix offset and pagination of unmounted tables [YTFRONT-4021] ([f7b2b68](https://github.com/ytsaurus/ytsaurus-ui/commit/f7b2b68ff0b5e04c96f6dc4a9d45664bf6a7530e))
* **Navigation/Table/SidPanel:** do not close side panel when go up to parent folder [YTFRONT-4729] ([b9afc28](https://github.com/ytsaurus/ytsaurus-ui/commit/b9afc28826bbef9126107ce1d109350def1f95bd))
* **Navigation/Table/CellPreview:** preview should work for columns with slashes [YTFRONT-4797] ([c93e8bc](https://github.com/ytsaurus/ytsaurus-ui/commit/c93e8bc2a2e8657c138c107b658cea3f37e4f090))
* **Navigation/Table/SidePanel: ** get rid of duplicated scrollbars [YTFRONT-4840] ([7c81ad5](https://github.com/ytsaurus/ytsaurus-ui/commit/7c81ad5cdad68a73cba2e602bcdd952793ee4391))
* **Naviagation/Table/Download:** do not use `dump_error_into_response: true` [YTFRONT-4856] ([6560aa7](https://github.com/ytsaurus/ytsaurus-ui/commit/6560aa7bba4d5eeb30de1300c541605033348713))
* **Nodes:** wrong color in contrast high theme [YTFRONT-4610] ([e00b6c0](https://github.com/ytsaurus/ytsaurus-ui/commit/e00b6c04d97d3d2ca4f4ff3a32473fedb3a0b3b0))
* **Operations:** big rows count in jobs [YTFRONT-4854] ([be0bde3](https://github.com/ytsaurus/ytsaurus-ui/commit/be0bde3ad66cc37eb2da22904df0e3f1be8bf08e))
* **Operation/Details/Alerts:** fix for infor urls [YTFRONT-4800] ([c6961a0](https://github.com/ytsaurus/ytsaurus-ui/commit/c6961a043218717ac20269a0aa09052a094445a4))
* **Operation/Jobs:** fix a misprint [YTFRONT-4798] ([31ea5a1](https://github.com/ytsaurus/ytsaurus-ui/commit/31ea5a137cbc2f7f4c8e46890e08a16d532a0042))
* **Operation/Statistics:** add copy button for values [YTFRONT-4805] ([d1832fe](https://github.com/ytsaurus/ytsaurus-ui/commit/d1832fe864049ec3f350fa5178218ed1addde290))
* **OperationJobsTable:** input path modal close on esc ([80629ec](https://github.com/ytsaurus/ytsaurus-ui/commit/80629ecc2b40eb8d43cc2621d270d13b4425ed16))
* **Operations/List:** table content should not jump up when toolbar becomes sticky [YTFRONT-4870] ([f227af2](https://github.com/ytsaurus/ytsaurus-ui/commit/f227af2f809c6948336bb4af00fb7eaf0e7cae99))
* **Operation/Jobs:** set min-width of 'Incarnation' filter popup [YTFRONT-4836] ([ee9fed0](https://github.com/ytsaurus/ytsaurus-ui/commit/ee9fed006d8d7c852d198f4357671aa1b91694a6))
* **PathEditor:** bring back onBlur callback ([dea479b](https://github.com/ytsaurus/ytsaurus-ui/commit/dea479be18178a9f6a891f75f1fc027fffa61c9a))
* **Queries:** allow empty ACO for existing query ([2ee0d26](https://github.com/ytsaurus/ytsaurus-ui/commit/2ee0d2661d2af2dca765b386ef6fbf33a780f6db))
* **Queries:** fix excel download link ([912dc1e](https://github.com/ytsaurus/ytsaurus-ui/commit/912dc1ef7a87db854c0c858facb02de554bbb9b4))
* **Queries:** chyt progress [YTFRONT-4833] ([a535db2](https://github.com/ytsaurus/ytsaurus-ui/commit/a535db27cf855f20b70298f618bf3956128af24a))
* **Queries:** move edit button to the right of the line [[#1033](https://github.com/ytsaurus/ytsaurus-ui/issues/1033)] ([af269c8](https://github.com/ytsaurus/ytsaurus-ui/commit/af269c805cef6656edd1e3865e5b4dabf7da707b))
* **Queries:** fix show inactive clique status when it is active and good ([8d4f2b7](https://github.com/ytsaurus/ytsaurus-ui/commit/8d4f2b732c2712b7c9ec1dbbead117d794812c8b))
* **Queries:** get path request ([45964bd](https://github.com/ytsaurus/ytsaurus-ui/commit/45964bd3d45f224cac600b1dc9a9f7b76c9a1761))
* **Queries:** improve navigation speed [YTFRONT-4771] ([f60341d](https://github.com/ytsaurus/ytsaurus-ui/commit/f60341d48c8675760a717d0acbf0196977bd226e))
* **Queries:** schema fix [YTFRONT-4771] ([c008d19](https://github.com/ytsaurus/ytsaurus-ui/commit/c008d1976db7119c7d2f4bfb3034ae71b41987db))
* **Queries:** update draft while query is running [YTFRONT-4791] ([d4647da](https://github.com/ytsaurus/ytsaurus-ui/commit/d4647daa58e8c7e060da730369f7711498a03dc8))
* **Queries/History:** status icon [YTFRONT-4821] ([b5f29a9](https://github.com/ytsaurus/ytsaurus-ui/commit/b5f29a9193127b1f8cde393eb126e202a4cb4e0b))
* **Queries/Monaco:** cut path suggest [YTFRONT-4806] ([9fb5334](https://github.com/ytsaurus/ytsaurus-ui/commit/9fb5334338e9b097d6e8dc85a6fcb0d5cffece0d))
* **Queries:** fix navigation by url path [YTFRONT-4802] ([fc5f0e5](https://github.com/ytsaurus/ytsaurus-ui/commit/fc5f0e5bc7b23d5eab5aff39e2fecd34199c0e58))
* **Queries:** fixed query suggest, now `` are fully replaced by `//` [[#1032](https://github.com/ytsaurus/ytsaurus-ui/issues/1032)] ([d1d43de](https://github.com/ytsaurus/ytsaurus-ui/commit/d1d43de49a224035b23a01c244d40ce3cfa7d62d))
* **RequestPermissions:** mark 'Subjects' field as required [YTFRONT-4745] ([ed3ec63](https://github.com/ytsaurus/ytsaurus-ui/commit/ed3ec633ee694d18157cc1388c0d955dc0ba8971))
* **Scheduling:** fix a mispring (+new icon) [YTFRONT-4275] ([919a26d](https://github.com/ytsaurus/ytsaurus-ui/commit/919a26d4c91d7f36443edbe30996fc29855eba02))
* **Scheduling/Details:** operation limits [YTFRONT-4820] ([4798733](https://github.com/ytsaurus/ytsaurus-ui/commit/4798733bd04be733bb1bb2d0bade339c517fa010))
* **System:** masters on small monitors [YTFRONT-4830] ([6e46f6c](https://github.com/ytsaurus/ytsaurus-ui/commit/6e46f6cf8967b39d2e27afa74b0ed19ecc8fea4d))
* **Users/GroupSuggest:** group suggest visibility [YTFRONT-4737] ([2084021](https://github.com/ytsaurus/ytsaurus-ui/commit/2084021c65c153117331d80fc177fc8c167e0a7d))
* **UI/Layout:** use 'maxContentWidth' for the most of pages [YTFRONT-4149] ([259754a](https://github.com/ytsaurus/ytsaurus-ui/commit/259754ab35a7df49c606146da1269b34f49ef1ed))
* **UIFactory:** rework `UIFactory.renderAppFooter` method [YTFRONT-4149] ([b3dc9b2](https://github.com/ytsaurus/ytsaurus-ui/commit/b3dc9b259df791822f42f41b09bdf88c7ff9464f))
* **YTErrorBlock:** copy button should use the same text format [YTFRONT-3310] ([cd03ff8](https://github.com/ytsaurus/ytsaurus-ui/commit/cd03ff8e86ee32e3ea20029467eb318d218bae16))
* **YsonView:** parsed value scroll [YTFRONT-4823] ([43e181d](https://github.com/ytsaurus/ytsaurus-ui/commit/43e181d5e19e2d8c3037f2db196a0ab37121ece5))
* **localmode:** sync cluster_name [YTFRONT-4326] ([3bddca1](https://github.com/ytsaurus/ytsaurus-ui/commit/3bddca1a4403da895605b992181b6c976c0d6209))
* changed default value of UIFactory.onChytAliasSqlClick - now sql button is working on chyt page ([2ed97f0](https://github.com/ytsaurus/ytsaurus-ui/commit/2ed97f06a344eb9bfd8a2112e4c144180d0aaa5a))
* do not show cluster selectors in case when we have only one cluster ([01ce947](https://github.com/ytsaurus/ytsaurus-ui/commit/01ce947dfb86ada716ae3fb5acbc47356a77737f))
* batch api types ([41a3d68](https://github.com/ytsaurus/ytsaurus-ui/commit/41a3d6823d596e913dffdf5e70e5de0f90a60e97))
* queue creation and deletion error messages ([35bc106](https://github.com/ytsaurus/ytsaurus-ui/commit/35bc106219d7926eaf78aa93a8f5d1242c07ad67))



{% endcut %}


{% cut "**1.84.0**" %}

**Release date:** 2025-03-12


#### [1.84.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.83.0...ui-v1.84.0) (2025-03-12)


#### Features

* **Components/Versions:** display more columns of node types [YTFRONT-4406] ([bbd62fa](https://github.com/ytsaurus/ytsaurus-ui/commit/bbd62fa594642ca6888a78392cf27ac3dc672472))
* **Components/Versions:** display more columns of node types [YTFRONT-4406] ([bbd62fa](https://github.com/ytsaurus/ytsaurus-ui/commit/bbd62fa594642ca6888a78392cf27ac3dc672472))
* **Navigation/Table:** add remount option [YTFRONT-3593] ([a1ea452](https://github.com/ytsaurus/ytsaurus-ui/commit/a1ea452174491b7304787c5a10b9066b150a11a4))
* **Navigation/Table:** allow to view umounted dynamic tables [YTFRONT-4021] ([519a1b7](https://github.com/ytsaurus/ytsaurus-ui/commit/519a1b763cfb317ff4e4198d0396fdb0b6cbcd0e))
* **Navigation:** change resource usage metadata [YTFRONT-4764] ([5247a7f](https://github.com/ytsaurus/ytsaurus-ui/commit/5247a7f952200e3bebae41ee382b75c207ce8adc))
* **Operation/Jobs:** add 'Incarnation' filter [YTFRONT-4684] ([fa3f2e1](https://github.com/ytsaurus/ytsaurus-ui/commit/fa3f2e10a92be34a59c2da9f1313f2af53cd6db1))
* **Operations/Details:** display layer paths [YTFRONT-4618] ([43f783d](https://github.com/ytsaurus/ytsaurus-ui/commit/43f783dae33dee87174d6a799d2bd64c3b0326ba))
* **Queries:** add full result link [YTFRONT-4674] ([c32a867](https://github.com/ytsaurus/ytsaurus-ui/commit/c32a867b7ba1b4bbc4b4f90ca9c932a0f62550cb))

#### Bug Fixes

* added proper padding in datepicker popup ([96f19af](https://github.com/ytsaurus/ytsaurus-ui/commit/96f19af972c6e429edccb639989fd599e1b4567e))
* **Components:** add margin for custom footers [YTFRONT-4406] ([48b5e40](https://github.com/ytsaurus/ytsaurus-ui/commit/48b5e401b1aa7b982ecd1919f26f7a87330e26a6))
* **Components:** processing node list data [YTFRONT-4765] ([e27323b](https://github.com/ytsaurus/ytsaurus-ui/commit/e27323baad47aae7c489502fa9fc0ecd9ae4e61e))
* **localmode/Queries/QueryClusterSelector:** use cluster from get_query_tracker_info and //sys/[@cluster](https://github.com/cluster)_name [YTFRONT-4326] ([becf0ec](https://github.com/ytsaurus/ytsaurus-ui/commit/becf0ec4a35336fbfe60c00c5153ee056f4d16a1))
* **navigation:** Fix typo in AccessLog ([fdecdbe](https://github.com/ytsaurus/ytsaurus-ui/commit/fdecdbe1c0082337d63b01d7d42640993b78da24))
* **OAuth:** replace Authorization header to access_token cookie [[#958](https://github.com/ytsaurus/ytsaurus-ui/issues/958)] ([2a3d604](https://github.com/ytsaurus/ytsaurus-ui/commit/2a3d604a59992d218851d790b668176b5cbe2408))
* **Operations:** utf8 statistic [YTFRONT-4700] ([e344415](https://github.com/ytsaurus/ytsaurus-ui/commit/e344415863e601d8f44857f482d018232a344a4a))
* **Queries:** change result align [YTFRONT-4736] ([95cd793](https://github.com/ytsaurus/ytsaurus-ui/commit/95cd793f073f2dad716581e0878000affb6c6084))
* **Queries:** now change of query ACO does not reset chart state and vice versa [[#1006](https://github.com/ytsaurus/ytsaurus-ui/issues/1006)] ([1a94c7f](https://github.com/ytsaurus/ytsaurus-ui/commit/1a94c7f8c7561d98bf01ebe3401fbb2c9a5ac94a))
* **Queries:** the body of the query is not displayed in a new window [[#266](https://github.com/ytsaurus/ytsaurus-ui/issues/266)] ([c80f1ed](https://github.com/ytsaurus/ytsaurus-ui/commit/c80f1ed4881021ffadbff054905376a2abe50256))
* **Queries:** fix exporting query results via excel [[#1022](https://github.com/ytsaurus/ytsaurus-ui/issues/1022)] ([4de3573](https://github.com/ytsaurus/ytsaurus-ui/commit/4de357391c9554389ee1638b21d3d9ae73addbdc))
* **Queries:** fixed insert of suggested path in query editor [[#1027](https://github.com/ytsaurus/ytsaurus-ui/issues/1027)] ([6515314](https://github.com/ytsaurus/ytsaurus-ui/commit/6515314ba74f60ea206c0ffccec232785b459368))
* **Queries:** now all area of query history item is clickable [[#1000](https://github.com/ytsaurus/ytsaurus-ui/issues/1000)] ([2ff5dec](https://github.com/ytsaurus/ytsaurus-ui/commit/2ff5dece061e8b585d7d2e4a2469254c22a5759a))


{% endcut %}


{% cut "**1.82.1**" %}

**Release date:** 2025-02-15


#### [1.82.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.82.0...ui-v1.82.1) (2025-02-15)


#### Bug Fixes

* **Navigation/Table:** query buttons should be always visible [YTFRONT-4706] ([8d19235](https://github.com/ytsaurus/ytsaurus-ui/commit/8d19235558016e2d342b4973c84c6d0fa09c88f5))
* **Navigation:** add missing attribute & fix copy button ([f18fd4e](https://github.com/ytsaurus/ytsaurus-ui/commit/f18fd4e21f328a638241d03de8f5140b1dc48a77))

{% endcut %}


{% cut "**1.82.0**" %}

**Release date:** 2025-02-14


#### [1.82.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.81.0...ui-v1.82.0) (2025-02-14)


#### Features

* **CHYT/Tabs:** add tab for logs link [YTFRONT-4675] ([fc2f4b9](https://github.com/ytsaurus/ytsaurus-ui/commit/fc2f4b9fa6135b0460921582635d5ae892caeb7c))
* **Navigation/NavigationError:** add copy button and impove error details [YTFRONT-4049] ([fb70cba](https://github.com/ytsaurus/ytsaurus-ui/commit/fb70cba73da9891e84fb3fc0b072752f05b6e108))
* **Operations:** preempted jobs [YTFRONT-4641] ([472d41e](https://github.com/ytsaurus/ytsaurus-ui/commit/472d41e25e79e408ed09ffb1192e38b0baa0b894))
* **Settings:** change suggestions settings item [YTFRONT-4703] ([4c1776c](https://github.com/ytsaurus/ytsaurus-ui/commit/4c1776cefc8348d2eaba97be83f05b397ff52818))
* **CopyObjectModal:** add recursive folders creating while copying [YTFRONT-3041] ([670c084](https://github.com/ytsaurus/ytsaurus-ui/commit/670c084f758d30477ca0cc9915070e06aacbcd62))
* **ManageTokens:** optional password for token issuing ([e696da8](https://github.com/ytsaurus/ytsaurus-ui/commit/e696da8540821afc5f471c2c3210c75bb9deba58))
* **Navigation:** add pages for 500 and 901 errors [YTFRONT-4049] ([8b20786](https://github.com/ytsaurus/ytsaurus-ui/commit/8b20786304fcdc2cf3210756483f48d7bc92d836))
* **System/Masters:** allow to 'Switch leader' for 'Secondary masters' and 'Timestamp provider' [YTFRONT-4214] ([20762b3](https://github.com/ytsaurus/ytsaurus-ui/commit/20762b3091b4d9994404b79d7f405302ae8b1cb5))
* **Navigation/Document:** allow `YQL query` button if _yql_type == "view" [YTFRONT-4463] ([0421120](https://github.com/ytsaurus/ytsaurus-ui/commit/0421120c1cd1e9e7c60a500973cf2dde0645372d))
* **Job/Statistics:** use operation_statistics_descriptions from supported_features [YTFRONT-3522] ([abf49e5](https://github.com/ytsaurus/ytsaurus-ui/commit/abf49e5af07010f7253a01e211e8a19f3a131e3a))
* **Node:** display node version [YTFRONT-4555] ([0e460d2](https://github.com/ytsaurus/ytsaurus-ui/commit/0e460d29bf528a34ea48f4ffef4bccb24dde32f1))
* **DownloadManager:** add setting for filename [YTFRONT-3564] ([285d075](https://github.com/ytsaurus/ytsaurus-ui/commit/285d075f06a478b096628bf69ada5aa355492eef))
* **Queries:** new progress graph [YTFRONT-4112] ([24c142e](https://github.com/ytsaurus/ytsaurus-ui/commit/24c142e1bd8fdff6cb948fd1a72c838d6d216db9))
* **Navigation/CreateTableModal:** add queue creation option [YTFRONT-4658] ([df445ed](https://github.com/ytsaurus/ytsaurus-ui/commit/df445ed485b5e8983a204fbcb7965d2a7fe15763))
* now UIFactory.getNavigationExtraTabs allows to request additional attributes for navigation node ([36a2d12](https://github.com/ytsaurus/ytsaurus-ui/commit/36a2d128fd36b9cdcc6e347c20166a3816ea59f8))
* **Queries:** inline suggestions [YTFRONT-4612] ([11d4b59](https://github.com/ytsaurus/ytsaurus-ui/commit/11d4b596c051310671a8bc805a55612af59e8f49))
* **Navigation/ContentViewer:** add ability to view chaos_cells [YTFRONT-3653] ([299de09](https://github.com/ytsaurus/ytsaurus-ui/commit/299de0962a3d2c4e2c00b110d39fa6d267331f6b))
* **Queries:** chart kit [YTFRONT-4506] ([5ad677d](https://github.com/ytsaurus/ytsaurus-ui/commit/5ad677df47f4f878268cf450ceeda1906d4c226e))


#### Bug Fixes

* **Navigation:** xlsx types checkbox off by default [YTFRONT-4699] ([233e945](https://github.com/ytsaurus/ytsaurus-ui/commit/233e945fdd820c2906e7ea3868414dc50e9b5893))
* prevent page hotkeys firing with opened YTDialog [[#768](https://github.com/ytsaurus/ytsaurus-ui/issues/768)] ([1e6d097](https://github.com/ytsaurus/ytsaurus-ui/commit/1e6d097bb1074c8a0b07c4e4a0679afe80380d53))
* **ExperimentalPages:** wait for allowedExperimentalPages before startPage redirect ([527ad72](https://github.com/ytsaurus/ytsaurus-ui/commit/527ad7269cb14a34009ad82408ebec41785b06e1))
* **CellPreview:** show preview button when yql v3 types disabled [[#928](https://github.com/ytsaurus/ytsaurus-ui/issues/928)] ([0e6fd69](https://github.com/ytsaurus/ytsaurus-ui/commit/0e6fd690a44a79eb4245017a7defc3950cb2e27c))
* **CopyObjectModal:** change checkbox text [YTFRONT-3041] ([ce6a02f](https://github.com/ytsaurus/ytsaurus-ui/commit/ce6a02fd6383371ccc51c844454de0fed605e3a2))
* **Users/Groups:** simpilfy CommaSeparatedListWithRestCounter component, now user always will be able to see all group members and all groups of specific user [[#704](https://github.com/ytsaurus/ytsaurus-ui/issues/704)] ([453d8d7](https://github.com/ytsaurus/ytsaurus-ui/commit/453d8d7f00fad7d2cf054a3b85fb43687c68e002))
* **ACL:** minor fixes for SubjectsControl [YTFRONT-4465] ([92f521b](https://github.com/ytsaurus/ytsaurus-ui/commit/92f521b773df34bff1c8fcf292a2232df4d61669))
* **ACL:** ui should display an error when user has no permission to set acl [[#938](https://github.com/ytsaurus/ytsaurus-ui/issues/938)] ([93722c7](https://github.com/ytsaurus/ytsaurus-ui/commit/93722c7fc76d344ce9e1f710316d8302c6ebe7fe))
* **ManageTokens:** manage tokens does not work for http [[#953](https://github.com/ytsaurus/ytsaurus-ui/issues/953)] ([03fdc67](https://github.com/ytsaurus/ytsaurus-ui/commit/03fdc67ac0ad31bb52cae1332d1b7af469f5f241))
* **Navigation:** check file existence by name [YTFRONT-4638] ([2edc61b](https://github.com/ytsaurus/ytsaurus-ui/commit/2edc61b2634e6df8dd529848e34fcee80e5e729a))
* now the attributes editor always using ordered merge, that prevents table rows from shuffle ([8097144](https://github.com/ytsaurus/ytsaurus-ui/commit/809714489139becd16ad1ebf75806d50054eae9e))
* **Queries:** graph nodes clickable again [YTFRONT-4682] ([ddb125c](https://github.com/ytsaurus/ytsaurus-ui/commit/ddb125cdfeff24fc34447bfd6e83087a8974e0dd))
* **Components/Node/MemoryPopup:** do not display lines with '0B' [YTFRONT-4625] ([00268e3](https://github.com/ytsaurus/ytsaurus-ui/commit/00268e33e40d486fd05eca3277867b1a4beec232))
* **Components/Node/SloutResources:** fix calculation of 'Slot Resources' [YTFRONT-4631] ([4c898c0](https://github.com/ytsaurus/ytsaurus-ui/commit/4c898c0706fea5872810d41384282aac67fe5ce8))
* **Jobs:** fixed filter by job state, now it's correctrly applying from URL [[#775](https://github.com/ytsaurus/ytsaurus-ui/issues/775)] ([e4f96b7](https://github.com/ytsaurus/ytsaurus-ui/commit/e4f96b749fd22bbb7824196e38221637f6b300a3))
* **MultipleActions:** filter empty sections [YTFRONT-4627] ([8986360](https://github.com/ytsaurus/ytsaurus-ui/commit/8986360756b9755a03f8dbefa503f402b6cc9668))
* **Navigation/PathEditor:** use last fragment for suggestion [YTFRONT-4032] ([f451603](https://github.com/ytsaurus/ytsaurus-ui/commit/f451603614e4f659b7eb75c12df5bf5bcbf5ae68))
* **Navigation/Tabs:** optimize attributes requests [YTFRONT-3182] ([ef9100c](https://github.com/ytsaurus/ytsaurus-ui/commit/ef9100cb0e751abb88821511c16be7f4e711f312))
* **Operaion/Jobs:** get rid of unnecessary filter 'DataSource' [YTFRONT-4629] ([77c2ace](https://github.com/ytsaurus/ytsaurus-ui/commit/77c2ace10ce2653adc81b6d8847d1cc68a44d574))
* **Operation/Events:** minor style-fix for progress [YTFRONT-4631] ([c3b1225](https://github.com/ytsaurus/ytsaurus-ui/commit/c3b1225974efe18c586ee74ae23853696003afa5))
* **OperationDetail/Tasks:** minor fix for Aborted column [YTFRONT-4632] ([73f32c9](https://github.com/ytsaurus/ytsaurus-ui/commit/73f32c95a024477991f6b16e8056c73068ff0557))
* **Scheduling:** replace deprecated attribute [YTFRONT-4652] ([71d0534](https://github.com/ytsaurus/ytsaurus-ui/commit/71d05349bac045ee4850bb5b3ba631db3e9f0f7b))
* **System:** minor css-fix for '[nonvoting]' [YTFRONT-4477] ([ec76ecc](https://github.com/ytsaurus/ytsaurus-ui/commit/ec76ecceca83d8abadc0bf2a9256a41ced466977))
* **DeleteObjectModal:** show different texts in case when user trying to remove objects permanently [[#937](https://github.com/ytsaurus/ytsaurus-ui/issues/937)] ([e067dba](https://github.com/ytsaurus/ytsaurus-ui/commit/e067dba80c45f8cac265694cbb31132f0f7ea7f9))
* **Navigation:** add missing attribute for flow tab [YTFRONT-4665] ([cb2d40b](https://github.com/ytsaurus/ytsaurus-ui/commit/cb2d40b6168139342f5fa623febf87465e70a656))
* **ClusterMenu:** fix for Settings/Queries ([2de6bd7](https://github.com/ytsaurus/ytsaurus-ui/commit/2de6bd78b1fdafaf559c1426dad35a88dc329b48))
* **Navigation/MapNode:** filter parameter doesn't work from url [YTFRONT-4481] ([b4d9ab2](https://github.com/ytsaurus/ytsaurus-ui/commit/b4d9ab29be683eb525942737c210c62a55674776))
* **Operation/JobsMonitor:** better condition of visibility [YTFRONT-4600] ([12184fb](https://github.com/ytsaurus/ytsaurus-ui/commit/12184fb3aac672751c2c4bdfb9aee2d49389e92f))
* **System/Nodes:** do not use banned=disabled for Rack link [YTFRONT-4603] ([565684b](https://github.com/ytsaurus/ytsaurus-ui/commit/565684b62a53d3a2f46d0a585a11c5667b19ed83))

{% endcut %}


{% cut "**1.75.1**" %}

**Release date:** 2024-12-18


#### [1.75.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.75.0...ui-v1.75.1) (2024-12-18)

#### Features

* **Attributes:** add download button [YTFRONT-4310] ([6710c0b](https://github.com/ytsaurus/ytsaurus-ui/commit/6710c0b67ff012f3ebec4de3108c25827177f8a5))
* **CellPreviewModal:** add support image preview [[#773](https://github.com/ytsaurus/ytsaurus-ui/issues/773)] ([7b266dc](https://github.com/ytsaurus/ytsaurus-ui/commit/7b266dc2f15d2a80dff861121244e927c4b2664a))
* **Navigation/Table:** add support truncated image preview [[#773](https://github.com/ytsaurus/ytsaurus-ui/issues/773)] ([34daef1](https://github.com/ytsaurus/ytsaurus-ui/commit/34daef10fef4c22d0f546b2ec6350c0008135212))
* **QueryTracker/Table:** add support truncated image preview [[#773](https://github.com/ytsaurus/ytsaurus-ui/issues/773)] ([39684bc](https://github.com/ytsaurus/ytsaurus-ui/commit/39684bc7d68ebb914d8bdddf27fa9f08da491242))
* **VCS:** new list sort order [YTFRONT-4520] ([cb6b000](https://github.com/ytsaurus/ytsaurus-ui/commit/cb6b0008fecf69dd052dcde2ec249539ccb40a78))


#### Bug Fixes

* **Accounts:** medium filter value in url [YTFRONT-4567] ([d61ff09](https://github.com/ytsaurus/ytsaurus-ui/commit/d61ff09edd2f841816132ddbf93c98b7234829fe))
* **Accounts/Create:** use 'inherit_acl=false' only if parent is 'root' [YTFRONT-4561] ([0a59da8](https://github.com/ytsaurus/ytsaurus-ui/commit/0a59da8bcbe846c07c41a72c40e96d56202198b5))
* **ACL:** use 'keep-missing-fields' mode for 'Manage Responsibles'/'Manage Inheritance' [YTFRONT-4560] ([1e00b7a](https://github.com/ytsaurus/ytsaurus-ui/commit/1e00b7ae5846d30b4a7009c2b1e7b518306f536d))
* **ManageTokensModal:** correct time format [[#914](https://github.com/ytsaurus/ytsaurus-ui/issues/914)] ([565b205](https://github.com/ytsaurus/ytsaurus-ui/commit/565b2050959b9b2687385bc6d753d5acf2aebb14))* **OAuth:** fix redirect to previous page instead of / ([77e3471](https://github.com/ytsaurus/ytsaurus-ui/commit/77e347147f95b601e3dd3a691f4e3f8077f79f88))
* **Scheduling/Overview:** add bottom padding [YTFRONT-4530] ([43837e8](https://github.com/ytsaurus/ytsaurus-ui/commit/43837e8a8dc395cb02ba512bfe0cc0faa75ece44))
* **Toaster:** line break in toaster content [YTFRONT-4543] ([eb8faba](https://github.com/ytsaurus/ytsaurus-ui/commit/eb8fabaa788f6e78c6921c7bbd4a19280d6f3cc5))

{% endcut %}


{% cut "**1.74.0**" %}

**Release date:** 2024-12-09


#### [1.74.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.73.0...ui-v1.74.0) (2024-12-09)


#### Features

* **Navigation/CellPreview:** add dynamic table cell preview [[#776](https://github.com/ytsaurus/ytsaurus-ui/issues/776)] ([4fbbb06](https://github.com/ytsaurus/ytsaurus-ui/commit/4fbbb06dcc428e1191f89d33367f15877539a7c2))
* **Queries:** alias and column autocomplete [YTFRONT-4486] ([b47d3fb](https://github.com/ytsaurus/ytsaurus-ui/commit/b47d3fb1fc215777c0b594dd7e7643c897e830ba))
* **Queries:** change engine order [YTFRONT-4498] ([9a8dce8](https://github.com/ytsaurus/ytsaurus-ui/commit/9a8dce8470268a076756c3d8692dd7f5e66ba7d6))
* **Queries:** change query result tabs order [YTFRONT-4381] ([d851ddf](https://github.com/ytsaurus/ytsaurus-ui/commit/d851ddfac14f6de1e55ee3c4192fcff6ddb92388))
* **Groups:** now we can create new group via UI [[#634](https://github.com/ytsaurus/ytsaurus-ui/issues/634)] ([99766cf](https://github.com/ytsaurus/ytsaurus-ui/commit/99766cf33bae8153e1c238899513664c0fb6f12c))
* **ACL:** separate dialog for 'Edit inheritance' [YTFRONT-3836] ([83d5965](https://github.com/ytsaurus/ytsaurus-ui/commit/83d5965d2e3cff0d77c38765604c669f2d9a6f63))
* **Bundles/Bundle:** add 'TabletErrors' tab for bundle page [YTFRONT-4119] ([7ec446a](https://github.com/ytsaurus/ytsaurus-ui/commit/7ec446a64561c842cb7911b9318d9b1b7b16f270))
* **Navigation/DynTable:** fetch tablet errors from tabletErrosApi [YTFRONT-4119] ([b082a3d](https://github.com/ytsaurus/ytsaurus-ui/commit/b082a3d830d0ef6da12b53ea271c64605738ccde))
* **Scheduling:** hide all alerts [YTFRONT-4322] ([63d60fa](https://github.com/ytsaurus/ytsaurus-ui/commit/63d60faeb702c451029ee987caf66120d1181255))
* **logout:** make oauth logout optional [[#488](https://github.com/ytsaurus/ytsaurus-ui/issues/488)] ([1b5591a](https://github.com/ytsaurus/ytsaurus-ui/commit/1b5591a6d0a7dda2654a23cc65e439dd0e7e857b))
* **Queries:** monaco line number in url [YTFRONT-4505] ([2433ed6](https://github.com/ytsaurus/ytsaurus-ui/commit/2433ed6235a9f61833df32c70b0f0b3d711db29c))


#### Bug Fixes

* **Navigation:** additional to a62d64acbc23a7eff7d4cfb4406fea6b6a1a3887 [YTFRONT-4511] ([b17545f](https://github.com/ytsaurus/ytsaurus-ui/commit/b17545f2735efac23f1f80bac42e777f2d9e6209))
* **Navigation:** keyboard navigation [YTFRONT-4493] ([3b9fb24](https://github.com/ytsaurus/ytsaurus-ui/commit/3b9fb24c2b3aae09177e10827e99213b45b9a978))
* **Operations:** job tooltip value format [YTFRONT-4211] ([de238b1](https://github.com/ytsaurus/ytsaurus-ui/commit/de238b19e4ce1ce66666c3c5dddb4e05693174b4))
* **System:** timestamp providers maintenance [YTFRONT-4452] ([fa837cd](https://github.com/ytsaurus/ytsaurus-ui/commit/fa837cd575aea8d30f6c6a434bbeffa65c16868c))
* **UI:** celebration theme fix ([67db4f4](https://github.com/ytsaurus/ytsaurus-ui/commit/67db4f41aaa7e84449a88cdeff96f78361b0c88b))
* **Components:** use search of substring for 'Filter hosts' ([ac2c477](https://github.com/ytsaurus/ytsaurus-ui/commit/ac2c477abb47c86ba098264c8e7132b40e547422))
* **Docs:** fix incorrect documentation urls ([9c4a8db](https://github.com/ytsaurus/ytsaurus-ui/commit/9c4a8db47461f14881d128b7887630331cbfbd6f))
* **System:** fixed typo, now we check correct "maintenance" attribute ([c97f9be](https://github.com/ytsaurus/ytsaurus-ui/commit/c97f9be7cb8db3a8dc1efaeb82252755ad4d5367))
* **ACL:** better names for buttons [YTFRONT-3836] ([a45be15](https://github.com/ytsaurus/ytsaurus-ui/commit/a45be15d96f4c2c287cda7e28cfb9ab04eac0aa9))
* **ACL:** inheritAcl/inheritResponsible should be properly checked [YTFRONT-4492] ([dca379f](https://github.com/ytsaurus/ytsaurus-ui/commit/dca379f66f892e74ea8a291aa8074e38449cbba7))
* **Navigation/ReplicatedTable:** attribute /[@tablet](https://github.com/tablet)_error_count should affect tablet errors count [YTFRONT-4447] ([5860748](https://github.com/ytsaurus/ytsaurus-ui/commit/58607481c9e642774d26b285b02c8f8092982be3))
* **Navigation:** allow 'Tablet errors' for nodes with `/@tablet_error_count >= 0` [YTFRONT-3951] ([637def7](https://github.com/ytsaurus/ytsaurus-ui/commit/637def78c2c448e0dfd4ce363222a8ba7fbef5a4))
* **Groups:** expand group tree on non-empty group filter [[#853](https://github.com/ytsaurus/ytsaurus-ui/issues/853)] ([1255e14](https://github.com/ytsaurus/ytsaurus-ui/commit/1255e1401e68199eda89c8106061eaec8a067061))
* **Navigation:** bring back 'Request permissions' button [YTFRONT-4511] ([a62d64a](https://github.com/ytsaurus/ytsaurus-ui/commit/a62d64acbc23a7eff7d4cfb4406fea6b6a1a3887))
* **Operations/Details/Specifiction:** do not display empty command [YTFRONT-4507] ([cadc5fc](https://github.com/ytsaurus/ytsaurus-ui/commit/cadc5fc9472240a1cb526aa107f777c8774c6bad))
* **Queries:** share button in safari [YTFRONT-4503] ([38842f4](https://github.com/ytsaurus/ytsaurus-ui/commit/38842f49be3ea945a91dc2f463f8448c5f92173e))
* **Scheduling/ACL:** error when switch to another pool from ACL tab [YTFRONT-4487] ([06809d8](https://github.com/ytsaurus/ytsaurus-ui/commit/06809d8cb03ea36b6214670c190d526bed2c0ebd))
* **Settigns:** export DiscribedSettings type [YTFRONT-4499] ([5ab93eb](https://github.com/ytsaurus/ytsaurus-ui/commit/5ab93ebc59c15f9257c39804effa7134181c3c9a))

{% endcut %}


{% cut "**1.68.1**" %}

**Release date:** 2024-11-18


#### [1.68.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.68.0...ui-v1.68.1) (2024-11-18)


#### Features

* **PoolTreeSuggestControl:** now we could select multiply pool trees on clique creation/edit [[#841](https://github.com/ytsaurus/ytsaurus-ui/issues/841)] ([89c79a8](https://github.com/ytsaurus/ytsaurus-ui/commit/89c79a8733128c8ce0214bbe0857b583bce1d449))
* **Queries:** change path auto suggest weight [YTFRONT-4479] ([9504939](https://github.com/ytsaurus/ytsaurus-ui/commit/95049392606b78cae579dc9e13734cb5b65f46fd))
* **Queries:** allow users to copy query id ([f6acc89](https://github.com/ytsaurus/ytsaurus-ui/commit/f6acc897caed62d6a160e657d2e605d8b0182d5a))
* **Queries:** info nodes in query error [YTFRONT-4342] ([c2f4f3f](https://github.com/ytsaurus/ytsaurus-ui/commit/c2f4f3febff66c4330e3db4b919e7896a244b55f))
* **System:** headers of sections should be sticky [YTFRONT-4420] ([7e04dae](https://github.com/ytsaurus/ytsaurus-ui/commit/7e04dae01262c2b4cf0c977acd269ef6593ae1e6))


#### Bug Fixes

* **Breadcrumbs:** breadcrumb popup links not working [YTFRONT-4121] ([bad5795](https://github.com/ytsaurus/ytsaurus-ui/commit/bad579593e391bfb73ec0f45e2b99c8e26659063))
* **Components/Nodes:** fixes for add_maintenance/remove_maintenance [YTFRONT-4480] ([85ef77d](https://github.com/ytsaurus/ytsaurus-ui/commit/85ef77dfbb540a0c253e754c2cefaa0db884a191))
* **Navigation/DeleteModal:** make header of deleting items list and  "permanently delete" checkbox is sticky [YTFRONT-4245] ([3da8d44](https://github.com/ytsaurus/ytsaurus-ui/commit/3da8d4436b3f1fc93e7d764e5e67bfb8dc2524fc))
* **Navigation/ReplicatedTable:** rename column [YTFRONT-4327] ([79f664b](https://github.com/ytsaurus/ytsaurus-ui/commit/79f664b663de172c8711e3aa255c4b73729514d1))
* **Navigation:** correct data format in errors [YTFRONT-4251] ([ed6096e](https://github.com/ytsaurus/ytsaurus-ui/commit/ed6096e7f1ce337546a0271506db6f5b7f9798a6))
* **Queries:** run query hotkeys [YTFRONT-4462] ([7d69305](https://github.com/ytsaurus/ytsaurus-ui/commit/7d6930558cbdd214044e07b16f7f8a63c748efac))


{% endcut %}


{% cut "**1.66.0**" %}

**Release date:** 2024-11-01


#### [1.66.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.65.0...ui-v1.66.0) (2024-11-01)


#### Features

* **Users:** introduce "Change password" tab in UsersPageEditor, now we can change user passwords via UI [[#633](https://github.com/ytsaurus/ytsaurus-ui/issues/633)] ([2a06c23](https://github.com/ytsaurus/ytsaurus-ui/commit/2a06c237ce10a94d7970a2b09462fdc31aa9a352))
* **Users:** introduce "create new" button which allows to create new user [[#633](https://github.com/ytsaurus/ytsaurus-ui/issues/633)] ([543dcf0](https://github.com/ytsaurus/ytsaurus-ui/commit/543dcf04764d9158cdc222fcccac71f1a3840836))
* **Users:** introduce "Name" field in UsersPageEditor dialog, now we can rename users via UI [[#633](https://github.com/ytsaurus/ytsaurus-ui/issues/633)] ([bcfaead](https://github.com/ytsaurus/ytsaurus-ui/commit/bcfaead8bb9f2e65ed78f0da3099130b3930626f))
* **Users:** introduce remove button which allows to remove user [[#633](https://github.com/ytsaurus/ytsaurus-ui/issues/633)] ([461f6d9](https://github.com/ytsaurus/ytsaurus-ui/commit/461f6d9ee5dce37d49b4e5452e1739e6b7630001))


#### Bug Fixes

* **Operation/Details:** Minified React error [#31](https://github.com/ytsaurus/ytsaurus-ui/issues/31) [YTFRONT-4417] ([6324642](https://github.com/ytsaurus/ytsaurus-ui/commit/6324642bfd1474cb9c688637d61a90e3cbd5c42b))

{% endcut %}


{% cut "**1.65.0**" %}

**Release date:** 2024-10-25


#### [1.65.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.64.0...ui-v1.65.0) (2024-10-25)


#### Features

* **Operation/Jobs:** add TaskName column in table  [[#828](https://github.com/ytsaurus/ytsaurus-ui/issues/828)] ([90c8586](https://github.com/ytsaurus/ytsaurus-ui/commit/90c8586df5bbb50ec08da012e0eab1b37dd64b36))
* **Queries:** default ACO [[#436](https://github.com/ytsaurus/ytsaurus-ui/issues/436)] ([0eba698](https://github.com/ytsaurus/ytsaurus-ui/commit/0eba6989e45b147a7ddd0ab0bfc753a2c3a68e2b))
* **System:** new cluster colors [YTFRONT-4409] ([f7cb2c0](https://github.com/ytsaurus/ytsaurus-ui/commit/f7cb2c06fa65bd6bf59f7a45cd8ef8bc7cdba8d7))
* **System:** new regexp shortname [YTFRONT-4386] ([ebe523f](https://github.com/ytsaurus/ytsaurus-ui/commit/ebe523f7a4323eb765cd41dbd0eccff1231b826c))
* **UIFactory:** introduce renderCustomPreloaderError method which allows to render custom error page ([b580749](https://github.com/ytsaurus/ytsaurus-ui/commit/b580749cb803e0bacb830b3a0fd33b9fbe2b9646))
* **UIFactory:** extract defaultUIFactory to separate file [YTFRONT-3814] ([dfc8930](https://github.com/ytsaurus/ytsaurus-ui/commit/dfc8930d7925f4a6389cd6600f1ce165f2fb2852))


#### Bug Fixes

* **ACL:** permissions should be sorted [YTFRONT-4432] ([881c08c](https://github.com/ytsaurus/ytsaurus-ui/commit/881c08c2c2883c6d0288682c12fbbf3e3dafd4d6))
* **ClusterPage:** align "Loading &lt;cluster name&gt;..." text to center of the page ([f367d5b](https://github.com/ytsaurus/ytsaurus-ui/commit/f367d5b3bc5b7b612b611a7aa0b70f00f909ebf5))
* **ColumnHeader/SortIcon:** add tooltip for sort direction (+allowUnordered) [YTFRONT-3801] ([911e457](https://github.com/ytsaurus/ytsaurus-ui/commit/911e45748fc818c50a962ef65b74c2b62b92ed96))
* **Components:** wrong tablet memory column name [YTFRONT-4408] ([21cc198](https://github.com/ytsaurus/ytsaurus-ui/commit/21cc198c475c62f2ee6041fc9a1b2c3057a39155))
* **Navigation:** correct cluster name in yql query [YTFRONT-4274] ([4b9cab5](https://github.com/ytsaurus/ytsaurus-ui/commit/4b9cab511c4ba1b65d02df8923a44cea0beba3ee))
* **Navigation/MapNode:** allow to select rows by click on first cell [YTFRONT-4391] ([85e915c](https://github.com/ytsaurus/ytsaurus-ui/commit/85e915cf2c72f4137e9b73b87b7c1748db5b5094))
* **Navigation/Queue:** allow Queue tab for replication_table/chaos_replicated_table [YTFRONT-4144] ([228db6a](https://github.com/ytsaurus/ytsaurus-ui/commit/228db6a3a4c9a57db0d36812d8fd2e412be0c901))
* **Navigation/Table:** draggable row selector should work properly [YTFRONT-4396] ([d702adb](https://github.com/ytsaurus/ytsaurus-ui/commit/d702adb6b2e50f81a4d60cd67f77308b454ee14e))
* **Navigation/TopRow/PathEditor:** select text when editor is focused [YTFRONT-4387] ([fd4beb6](https://github.com/ytsaurus/ytsaurus-ui/commit/fd4beb695e05e35d7648ee0ee430ce759f90a825))
* **Navigation/RequestPermissionsButton:** minor fix of styles [YTFRONT-4379] ([9eefa05](https://github.com/ytsaurus/ytsaurus-ui/commit/9eefa050f002a9a4dd829b32c4cf75bf4de068cb))
* **Odin:** use the same format for DatePicker in odin ([5a25c2f](https://github.com/ytsaurus/ytsaurus-ui/commit/5a25c2f9aec338b39bf49a7e9a8836516d6a0980))
* **Operations:** save pool tree in url [YTFRONT-4355] ([ee2fe0e](https://github.com/ytsaurus/ytsaurus-ui/commit/ee2fe0e5b7bc6e64c26cdc4290c7887fc554cedf))
* **PathViewer:** now path viewer run list command by default, because "get /" command might lead to perfomance issue [[#814](https://github.com/ytsaurus/ytsaurus-ui/issues/814)] ([006d215](https://github.com/ytsaurus/ytsaurus-ui/commit/006d21576975feb2d20d6919f88c62542ab4ff30))
* **System/Nodes:** minor fixes [YTFRONT-3297] ([3d78cfe](https://github.com/ytsaurus/ytsaurus-ui/commit/3d78cfe2b99cec3c779eead885d6e8cd5440f413))


{% endcut %}


{% cut "**1.60.1**" %}

**Release date:** 2024-10-02


#### [1.60.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.60.0...ui-v1.60.1) (2024-10-02)

#### Features

* **Navigation/Table/CellPreviewModal:** add support string preview [[#765](https://github.com/ytsaurus/ytsaurus-ui/issues/765)] ([e779d16](https://github.com/ytsaurus/ytsaurus-ui/commit/e779d161b928bc372be57cfaae199311c99dae1d))
* **Navigation:** open access logs in qt [YTFRONT-4345] ([97a42b8](https://github.com/ytsaurus/ytsaurus-ui/commit/97a42b87d55c78e92224f43c28bac4d9fd8932ca))
* **Navigation/MapNode:** allow override node-icon through UIFactory.getNavigationMapNodeSettings ([c97a4a0](https://github.com/ytsaurus/ytsaurus-ui/commit/c97a4a09bcd8cfc40f5e5eeec203ee1aab417948))
* **Navigation/Queue:** add alerts section [YTFRONT-4144] ([8b0157c](https://github.com/ytsaurus/ytsaurus-ui/commit/8b0157c08dac8757a04a4129c83ea8de332f6861))
* **System:** maintenance button [YTFRONT-4217] ([3c5d0d2](https://github.com/ytsaurus/ytsaurus-ui/commit/3c5d0d225d244204b87fde6dc182489130ad20e6))

#### Bug Fixes

* **BFF** fix logging of axios error in sendAndLogError function ([b9239dc](https://github.com/ytsaurus/ytsaurus-ui/commit/b9239dc43feab214b4e3520b21e662755be4f33a))
* **Navigation:** pool tree select popup [YTFRONT-4380] ([f52eb90](https://github.com/ytsaurus/ytsaurus-ui/commit/f52eb90da82306d6bf191a0d1375f3c30eaa3aac))
* **Navigation/Consumer,Navigation/Queue:** show errors [YTFRONT-4144] ([914a6a0](https://github.com/ytsaurus/ytsaurus-ui/commit/914a6a066ce30928673749bd8e2250c51a3e637b))
* **Navigation/Table/CellPreview:** fix opening preview for table with offset [[#778](https://github.com/ytsaurus/ytsaurus-ui/issues/778)] ([7347349](https://github.com/ytsaurus/ytsaurus-ui/commit/7347349c9adaedb1a8d7ea4a933a8316f2b296d2))
* **Queries:** chyt spyt path autocomplete [YTFRONT-4368] ([df3cff1](https://github.com/ytsaurus/ytsaurus-ui/commit/df3cff140ee9d7bee48c19be55cc66d00e06cdcd))
* **Queries:** do not show vcs if vcsSettings is empty ([7df0b04](https://github.com/ytsaurus/ytsaurus-ui/commit/7df0b045ecba077363d03471b8d196301d6b8a65))
* **Queries:** fix adhoc charts ([2c441c5](https://github.com/ytsaurus/ytsaurus-ui/commit/2c441c5d79cc680000038d2bfc670e247e488e08))
* **Sort,Merge:** get rid of missing node errors [YTFRONT-4392] ([cf79a79](https://github.com/ytsaurus/ytsaurus-ui/commit/cf79a79ac5366bd5e547eb18ebb2284cc5ae6234))
* **System:** fix color of stats text in dark mode ([f1c3ec3](https://github.com/ytsaurus/ytsaurus-ui/commit/f1c3ec3fc73a5bac17c0bffa2c3229a47c710ec3))
* **YQLTable:** fixed exception in truncated cells preview ([a351378](https://github.com/ytsaurus/ytsaurus-ui/commit/a35137899782ab251f9515db8d87ad92f05c23b1))

{% endcut %}


{% cut "**1.58.1**" %}

**Release date:** 2024-09-10


#### [1.58.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.58.0...ui-v1.58.1) (2024-09-10)

#### Features

* **ACL:** add inheritedFrom field [YTFRONT-3836] ([4bd121d](https://github.com/ytsaurus/ytsaurus-ui/commit/4bd121d7b61907997d0e525e73d0312a43d01a50))
* **ACL:** inherited roles should be displayed separately [YTFRONT-3836] ([7edb2c4](https://github.com/ytsaurus/ytsaurus-ui/commit/7edb2c42293088b865c6205fa3085929f082d10f))
* **ACL:** use @idm_roles for ACO (+tvm name) [YTFRONT-3836] ([03f139e](https://github.com/ytsaurus/ytsaurus-ui/commit/03f139ed5c38c2e211bafddab8b5bb4e3805c918))
* **Components/Nodes:** add gpu progress [YTFRONT-4306] ([7ec1f62](https://github.com/ytsaurus/ytsaurus-ui/commit/7ec1f62eb996fdf10cc0d78a375ed07fd33f9a35))
* **Components:** show all tags [YTFRONT-4315] ([723e772](https://github.com/ytsaurus/ytsaurus-ui/commit/723e77216750e3608b284b7953d43cbdee65c0d3))
* **Markdown:** use @diplodoc/transform  [YTFRONT-4108] ([3b33bc9](https://github.com/ytsaurus/ytsaurus-ui/commit/3b33bc9ef85069159b53aa12e7ca4c0eb09bf8b9))
* **Navigation/CreateTableModal:** changing the default name for new tables [YTFRONT-4249] ([cc19d6b](https://github.com/ytsaurus/ytsaurus-ui/commit/cc19d6bc71607751a5d019dfc6ce8fa1261a6e1c))
* **Navigation/Flow:** add new tab [YTFRONT-3978] ([1ef39d7](https://github.com/ytsaurus/ytsaurus-ui/commit/1ef39d7cee11e4e6a6f3eb4bfb044e95b4f6fc60))
* **Operations:** now we can disable filter optimization on the operations page via cluster config [[#700](https://github.com/ytsaurus/ytsaurus-ui/issues/700)] ([771294a](https://github.com/ytsaurus/ytsaurus-ui/commit/771294ab5bb33b2b11413da2c52ec8e85d175f3d))
* **Queries:** add ability do define VCS [YTFRONT-4257] ([3e0df9b](https://github.com/ytsaurus/ytsaurus-ui/commit/3e0df9b3b64bb515d748118f8a60d4f88f3274a2))
* **Queries:** introduce poc of adhoc visualization on query results [[#641](https://github.com/ytsaurus/ytsaurus-ui/issues/641)] ([6dd9896](https://github.com/ytsaurus/ytsaurus-ui/commit/6dd98968ce15cf9667619f0c710d6a3dec8c21dc))
* **Queries:** new navigation tab [YTFRONT-4235] ([428a72c](https://github.com/ytsaurus/ytsaurus-ui/commit/428a72c7163bc353a5524445e956d4ca1ff50e9e))
* **Queries:** new query ACO format [YTFRONT-4238] ([a3ba06a](https://github.com/ytsaurus/ytsaurus-ui/commit/a3ba06a2317b1f54fdd23f52d7bf5795dabc4643))
* **Queries:** spyt clicue selector [YTFRONT-4219] ([6288c73](https://github.com/ytsaurus/ytsaurus-ui/commit/6288c73e4a1919312aae55040ce2baab331e1875))
* **Queries:** vcs navigation [YTFRONT-4147] ([58be722](https://github.com/ytsaurus/ytsaurus-ui/commit/58be72232945ef8bcbf17327e3041a5c263256af))
* **Queries:** share query button [YTFRONT-4239] ([67e84bc](https://github.com/ytsaurus/ytsaurus-ui/commit/67e84bc383ebce81a57928d44d384a5ed7ab0d99))
* **Table:** add "View" button for truncated cells [[#655](https://github.com/ytsaurus/ytsaurus-ui/issues/655)] ([c688f1f](https://github.com/ytsaurus/ytsaurus-ui/commit/c688f1f6e4b674c1cb79bafc523ded16948e0516))
* **Table/Excel:** allow to setup uploadTableExcelBaseUrl and exportTableBaseUrl per cluster [[#717](https://github.com/ytsaurus/ytsaurus-ui/issues/717)] ([88dec84](https://github.com/ytsaurus/ytsaurus-ui/commit/88dec846b765b5e4f9413de245aad6ca956819b9))
* **javascript-wrapper:** add new commands for pipelines [YTFRONT-3978] ([da70313](https://github.com/ytsaurus/ytsaurus-ui/commit/da70313424b8042e6782d8fe9a642c9703465d54))
* **uikit6:** update dependencies [[#502](https://github.com/ytsaurus/ytsaurus-ui/issues/502)] ([5a92c5f](https://github.com/ytsaurus/ytsaurus-ui/commit/5a92c5fbbfccf43a788946b3ab9e95ebca0e74bf))
* **YQLTable:** add "view" button for truncated cells [[#702](https://github.com/ytsaurus/ytsaurus-ui/issues/702)] ([ee776c1](https://github.com/ytsaurus/ytsaurus-ui/commit/ee776c1158eaaf21a53ae6226dbd5ba83427c646))
* udpate @gravity-ui/charkit, @gravity-ui/yagr [YTFRONT-4305] ([a65be74](https://github.com/ytsaurus/ytsaurus-ui/commit/a65be74a5d9017a5c0d8159f80982891e3afc8cc))

#### Bug Fixes

* **Components/Nodes:** fix filtering by racks ([b58e8ba](https://github.com/ytsaurus/ytsaurus-ui/commit/b58e8ba8de094e635d2d697589d68e2418b5660b))
* **Components/Node:** show decimal cpus [[#675](https://github.com/ytsaurus/ytsaurus-ui/issues/675)] ([b42b0bb](https://github.com/ytsaurus/ytsaurus-ui/commit/b42b0bb147cc13abe05715e6a0fb453724d2ec50))
* **Navigation/ReplicatedTable:** add info icon for 'Automatic mode switch' [YTFRONT-4327] ([5446fc3](https://github.com/ytsaurus/ytsaurus-ui/commit/5446fc381fd17695cdbd22b280c53d0deb5e8a86))
* **Navigation/Table:** use POST-requests to read tables [YTFRONT-4259] ([7281e79](https://github.com/ytsaurus/ytsaurus-ui/commit/7281e79d41f3db5dabe056378b7c276412a4ed5a))
* **Operations/Operation/JobsMonitor:** use 'with_monitoring_descriptor' flag [YTFRONT-4346] ([bbf5415](https://github.com/ytsaurus/ytsaurus-ui/commit/bbf54154a389895f64eb7e6c04dbdc15aee30e40))
* **System/Masters:** minor fix for layout with alerts [YTFRONT-4295] ([2134144](https://github.com/ytsaurus/ytsaurus-ui/commit/2134144fb7829d6eb2010d65103376019a932036))
**ClustersMenu:** the page should not be broken with '[' filter [YTFRONT-4272] ([7eb5c7c](https://github.com/ytsaurus/ytsaurus-ui/commit/7eb5c7cfedc28935034041a82ef989ff44e4c460))
* **Components/Nodes:** get rid of duplicates of nodes [YTFRONT-4268] ([131c857](https://github.com/ytsaurus/ytsaurus-ui/commit/131c8574b6cc1e076bb8076437ea542a7c149415))
* **Components/Nodes:** fix alerts filter [YTFRONT-4301] ([861f57c](https://github.com/ytsaurus/ytsaurus-ui/commit/861f57c5c213c0c22e6df8f46f16e2d0c3d2a188))
* **Componens/Nodes/Node:** fix for width of memory popup [[#502](https://github.com/ytsaurus/ytsaurus-ui/issues/502)] ([fc9c882](https://github.com/ytsaurus/ytsaurus-ui/commit/fc9c882e1177cce8f5f007c6a8a0d187724ffb1d))
* **MaintenancePage:** rework maintenance activation ([c7ed6e4](https://github.com/ytsaurus/ytsaurus-ui/commit/c7ed6e4702add1a9e62dae2e278104f3be01c007))
* **Navigation:** correct output of numbers in tablet errors [YTFRONT-4251] ([fe02b58](https://github.com/ytsaurus/ytsaurus-ui/commit/fe02b5879e092fc1f4d674b7594b6eb8eb3d10fd))
* **Navigation:** do not reset contentMode on navigation [[#511](https://github.com/ytsaurus/ytsaurus-ui/issues/511)] ([916ede3](https://github.com/ytsaurus/ytsaurus-ui/commit/916ede342fac224d6c077704be929999ab326863))
* **Navigation:** now breadcrumbs dropdown items are clickable [[#528](https://github.com/ytsaurus/ytsaurus-ui/issues/528)] ([2df7319](https://github.com/ytsaurus/ytsaurus-ui/commit/2df73197d827d0912ef8203f0357f1dfda681ecd))
* **Navigation/Favourites:** allow to add items when the value is undefined ([69c9202](https://github.com/ytsaurus/ytsaurus-ui/commit/69c920222ce1f3a0381efc70aa65da190f04b0e0))
* **Navigation/File:** allow remote-copy for 'file' node type [YTFRONT-4296] ([aff83de](https://github.com/ytsaurus/ytsaurus-ui/commit/aff83def8abee98eb1937b9495f828d553fd4d16))
* **Navigation/MapNode:** minor fix for css [YTFRONT-4291] ([e5932f8](https://github.com/ytsaurus/ytsaurus-ui/commit/e5932f817c8ceacdca713ad532814d0b8f7b3f39))
* **Navigation/RemoteCopy:** fix for disabled 'Confirm' button [YTFRONT-4296] ([a6b7bde](https://github.com/ytsaurus/ytsaurus-ui/commit/a6b7bdebb3db6639174761fe4551066276baaed0))
* **Navigation/Table:** do not throw `undefined` as an exception [YTFRONT-4312] ([d3e94cd](https://github.com/ytsaurus/ytsaurus-ui/commit/d3e94cd2893970a2982bf3943da0dfd181f649f8))
* **Odin:** now non-standalone odin page recieve correct cluster ([b4739a4](https://github.com/ytsaurus/ytsaurus-ui/commit/b4739a4503028aebcfefc0f8ae42fe3b29e26b50))
* **Operations/Details/MetaTable:** blinking UI when hover on long pool name [YTFRONT-4308] ([395c849](https://github.com/ytsaurus/ytsaurus-ui/commit/395c8496e6770c9da1263c9b9e6589bfd157e991))
* **Operation/Job/Statistics:** handle undefined [YTFRONT-4300] ([ca0da3c](https://github.com/ytsaurus/ytsaurus-ui/commit/ca0da3c32e34403491a9c68fc78e769ac06989ce))
* **Operation/Specification/Input:** fix for 'remote_copy' operations [YTFRONT-4265] ([502bd53](https://github.com/ytsaurus/ytsaurus-ui/commit/502bd539edacac20c4f9e6caf7e4360488440cd1))
* **OperationPool:** minor fix for css ([9f9b32d](https://github.com/ytsaurus/ytsaurus-ui/commit/9f9b32d74c09780b207c6db1c52f327ed3549be1))
* **OperationsList:** fix incorrect filters on OperationSuggestFilter blur [[#705](https://github.com/ytsaurus/ytsaurus-ui/issues/705)] ([a738c5b](https://github.com/ytsaurus/ytsaurus-ui/commit/a738c5b182a192cb0e5c847ab31e90675da1c144))
* **Queries:** fix error when switching to another query with open statistics tab ([ef61008](https://github.com/ytsaurus/ytsaurus-ui/commit/ef61008be538119dfc8754b20c06105dbf8058ff))
ui/commit/b39aa3e873f44dd45da2c7bf8005ccb93294a40e))
* **Queries:** multiple aco with backward compatible [YTFRONT-4238] ([7efe878](https://github.com/ytsaurus/ytsaurus-ui/commit/7efe87881d5fd69d8a86252c0d401f8c950bb7a2))
* **Queries:** redirect to yt operations from running yql queries [[#522](https://github.com/ytsaurus/ytsaurus-ui/issues/522)] ([2a91613](https://github.com/ytsaurus/ytsaurus-ui/commit/2a916136fee38055f3e85ad1325829dd68e2fcd0))
* **Queries:** support dark theme in statistic table ([b3f1d57](https://github.com/ytsaurus/ytsaurus-ui/commit/b3f1d5766f880605c6aa975fc515a8cca568a933))
* **Queries:** use treatValAsData option of `@gravity-ui/unipika` by default ([2009d96](https://github.com/ytsaurus/ytsaurus-ui/commit/2009d96f9b822dbbd3e043628faa681731c4cd77))
* **Queries:** share button new design [YTFRONT-4286] ([7d66e6c](https://github.com/ytsaurus/ytsaurus-ui/commit/7d66e6c56bc9f9489bb0ee89a2de3791dfd22103))
* **Queries/Editor:** path autocomplete [YTFRONT-4264] ([ab9ba1f](https://github.com/ytsaurus/ytsaurus-ui/commit/ab9ba1f4497c70649a8e92df9666c2cf2ff9ed24))
* **Queries/Result:** fix for empty blocks [YTFRONT-4323] ([b39aa3e](https://github.com/ytsaurus/ytsaurus-))
* **Scheduling/CreatePool:** do not use validation for pool name [YTFRONT-4319] ([7bd7852](https://github.com/ytsaurus/ytsaurus-ui/commit/7bd7852ab57bd084b6fe9f24086272a4fd3b7aa9))
* **System/Nodes:** allow to expand groups of nodes [YTFRONT-3297] ([7c8330e](https://github.com/ytsaurus/ytsaurus-ui/commit/7c8330e49ead3b01ae7bd962a9887e05940ec10d))
* **Table/CellPreviewModal:** fix control to be sticky when scroll [[#703](https://github.com/ytsaurus/ytsaurus-ui/issues/703)] ([c5e91cb](https://github.com/ytsaurus/ytsaurus-ui/commit/c5e91cbee5d122c32784145dbc2d49e76a5ab434))
* **Tablet:** fix for node url [YTFRONT-4269] ([82de290](https://github.com/ytsaurus/ytsaurus-ui/commit/82de2908b58d2f18f583c6869db6de601064725c))
* **uiSettings:** now we can specify loginPageSettings per cluster ([be872cc](https://github.com/ytsaurus/ytsaurus-ui/commit/be872ccd9a52d5ad3d04f694602713f1b971759b))
* **YFM/Markdown**: add support for dark and light themes by override yfm styles [[#712](https://github.com/ytsaurus/ytsaurus-ui/issues/712)] ([b7cce12](https://github.com/ytsaurus/ytsaurus-ui/commit/b7cce12fcbf2612d69b528932fc39b160f8bb464))
* **Navigation/MapNode:** use 'navmode=auto' when node is clicked ([3d473c4](https://github.com/ytsaurus/ytsaurus-ui/commit/3d473c426daea116c610860e7c15141f963e381f))
* **Queries:** queries page fixes [YTFRONT-4340] ([676354e](https://github.com/ytsaurus/ytsaurus-ui/commit/676354e75f8b2d8457ceda002f2943b167cd0192))
* **Navigation/File:** allow remote-copy for 'file' node type [YTFRONT-4296] ([459cef0](https://github.com/ytsaurus/ytsaurus-ui/commit/459cef089c994511aa2d4117acf3bab3e2cd39d8))
* **Navigation/RemoteCopy:** fix for disabled 'Confirm' button [YTFRONT-4296] ([3ce933a](https://github.com/ytsaurus/ytsaurus-ui/commit/3ce933a38678371fc3675fefa4a0bda71f67e481))

{% endcut %}


{% cut "**1.46.2**" %}

**Release date:** 2024-07-21


#### [1.46.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.46.1...ui-v1.46.2) (2024-07-21)


#### Bug Fixes

* **Operation/Specification/Input:** fix for 'remote_copy' operations [YTFRONT-4265] ([b9ba7d9](https://github.com/ytsaurus/ytsaurus-ui/commit/b9ba7d901f38b49d75c10c072339dce9072d9f0e))

{% endcut %}


{% cut "**1.46.0**" %}

**Release date:** 2024-07-02


#### [1.46.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.45.0...ui-v1.46.0) (2024-07-02)


#### Features

* **Components:** alert for offline node [YTFRONT-4153] ([e95801e](https://github.com/ytsaurus/ytsaurus-ui/commit/e95801ee4abef7b6e47d8a6f5c96e6b8dcd87cbb))
* **Login**: added ability to override default text on login page [[#636](https://github.com/ytsaurus/ytsaurus-ui/issues/636)] ([28a47ec](https://github.com/ytsaurus/ytsaurus-ui/commit/28a47ec5ccf66d085e886391ab6fa3ff15b7372d))
* **Navigation/Table**: add support of new types `date32`/`datetime64`/`timestamp64`/`interval64` [YTFRONT-4087] ([6f2c8e5](https://github.com/ytsaurus/ytsaurus-ui/commit/6f2c8e51c23f0099d1715cf21156fd36f43a34e4))
* **ManageTokens:** allow user to issue and manage tokens from ui [[#241](https://github.com/ytsaurus/ytsaurus-ui/issues/241)] ([6bdd6d2](https://github.com/ytsaurus/ytsaurus-ui/commit/6bdd6d2d6ae767a90a2c72f629325b0d6c56db3a))
* **Job:** add 'Job trace' meta-table item [YTFRONT-4182] ([00c0691](https://github.com/ytsaurus/ytsaurus-ui/commit/00c06919c5a31ea45068c9dbfe3f3ce5e0bbef3b))
* **Query:** title with query data [YTFRONT-4186] ([5282fb7](https://github.com/ytsaurus/ytsaurus-ui/commit/5282fb77dc5038bff78d55d25f146882c04adfda))
* **QueryTracker:** add search field to statistics tab in query tracker [[#301](https://github.com/ytsaurus/ytsaurus-ui/issues/301)] ([551e66a](https://github.com/ytsaurus/ytsaurus-ui/commit/551e66aaa127c0db31abefa92b41782a598a4899))
* **UIFactory:** add UIFactory.getNavigationExtraTabs() method ([bddf57c](https://github.com/ytsaurus/ytsaurus-ui/commit/bddf57cf45c52a0ab1002df5827ef49698c7644f))
* **UIFactory:** add UIFactory.getMapNodeExtraCreateActions(...) method ([ae6ae51](https://github.com/ytsaurus/ytsaurus-ui/commit/ae6ae5187e29787d09a26144215736ade9b8d1f4))
* **UIFactory:** add UIFactory.renderAppFooter() method [YTFRONT-4173] ([616ff0b](https://github.com/ytsaurus/ytsaurus-ui/commit/616ff0b2d8786df0eb1d05f7f45169984cb20162))

#### Bug Fixes

* **AccountsGeneralTab:** do not show TabletAccountingNotice if enable_per_account_tablet_accounting is enabled ([7de2eb5](https://github.com/ytsaurus/ytsaurus-ui/commit/7de2eb5f25e9216b9dd03e3bf2d8131397cd77e9))
* **ACL:** request for ACL for ACO [[#576](https://github.com/ytsaurus/ytsaurus-ui/issues/576)] ([0f46beb](https://github.com/ytsaurus/ytsaurus-ui/commit/0f46beb68fa523ada3200123769ac95927d0b3ff))
* **Auth:** use names of cookies with a colon [[#587](https://github.com/ytsaurus/ytsaurus-ui/issues/587)] ([79a4254](https://github.com/ytsaurus/ytsaurus-ui/commit/79a42545c1a2684fb10aaa20e754c9fa60a9ae14))
* **ClusterPage:** footer problem in cluster page [YTFRONT-4173] ([8800bbc](https://github.com/ytsaurus/ytsaurus-ui/commit/8800bbc205ffe2bc0211fb4c1ac081967287c695))
* **Clusters:** change body flex grow [YTFRONT-4221] ([097da73](https://github.com/ytsaurus/ytsaurus-ui/commit/097da73a10fb8ab2f1392313148cb74b2ba00867))
* **DownloadManager:** fix for ranges [YTFRONT-4215] ([0f117ca](https://github.com/ytsaurus/ytsaurus-ui/commit/0f117ca913abd8a57d252b40904d1b050bd5c39e))
* **Navigation:** widget with footer problem [YTFRONT-4221] ([5b4bbe1](https://github.com/ytsaurus/ytsaurus-ui/commit/5b4bbe152f8ba3701b895b0f8b0bf21726e1bf17))
* **ManageTokens:** show null in the token list if tokenPrefix is unknown [[#626](https://github.com/ytsaurus/ytsaurus-ui/issues/626)] ([135c92e](https://github.com/ytsaurus/ytsaurus-ui/commit/135c92e4e31b7776b0e5ce6604e0d143522012ff))
* **ManageTokens:** fixed freeze of the password  window ([c2e20ab](https://github.com/ytsaurus/ytsaurus-ui/commit/c2e20ab1623d2781a45d0fa4b93781d972cebaf4))
* **ManageTokens:** horizontal scroll in the table is off ([65e0b6a](https://github.com/ytsaurus/ytsaurus-ui/commit/65e0b6acf270d42a46680e9ea5e4b110eccb8b2c))
* **query/custom-result-tab:** show tab when there is a query result ([bf64b2c](https://github.com/ytsaurus/ytsaurus-ui/commit/bf64b2cf9aef5e5d6281799e45c831eb58f910f3))
* **Query:** query and progress tab [YTFRONT-4185] ([c74c0fc](https://github.com/ytsaurus/ytsaurus-ui/commit/c74c0fced87cc839839d2128d4e8a910c813b0d2))
* **Query:** select in error line [YTFRONT-4208] ([85508dc](https://github.com/ytsaurus/ytsaurus-ui/commit/85508dcdd79758f6e1c744b7d2805e7f25056cd7))
* **Query:** utf decode in result table [[#533](https://github.com/ytsaurus/ytsaurus-ui/issues/533)] ([7cadb62](https://github.com/ytsaurus/ytsaurus-ui/commit/7cadb62ffb276edece14266393a8a9f3b0345dfe))
* **System:** nonvoting position [YTFRONT-4209] ([901da6f](https://github.com/ytsaurus/ytsaurus-ui/commit/901da6f484b16cdd4b8c79beb2e53d422be6b48c))
* **System:** now we are trying make a request to another primary masters if first one did not responded correctrly [[#529](https://github.com/ytsaurus/ytsaurus-ui/issues/529)] ([fc25ad4](https://github.com/ytsaurus/ytsaurus-ui/commit/fc25ad493adb410ae66876ca7746dd3665f6a04a))

{% endcut %}


{% cut "**1.41.1**" %}

**Release date:** 2024-05-28


#### [1.41.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.41.0...ui-v1.41.1) (2024-05-28)

#### Features

* **Components/Nodes:** use better sorting for progress columns [YTFRONT-3801] ([3502577](https://github.com/ytsaurus/ytsaurus-ui/commit/3502577afb59ee36a05d975e0e69b8647ece9d5d))
* **DownloadManager/Excel:** add 'Number precision mode' option [YTFRONT-4150] ([5a3a641](https://github.com/ytsaurus/ytsaurus-ui/commit/5a3a641a0427700df566d5b35f3a1c2581c9ff50))
* **Navigation/CreateTableModal:** add 'Optimize for' option [YTFRONT-4139] ([be84c6a](https://github.com/ytsaurus/ytsaurus-ui/commit/be84c6a288647b02775e9cbc288b865ffc11538b))
* **Operation/Jobs:** add 'with_monitoring_descriptor' filter [YTFRONT-4078] ([aa575c9](https://github.com/ytsaurus/ytsaurus-ui/commit/aa575c9e3427c92f6f935fd5b25ef88887f2a911))


#### Bug Fixes

* **AclUpdateMessage:** minor fix for layout ([ed85d7f](https://github.com/ytsaurus/ytsaurus-ui/commit/ed85d7fab663ddc1a75c614aabb6a062a635b329))
* **ACL:** minor fix for meta-block [YTFRONT-3836] ([a3859d0](https://github.com/ytsaurus/ytsaurus-ui/commit/a3859d059efff237bbd7f58b25b67f40bca8b99e))
* **Bundles:** memory limit [YTFRONT-4170] ([26491e0](https://github.com/ytsaurus/ytsaurus-ui/commit/26491e0096c0c95371a88ea0d7d13cf14cf65018))
* **Bundles:** memory limit [YTFRONT-4170] ([4be139c](https://github.com/ytsaurus/ytsaurus-ui/commit/4be139c64a7538e038443cdd8100150c1b8a00f8))
* **Operation/JobsMonitor:** tab should be displayed without delay [YTFRONT-4077] ([9673252](https://github.com/ytsaurus/ytsaurus-ui/commit/96732524d0025dff154710c1b5b814da1d01865d))
* **Scheduling/ACL:** reload acl when pool tree changed [YTFRONT-4172] ([b697bf3](https://github.com/ytsaurus/ytsaurus-ui/commit/b697bf3e40fdc97a07d3d009201ec6e5bdafef17))
* **System/Master:** bring back 'Queue agents' [YTFRONT-4145] ([1a82e8e](https://github.com/ytsaurus/ytsaurus-ui/commit/1a82e8e88e9ce3069c279a4b931866a30734629a))
* **Table/Schema:** minor css fix [YTFRONT-4166] ([6b9cca4](https://github.com/ytsaurus/ytsaurus-ui/commit/6b9cca40f314fd544f3f90a280b1db72f48404db))
* **TimelinePicker:** minor fix [YTFRONT-4180] ([20326c0](https://github.com/ytsaurus/ytsaurus-ui/commit/20326c0bb47889a0d0ffe8fd55b25b6b11681ab1))

{% endcut %}


{% cut "**1.39.0**" %}

**Release date:** 2024-05-23


#### [1.39.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.38.2...ui-v1.39.0) (2024-05-23)


#### Features

* **System:** introduce new switch leader button ([43f5034](https://github.com/ytsaurus/ytsaurus-ui/commit/43f5034405fcf58bd045406551a97f52e7a4a3ed))


#### Bug Fixes

* **axios/withXSRFToken:** additional to b7738a97c3177df02a3a9112112ac97e4afef118 ([88b5efa](https://github.com/ytsaurus/ytsaurus-ui/commit/88b5efafd5d4ec480ea50f75e15314974786f427))
* **Bundles:** memory limit [YTFRONT-4170] ([4be139c](https://github.com/ytsaurus/ytsaurus-ui/commit/4be139c64a7538e038443cdd8100150c1b8a00f8))

{% endcut %}


{% cut "**1.38.0**" %}

**Release date:** 2024-05-16


#### [1.38.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.37.0...ui-v1.38.0) (2024-05-16)

#### Features

* **Navigation:** output path attributes [YTFRONT-3869] ([8f90df0](https://github.com/ytsaurus/ytsaurus-ui/commit/8f90df06bfccfb7c7aeca8bc1dc9056a12eb5395))
* **Odin:** added timepicker on overview [YTFRONT-2733] ([f3ad6ba](https://github.com/ytsaurus/ytsaurus-ui/commit/f3ad6ba43d55e1133f8f5655a8d20c3868ad68f1))

#### Bug Fixes

* **login:** error message displayed when entering incorrect login credentials [[#490](https://github.com/ytsaurus/ytsaurus-ui/issues/490)] ([b6d7a34](https://github.com/ytsaurus/ytsaurus-ui/commit/b6d7a34c4ca0e4b1934ed75be252289b32d442df))
* **login:** user still see the login page after click to browser navigation back ([688043e](https://github.com/ytsaurus/ytsaurus-ui/commit/688043e5fe4babecbe67625fa0004b21e3e676fa))
* **TabletCellBundle/Instances:** notice for in-progress allocation [YTFRONT-4167] ([2218d61](https://github.com/ytsaurus/ytsaurus-ui/commit/2218d617dffc89a33cdab5ef4a1de908300a2545))
* **YQLTable:** fix appearance of truncated value in cell ([f5daaca](https://github.com/ytsaurus/ytsaurus-ui/commit/f5daaca691c0e94e7cfac2b712a2d392d66cfb5e))
* **login:** error message displayed when entering incorrect login credentials [[#490](https://github.com/ytsaurus/ytsaurus-ui/issues/490)] ([b6d7a34](https://github.com/ytsaurus/ytsaurus-ui/commit/b6d7a34c4ca0e4b1934ed75be252289b32d442df))
* **login:** user still see the login page after click to browser navigation back ([688043e](https://github.com/ytsaurus/ytsaurus-ui/commit/688043e5fe4babecbe67625fa0004b21e3e676fa))
* fix for release ([e8019bc](https://github.com/ytsaurus/ytsaurus-ui/commit/e8019bc26ec8a22a81eb2b5aef2683d9a39d3994))

{% endcut %}


{% cut "**1.33.0**" %}

**Release date:** 2024-05-08


#### [1.33.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.32.0...ui-v1.33.0) (2024-05-08)


#### Features

* **Navigation:** sort button in table columns [YTFRONT-4135] ([44d67a3](https://github.com/ytsaurus/ytsaurus-ui/commit/44d67a3e51a564d4b78a5c9381d8205bd313d473))


#### Bug Fixes

* **AccountsUsage:** fix for 'view'-parameter [YTFRONT-3737] ([7d31cda](https://github.com/ytsaurus/ytsaurus-ui/commit/7d31cdac26fafb4695a9893b8ad3e9e749bf9ba4))
* **AccountsUsage:** fix for dropdowns of Select [YTFRONT-4155] ([63645e1](https://github.com/ytsaurus/ytsaurus-ui/commit/63645e1dda2d73155967ed0a47e8b523c46a13fa))
* **BundleEditorDialog:** better error message [YTFRONT-4148] ([d233f9c](https://github.com/ytsaurus/ytsaurus-ui/commit/d233f9ca8b409626874b77519c5f2c72e1daa77a))

{% endcut %}


{% cut "**1.32.0**" %}

**Release date:** 2024-05-07


#### [1.32.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.31.0...ui-v1.32.0) (2024-05-07)


#### Features

* added ability to render custom query tab at query-tracker page ([193d24b](https://github.com/ytsaurus/ytsaurus-ui/commit/193d24bcf12588579b27331e3553a72fc8b17ab8))
* **Query:** added file editor [YTFRONT-3984] ([3ca0b33](https://github.com/ytsaurus/ytsaurus-ui/commit/3ca0b33b3a834b090238d71763c833338699d0f2))


#### Bug Fixes

* **Accounts:** select problem [YTADMINREQ-41653] ([a742969](https://github.com/ytsaurus/ytsaurus-ui/commit/a7429691c93d1884a2e2adf5d5b78b059c63bb9c))
* fix lint errors ([5250818](https://github.com/ytsaurus/ytsaurus-ui/commit/5250818deead43893caaf8f036493fa88e914442))
* **Host:** add ellipsis text in host ([2342404](https://github.com/ytsaurus/ytsaurus-ui/commit/2342404235fa050acd6f3046f966f48ca3bbd133))
* **Host:** remove classname ([70a7558](https://github.com/ytsaurus/ytsaurus-ui/commit/70a75582a7d77832d8402953e4560a0caeb955dd))
* **Host:** review changes ([5742f26](https://github.com/ytsaurus/ytsaurus-ui/commit/5742f2643ec7db5b67e641edb63ea0497119d74d))
* **Navigation:** text bug in tablets layout [YTFRONT-4133] ([ca58a9e](https://github.com/ytsaurus/ytsaurus-ui/commit/ca58a9e12eaabf1933ee7dc37bffd4d51581f3a8))
* **Navigation:** wrong symlinks path [YTFRONT-4128] ([f43e6f7](https://github.com/ytsaurus/ytsaurus-ui/commit/f43e6f7621f770f8fd94c1a1d23a76ad539e029b))
* **Query:** autocompete error old safari [YTFRONT-4125] ([bacb350](https://github.com/ytsaurus/ytsaurus-ui/commit/bacb350f21499e421c5309bdd5cab4a12a329110))
* **Query:** generating a query from a table [YTFRONT-4137] ([0fd3271](https://github.com/ytsaurus/ytsaurus-ui/commit/0fd3271fd9b543a4136502a52c749343a177e43f))

{% endcut %}


{% cut "**1.31.0**" %}

**Release date:** 2024-04-19


#### [1.31.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.30.0...ui-v1.31.0) (2024-04-19)


#### Features

* **ACL/RequestPermissions:** add all permission for Navigation [[#474](https://github.com/ytsaurus/ytsaurus-ui/issues/474)] ([fb219aa](https://github.com/ytsaurus/ytsaurus-ui/commit/fb219aa6a3b8df0a33848d18b876499a29903fad))
* **ACL:** request read permission for column group [YTFRONT-3482] ([62e9504](https://github.com/ytsaurus/ytsaurus-ui/commit/62e95048be674fd45f114d35875841689f2003c1))
* **ACL:** use separate tab for columns [YTFRONT-3836] ([374003a](https://github.com/ytsaurus/ytsaurus-ui/commit/374003ac979bb5a91e2dfff5d447c88c411b51e3))
* **Query:** change new query button to link [YTFRONT-4093] ([320cd98](https://github.com/ytsaurus/ytsaurus-ui/commit/320cd989a5b88ee93973e97113b243f06bb8968c))
* **Query:** spyt ytql autocomplete [YTFRONT-4118] ([ca86bb8](https://github.com/ytsaurus/ytsaurus-ui/commit/ca86bb84ceae35aa4b3cda11b38345ec7e26dc9c))


#### Bug Fixes

* **ACL/RequestPermissions:** handle path from attributes of error [YTFRONT-3502] ([f078a89](https://github.com/ytsaurus/ytsaurus-ui/commit/f078a89950169a642a48366b122492ffbfbd4b60))
* **Navigation:** allow to open items without access [YTFRONT-3836] ([0ad6f51](https://github.com/ytsaurus/ytsaurus-ui/commit/0ad6f514d9fc11c8f868e6c78ec804d918a7db31))
* **QueryTracker:** fix request parameters for validate and explain buttons in yql query [[#370](https://github.com/ytsaurus/ytsaurus-ui/issues/370)] ([85c052e](https://github.com/ytsaurus/ytsaurus-ui/commit/85c052ee5abfa3f825739829602c238ddb902e54))
* **userSettings:** user settings are not applied if you go to the cluster from the page with clusters [[#471](https://github.com/ytsaurus/ytsaurus-ui/issues/471)] ([37c2642](https://github.com/ytsaurus/ytsaurus-ui/commit/37c26422e1aa6923fa5e48929ef7673f4afb038c))

{% endcut %}


{% cut "**1.30.0**" %}

**Release date:** 2024-04-17


#### [1.30.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.29.0...ui-v1.30.0) (2024-04-17)


#### Features

* **UserCard:** add UserCard support in UIFactory ([c99fae5](https://github.com/ytsaurus/ytsaurus-ui/commit/c99fae5158143a77fa43b0f92ad7a18aba6d2240))


#### Bug Fixes

* **Query:** error while parsing yson [YTFRONT-4110] ([11b71cf](https://github.com/ytsaurus/ytsaurus-ui/commit/11b71cf7a4f96bcf788b19a2c87313eaf1596214))
* **QueryTracker:** fix request parameters for validate and explain buttons in yql query [[#370](https://github.com/ytsaurus/ytsaurus-ui/issues/370)] ([65abfc5](https://github.com/ytsaurus/ytsaurus-ui/commit/65abfc5dbd5b82df1ceca4852b8b6a4bee7c6db8))
* **userSettings:** user settings are not applied if you go to the cluster from the page with clusters [[#471](https://github.com/ytsaurus/ytsaurus-ui/issues/471)] ([134f5c1](https://github.com/ytsaurus/ytsaurus-ui/commit/134f5c17d6c0880cb6769543429e7476733d9a49))

{% endcut %}


{% cut "**1.29.0**" %}

**Release date:** 2024-04-12


#### [1.29.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.28.1...ui-v1.29.0) (2024-04-12)


#### Features

* **QueryTracker:** support validate and explain for yql queries [[#370](https://github.com/ytsaurus/ytsaurus-ui/issues/370)] ([2ba362e](https://github.com/ytsaurus/ytsaurus-ui/commit/2ba362e33cbcf3ba36443bb8e3c182b7b3617bb7))


#### Bug Fixes

* **Navigation/Table:** sync width of headers with data [YTFRONT-4109] ([cfb18df](https://github.com/ytsaurus/ytsaurus-ui/commit/cfb18dfd65e4595a8bc5b4ec29037c7b8841aeb0))
* **QueryTracker:** yql query progress shows wrong stage for a query step [[#368](https://github.com/ytsaurus/ytsaurus-ui/issues/368)] ([2c0fd6c](https://github.com/ytsaurus/ytsaurus-ui/commit/2c0fd6ca5a877fb2a3d5e513f42cf98ab6e4b06e))
* **QueryTracker:** yql query steps redirect on a wrong page [[#369](https://github.com/ytsaurus/ytsaurus-ui/issues/369)] ([d5ec33b](https://github.com/ytsaurus/ytsaurus-ui/commit/d5ec33ba72d34fcff628e33f8a518f1b29c2fd41))
* **Store:** change redux toolkit configuration [YTFRONT-4115] ([891ebdc](https://github.com/ytsaurus/ytsaurus-ui/commit/891ebdc15e4aa805632db5472ace701af16d8cae))
* **table:** there  is no headers in full window mode for table preview [[#422](https://github.com/ytsaurus/ytsaurus-ui/issues/422)] ([0e82358](https://github.com/ytsaurus/ytsaurus-ui/commit/0e82358d58a3598722edaba69f28a125a07ba44c))

{% endcut %}


{% cut "**1.28.1**" %}

**Release date:** 2024-04-10


#### [1.28.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.28.0...ui-v1.28.1) (2024-04-10)


#### Bug Fixes

* **Navigation:** emojis in names [YTFRONT-4104] ([fbf8c12](https://github.com/ytsaurus/ytsaurus-ui/commit/fbf8c122a7e2a7f10774ff9a65de62a1c3a0273c))
* **Query:** clique disabled by history query [YTFRONT-4105] ([747f9e2](https://github.com/ytsaurus/ytsaurus-ui/commit/747f9e2e6bc1966bb8d05b314eb233a33d637fdd))

{% endcut %}


{% cut "**1.28.0**" %}

**Release date:** 2024-04-09


#### [1.28.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.27.0...ui-v1.28.0) (2024-04-09)


#### Features

* add redux toolkit [YTFRONT-4094] ([e750edb](https://github.com/ytsaurus/ytsaurus-ui/commit/e750edb38aac578a9d48b92a2e769641cf13534a))
* **QueryTracker:** new columns to the list of queries [[#267](https://github.com/ytsaurus/ytsaurus-ui/issues/267)] ([22d69a8](https://github.com/ytsaurus/ytsaurus-ui/commit/22d69a89cdcff82346649adcf64fb46f4cec1d66))


#### Bug Fixes

* **configs:** add backward compatibility for YT_AUTH_CLUSTER_ID [[#349](https://github.com/ytsaurus/ytsaurus-ui/issues/349)] ([0deca57](https://github.com/ytsaurus/ytsaurus-ui/commit/0deca57a1a0ea3c32259ca8a83e340bd63514439))
* **Scheduling/Overview:** replace name filter with pool-selector [YTFRONT-4075] ([2865e09](https://github.com/ytsaurus/ytsaurus-ui/commit/2865e09cf3ffa91dcfc4378876b9dc881b20e2d8))
* **Scheduling:** fix for pools filter [[#460](https://github.com/ytsaurus/ytsaurus-ui/issues/460)] ([edf380d](https://github.com/ytsaurus/ytsaurus-ui/commit/edf380df6750cb3f5a2f23872dc4adee9247769d))

{% endcut %}


{% cut "**1.27.0**" %}

**Release date:** 2024-04-04


#### [1.27.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.26.0...ui-v1.27.0) (2024-04-04)


#### Features

* **Query:** add tooltip to search field qt [YTFRONT-4096] ([f5b2c7e](https://github.com/ytsaurus/ytsaurus-ui/commit/f5b2c7e8cc0000708b2407ed69dde7ce07ef4115))
* **Query:** add yql and chyt autocomplete [YTFRONT-4074] ([2e025aa](https://github.com/ytsaurus/ytsaurus-ui/commit/2e025aa8dab454e9d3a0fcc9c47967a8202d4af8))
* **Query:** page header redesign [YTFRONT-4041] ([b2d6696](https://github.com/ytsaurus/ytsaurus-ui/commit/b2d66969c46fae1bd68884298c92fda3be7183db))


#### Bug Fixes

* **Bundles/Editor:** allow to edit Memory/Reserved through 'Reset to default' [YTFRONT-4098] ([2b371fc](https://github.com/ytsaurus/ytsaurus-ui/commit/2b371fc64f137f5c18a9f839ede5a95131527269))
* **Bundles:** fix request params [YTFRONT-4072] ([92fd224](https://github.com/ytsaurus/ytsaurus-ui/commit/92fd2245d1c02e9fa50e37d2b969312a98e0bad9))
* delete unnecessary column [YTFRONT-4072] ([cc800a8](https://github.com/ytsaurus/ytsaurus-ui/commit/cc800a873c94cbbdf43622e1100b1886ebd75547))
* **Navigation:** wrong path to table in notification [YTFRONT-4091] ([21d87c1](https://github.com/ytsaurus/ytsaurus-ui/commit/21d87c193c24e82891a9b7457e11b516e11f29cf))
* **Navigation:** wrong schema col width [YTFRONT-4092] ([32e80d9](https://github.com/ytsaurus/ytsaurus-ui/commit/32e80d90ca8a3bd7748ac7cec5c87e551583dabc))
* **Query:** fix progress stages view [YTFRONT-4097] ([0098222](https://github.com/ytsaurus/ytsaurus-ui/commit/00982227f7219e864ec487cc8571b7d834c1a84b))
* **Query:** fixed error text selection [YTFRONT-4089] ([be5d802](https://github.com/ytsaurus/ytsaurus-ui/commit/be5d80270163541e7a77085b8d1c02391301c685))

{% endcut %}


{% cut "**1.26.0**" %}

**Release date:** 2024-03-29


#### [1.26.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.25.0...ui-v1.26.0) (2024-03-29)


#### Features

* **System:** monitoring tab [YTFRONT-4022] ([24d1885](https://github.com/ytsaurus/ytsaurus-ui/commit/24d18859a40226347efc4475db199b526063aa21))

{% endcut %}


{% cut "**1.25.0**" %}

**Release date:** 2024-03-28


#### [1.25.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.24.1...ui-v1.25.0) (2024-03-28)


#### Features

* **ACL:** add ability to describe custom permissionFlags [YTFRONT-4073] ([17be3da](https://github.com/ytsaurus/ytsaurus-ui/commit/17be3da4a03c6f807a13216215fb724f77b6f44e))
* **Query:** progress for spyt engine [YTFRONT-3981] ([14a59b0](https://github.com/ytsaurus/ytsaurus-ui/commit/14a59b01712146e99113a27dba6a556c10f8ca69))


#### Bug Fixes

* **navigation:** corrected the description change form [YTFRONT-4083] ([93ee0d1](https://github.com/ytsaurus/ytsaurus-ui/commit/93ee0d16c088e3aaebf7e71f825864ee945ba4be))

{% endcut %}


{% cut "**1.24.1**" %}

**Release date:** 2024-03-26


#### [1.24.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.24.0...ui-v1.24.1) (2024-03-26)


#### Bug Fixes

* **Query:** underline in monaco [YTFRONT-4069] ([d2bc351](https://github.com/ytsaurus/ytsaurus-ui/commit/d2bc35179fd924a2150f28f4aa2d278213592d7b))

{% endcut %}


{% cut "**1.24.0**" %}

**Release date:** 2024-03-21


#### [1.24.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.23.1...ui-v1.24.0) (2024-03-21)


#### Features

* **Accounts:** add attribute modal [YTFRONT-3829] ([0af6f85](https://github.com/ytsaurus/ytsaurus-ui/commit/0af6f85395fdbdd2b0168d360a8d213dbde920a9))

{% endcut %}


{% cut "**1.23.1**" %}

**Release date:** 2024-03-21


#### [1.23.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.23.0...ui-v1.23.1) (2024-03-21)


#### Bug Fixes

* **BundleEditorDialog:** confirm button should be clickable [YTFRONT-4076] ([e8f10da](https://github.com/ytsaurus/ytsaurus-ui/commit/e8f10daba41fb088f2d505ab30054f51531c36b8))
* request for settings is made without a cluster ([5243dc5](https://github.com/ytsaurus/ytsaurus-ui/commit/5243dc533d14576901d1749e029633b497de981e))

{% endcut %}


{% cut "**1.23.0**" %}

**Release date:** 2024-03-20


#### [1.23.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.22.3...ui-v1.23.0) (2024-03-20)


#### Features

* **Query:** new query error component [YTFRONT-4000] ([9d781f4](https://github.com/ytsaurus/ytsaurus-ui/commit/9d781f4633b57a287554b91e56ecde182e86abd4))

{% endcut %}


{% cut "**1.22.3**" %}

**Release date:** 2024-03-18


#### [1.22.3](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.22.2...ui-v1.22.3) (2024-03-18)


#### Bug Fixes

* **CHYT:** do not load pools while defaultPoolTree is empty [YTFRONT-3863] ([0b2e823](https://github.com/ytsaurus/ytsaurus-ui/commit/0b2e823adcc8f794fc4ec11c3951b2d444d9fb68))

{% endcut %}


{% cut "**1.22.2**" %}

**Release date:** 2024-03-18


#### [1.22.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.22.1...ui-v1.22.2) (2024-03-18)


#### Bug Fixes

* **CHYT/CreateModal:** use RangeInputPicker for 'Instances count' field [YTFRONT-3863] ([efdf7a5](https://github.com/ytsaurus/ytsaurus-ui/commit/efdf7a5b883381d1081acf3dc8fce8a28c4a077b))
* **CHYT:** default pool tree should be loaded properly [YTFRONT-3683] ([4ef42f4](https://github.com/ytsaurus/ytsaurus-ui/commit/4ef42f4597961af707ddeecd73eb36f751270e88))
* **CHYT:** minor fixes [YTFRONT-3863] ([fedd43d](https://github.com/ytsaurus/ytsaurus-ui/commit/fedd43dae9928c298da02fb60d427a91c9143245))
* **main:** Added missing used deps to package.json ([be505ec](https://github.com/ytsaurus/ytsaurus-ui/commit/be505ec0a489406a5dfda6503cbf52ad23b475f9))
* use var(--yt-font-weight) css variable ([847ba30](https://github.com/ytsaurus/ytsaurus-ui/commit/847ba308553e18507053800c888bfc5586a1815b))

{% endcut %}


{% cut "**1.22.1**" %}

**Release date:** 2024-03-17


#### [1.22.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.22.0...ui-v1.22.1) (2024-03-17)


#### Bug Fixes

* add migration description for ytAuthCluster -&gt; allowPasswordAuth [[#349](https://github.com/ytsaurus/ytsaurus-ui/issues/349)] ([d1a9b2b](https://github.com/ytsaurus/ytsaurus-ui/commit/d1a9b2b3e011ba77df2ee48bc8959530e5186ed5))

{% endcut %}


{% cut "**1.22.0**" %}

**Release date:** 2024-03-13


#### [1.22.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.21.0...ui-v1.22.0) (2024-03-13)


#### Features

* **odin:** support odin url per cluster ([959cbb9](https://github.com/ytsaurus/ytsaurus-ui/commit/959cbb9fcc0c4685f36eaa80748895593da94022))

{% endcut %}


{% cut "**1.21.0**" %}

**Release date:** 2024-03-12


#### [1.21.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.20.0...ui-v1.21.0) (2024-03-12)


#### Features

* **Navigation:** add new node types: rootstock, scion [YTFRONT-4046] ([1b5bdca](https://github.com/ytsaurus/ytsaurus-ui/commit/1b5bdcaa9f5ebc66fb2d10fae00706cd37eb0c32))


#### Bug Fixes

* **Operation/JobsMonitor:** better condition of visibility [YTFRONT-3940] ([caedb0c](https://github.com/ytsaurus/ytsaurus-ui/commit/caedb0c27c7464b734b55808fd1dbb074104d286))

{% endcut %}


{% cut "**1.20.0**" %}

**Release date:** 2024-03-06


#### [1.20.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.19.0...ui-v1.20.0) (2024-03-06)


#### Features

* **Navigation:** added quick header editing [YTFRONT-3783] ([514120a](https://github.com/ytsaurus/ytsaurus-ui/commit/514120a01f48cad3ea3a577a47a12b5aafaa4606))

{% endcut %}


{% cut "**1.19.0**" %}

**Release date:** 2024-03-04


#### [1.19.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.18.1...ui-v1.19.0) (2024-03-04)


#### Features

* **System:** add //sys/[@master](https://github.com/master)_alerts [YTFRONT-3960] ([7b2503d](https://github.com/ytsaurus/ytsaurus-ui/commit/7b2503d9c2aa8c2fde4c805d8e91589bafc80d6e))


#### Bug Fixes

* **Bundles/BundleEditor:** better validation of resources [YTFRONT-4035] ([349ac37](https://github.com/ytsaurus/ytsaurus-ui/commit/349ac37a2163b1c8712101cd19377a62b5df78a9))
* **Bundles/MetaTable:** add icons for state with details [YTFRONT-4038] ([96f7533](https://github.com/ytsaurus/ytsaurus-ui/commit/96f753355e3076c490591bd388c1561f34849ace))
* **Navigation:** hide unnecessary error `[code cancelled]` [YTFRONT-4034] ([e31cc61](https://github.com/ytsaurus/ytsaurus-ui/commit/e31cc61f57c6e89d6edf0627873c3f9d17f4d995))
* **Scheduling/Details:** for 'Cannot read properties of undefined (reading 'cpu')' [YTFRONT-4042] ([d3be924](https://github.com/ytsaurus/ytsaurus-ui/commit/d3be924172b78b2cd8b371d5df9adf02a6cf9a45))
* **System/Masters:** do not load hydra for discovery servers [YTFRONT-4043] ([d2513ff](https://github.com/ytsaurus/ytsaurus-ui/commit/d2513ff255d287e46540b5156bb3cc47236fc7df))

{% endcut %}


{% cut "**1.18.1**" %}

**Release date:** 2024-02-27


#### [1.18.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.18.0...ui-v1.18.1) (2024-02-27)


#### Bug Fixes

* **Scheduling:** do not use '.../orchid/scheduler/scheduling_info_per_pool_tree' [YTFRONT-3937] ([a5a93bb](https://github.com/ytsaurus/ytsaurus-ui/commit/a5a93bb5d61a8814c80c7f512ae7a4aaa4bcd764))

{% endcut %}


{% cut "**1.18.0**" %}

**Release date:** 2024-02-27


#### [1.18.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.17.1...ui-v1.18.0) (2024-02-27)


#### Features

* **Navigation:** added the ability to edit a document [YTFRONT-3921] ([98b6dba](https://github.com/ytsaurus/ytsaurus-ui/commit/98b6dba191c010b34b282098866ea5dc59ee724c))

{% endcut %}


{% cut "**1.17.1**" %}

**Release date:** 2024-02-27


#### [1.17.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.17.0...ui-v1.17.1) (2024-02-27)


#### Bug Fixes

* **Queries:** use POST-data for parameters of startQuery-command [YTFRONT-4023] ([d03c8e0](https://github.com/ytsaurus/ytsaurus-ui/commit/d03c8e0f65800a8332dac4165c43efe46a868885))
* scheduling broken aggregation [YTFRONT-4031] ([d460549](https://github.com/ytsaurus/ytsaurus-ui/commit/d460549d4073572dff855a962b8e7c1085566415))

{% endcut %}


{% cut "**1.16.2**" %}

**Release date:** 2024-02-22


#### [1.16.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.16.1...ui-v1.16.2) (2024-02-22)


#### Bug Fixes

* scheduling broken aggregation [YTFRONT-4031] ([f2c2246](https://github.com/ytsaurus/ytsaurus-ui/commit/f2c224699e2ef9d1efa9b245ba2716dc5f511376))

{% endcut %}


{% cut "**1.17.0**" %}

**Release date:** 2024-02-16


#### [1.17.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.16.1...ui-v1.17.0) (2024-02-16)


#### Features

* add multi-cluster passwd-auth [[#349](https://github.com/ytsaurus/ytsaurus-ui/issues/349)] ([ddf4617](https://github.com/ytsaurus/ytsaurus-ui/commit/ddf4617387ab8f88f901f268d72e17eff66d0f57))
* show authorized clusters [[#349](https://github.com/ytsaurus/ytsaurus-ui/issues/349)] ([d582bfc](https://github.com/ytsaurus/ytsaurus-ui/commit/d582bfcd490b8199e5713f24f7970697ba0513dd))

{% endcut %}


{% cut "**1.16.1**" %}

**Release date:** 2024-02-14


#### [1.16.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.16.0...ui-v1.16.1) (2024-02-14)


#### Bug Fixes

* minor fix for ts-error after rebase ([e5689aa](https://github.com/ytsaurus/ytsaurus-ui/commit/e5689aa02acb9e173b2689af100e76ced0b4e821))

{% endcut %}


{% cut "**1.16.0**" %}

**Release date:** 2024-02-14


#### [1.16.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.15.3...ui-v1.16.0) (2024-02-14)


#### Features

* **Operation/Details:** better live preview [YTFRONT-3956] ([0b1ffb9](https://github.com/ytsaurus/ytsaurus-ui/commit/0b1ffb97fbbc52c24836802c23200661b0ab344e))
* **query-tracker:** query aco management [[#246](https://github.com/ytsaurus/ytsaurus-ui/issues/246)] ([8b79661](https://github.com/ytsaurus/ytsaurus-ui/commit/8b79661cabc4a949687c407e4abcc08762bd776f))

{% endcut %}


{% cut "**1.15.3**" %}

**Release date:** 2024-02-12


#### [1.15.3](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.15.2...ui-v1.15.3) (2024-02-12)


#### Bug Fixes

* **Operation/DataFlow:** better column name for chunk_count [YTFRONT-3924] ([7477c48](https://github.com/ytsaurus/ytsaurus-ui/commit/7477c4837815b3b96923b81e5e66b32233e9346b))

{% endcut %}


{% cut "**1.15.2**" %}

**Release date:** 2024-02-09


#### [1.15.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.15.1...ui-v1.15.2) (2024-02-09)


#### Bug Fixes

* **layout:** the left menu disappears on long tables [[#225](https://github.com/ytsaurus/ytsaurus-ui/issues/225)] ([d54448a](https://github.com/ytsaurus/ytsaurus-ui/commit/d54448ab86c6ae888128ef485ff03d565331c2b2))
* **Navigation/MapNode:** do not wrap escaped characters ([60c0893](https://github.com/ytsaurus/ytsaurus-ui/commit/60c089364290fffae0d3ea88476c8a2cc6c52e40))
* **OperationsList:** fix for 'unexpected error' after aborting an operation [YTFRONT-4013] ([7847e91](https://github.com/ytsaurus/ytsaurus-ui/commit/7847e91ec931654a97b23d0e9c09972cb91ce61a))
* **Scheduling:** allow to create pools with &lt;Root&gt; parent [[#274](https://github.com/ytsaurus/ytsaurus-ui/issues/274)] ([91aa32e](https://github.com/ytsaurus/ytsaurus-ui/commit/91aa32e45aee7700fd55b12c2a0e77011fdb40a7))
* **table:** the left menu disappears on long tables [[#225](https://github.com/ytsaurus/ytsaurus-ui/issues/225)] ([4c4b015](https://github.com/ytsaurus/ytsaurus-ui/commit/4c4b015c1c327a9fc26dce39ca373fff3a4d709f))

{% endcut %}


{% cut "**1.15.1**" %}

**Release date:** 2024-02-05


#### [1.15.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.15.0...ui-v1.15.1) (2024-02-05)


#### Bug Fixes

* **settings:** read settings from localStorage [[#341](https://github.com/ytsaurus/ytsaurus-ui/issues/341)] ([ea6ddbd](https://github.com/ytsaurus/ytsaurus-ui/commit/ea6ddbd7a2d8f8a7ff8b0ab9c2eba7af4acfe3cb))

{% endcut %}


{% cut "**1.15.0**" %}

**Release date:** 2024-02-05


#### [1.15.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.14.3...ui-v1.15.0) (2024-02-05)


#### Features

* add UISettings.reportBugUrl [[#336](https://github.com/ytsaurus/ytsaurus-ui/issues/336)] ([e86ccb2](https://github.com/ytsaurus/ytsaurus-ui/commit/e86ccb2865918a7b62c7857a1079c2248b855286))


#### Bug Fixes

* **Operations/Details:** minor css fix [YTFRONT-3518] ([91d9b01](https://github.com/ytsaurus/ytsaurus-ui/commit/91d9b0172dd8de0c1d610bc9eb1ca8d2117b1dd6))
* **Scheduling/PoolEditor:** correct value for fifo_sort_parameters [YTFRONT-3957] ([34d5cdb](https://github.com/ytsaurus/ytsaurus-ui/commit/34d5cdb672b0eeadce80f10d1f828f1579e326ac))
* **SupportForm:** rework api of makeSupportContent [YTFRONT-3994] ([a563179](https://github.com/ytsaurus/ytsaurus-ui/commit/a563179b6ce5d85e79e24a58a4aa425b5708b281))

{% endcut %}


{% cut "**1.14.3**" %}

**Release date:** 2024-02-01


#### [1.14.3](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.14.2...ui-v1.14.3) (2024-02-01)


#### Bug Fixes

* do not wait for checkIsDeveloper response [YTFRONT-3862] ([4f0470b](https://github.com/ytsaurus/ytsaurus-ui/commit/4f0470b8a8a0fdd2beae8f911ab7666c0cdc5bbe))
* **timestampProvider:** update the default value when `clock_cell` is missing [YTFRONT-3946] ([8f44e05](https://github.com/ytsaurus/ytsaurus-ui/commit/8f44e05e1b6d655eab4fecdb1112647466625511))

{% endcut %}


{% cut "**1.14.2**" %}

**Release date:** 2024-01-30


#### [1.14.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.14.1...ui-v1.14.2) (2024-01-30)


#### Bug Fixes

* better error message for executeBatch ([1d96e53](https://github.com/ytsaurus/ytsaurus-ui/commit/1d96e539ffa3f2a3a690d3b3b081e8dea6b3a2db))
* update @ytsaurus/javascript v0.6.1 ([935cee4](https://github.com/ytsaurus/ytsaurus-ui/commit/935cee4ea253b13c8246c1ae740bf2833fb706f0))

{% endcut %}


{% cut "**1.14.1**" %}

**Release date:** 2024-01-30


#### [1.14.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.14.0...ui-v1.14.1) (2024-01-30)


#### Bug Fixes

* **Components/Node:** handle 'offline' nodes properly [YTFRONT-3993] ([53c4d16](https://github.com/ytsaurus/ytsaurus-ui/commit/53c4d169094d441df24b1c0b00210e186d1623af))
* **Navigation/Table/Merge:** update @ytsaurus/javascript-wrapper [YTFRONT-3953] ([f5b4128](https://github.com/ytsaurus/ytsaurus-ui/commit/f5b412879a8dffc453e2530878600f4723a95b0c))
* **Operations:** cancle requests properly [YTFRONT-3996] ([d3afc0f](https://github.com/ytsaurus/ytsaurus-ui/commit/d3afc0f4945e2679918f30061359fe70112b34cf))
* **xss:** fix an xss [YTFRONT-4004] ([7819f8a](https://github.com/ytsaurus/ytsaurus-ui/commit/7819f8a4c54c379d7e8300bbcc56b8192abb3e41))

{% endcut %}


{% cut "**1.14.0**" %}

**Release date:** 2024-01-29


#### [1.14.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.13.1...ui-v1.14.0) (2024-01-29)


#### Features

* **Scheduling:** load data only for visible pools [YTFRONT-3862] ([056f431](https://github.com/ytsaurus/ytsaurus-ui/commit/056f4319b034173728564cdb4f97f067665ff5af))


#### Bug Fixes

* **Components/Node:** 'offline' nodes should be handled properly [YTFRONT-3993] ([eb34e49](https://github.com/ytsaurus/ytsaurus-ui/commit/eb34e49e9ec12506f79d99a3cc32ce4ee1949f78))
* **Scheduling:** use pool_trees instead of scheduling_info_per_pool_tree [YTFRONT-3937] ([f745a67](https://github.com/ytsaurus/ytsaurus-ui/commit/f745a67285b1f649debc061a5938425626a07931))
* **support.js:** get rid of _DEV_PATCH_NUMBER [YTFRONT-3862] ([35caa4b](https://github.com/ytsaurus/ytsaurus-ui/commit/35caa4b7305a52f7bc8aa75d494e2fc109172756))
* **support:** scheduler, master should be checked properly [YTFRONT-3862] ([64e5583](https://github.com/ytsaurus/ytsaurus-ui/commit/64e5583485a48d5565964e8be380a68bf96b8910))

{% endcut %}


{% cut "**1.13.1**" %}

**Release date:** 2024-01-29


#### [1.13.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.13.0...ui-v1.13.1) (2024-01-29)


#### Bug Fixes

* receive the correct cell_tag [YTFRONT-3946] ([dee458b](https://github.com/ytsaurus/ytsaurus-ui/commit/dee458b1aa052e0d56ae90b2b405e4b662bed2ad))

{% endcut %}


{% cut "**1.13.0**" %}

**Release date:** 2024-01-26


#### [1.13.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.12.2...ui-v1.13.0) (2024-01-26)


#### Features

* implement an OAuth authorize [YTFRONT-3903] ([38fcda4](https://github.com/ytsaurus/ytsaurus-ui/commit/38fcda40dacbd12be0deba573b9fc32f17d445b5))

{% endcut %}


{% cut "**1.12.2**" %}

**Release date:** 2024-01-23


#### [1.12.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.12.1...ui-v1.12.2) (2024-01-23)


#### Bug Fixes

* add bundle resources validation [YTFRONT-3931] ([72b17d3](https://github.com/ytsaurus/ytsaurus-ui/commit/72b17d3160dd557c3b3cb4b7c311c4f348be237e))
* nodejs error during logout [[#292](https://github.com/ytsaurus/ytsaurus-ui/issues/292)] ([3e64d2c](https://github.com/ytsaurus/ytsaurus-ui/commit/3e64d2cef1760f7b40b866e806fc6f835d007cbd))

{% endcut %}


{% cut "**1.12.1**" %}

**Release date:** 2024-01-22


#### [1.12.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.12.0...ui-v1.12.1) (2024-01-22)


#### Bug Fixes

* **Queries/Results:** synchronize table header when resizing [[#294](https://github.com/ytsaurus/ytsaurus-ui/issues/294)] ([f625984](https://github.com/ytsaurus/ytsaurus-ui/commit/f6259848aa7159474ce929cc963f065383e3382b))

{% endcut %}


{% cut "**1.12.0**" %}

**Release date:** 2024-01-22


#### [1.12.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.11.2...ui-v1.12.0) (2024-01-22)


#### Features

* added a new query button [[#238](https://github.com/ytsaurus/ytsaurus-ui/issues/238)] ([b66fa31](https://github.com/ytsaurus/ytsaurus-ui/commit/b66fa31927187debe8361e7866f33dc62b211026))
* **Components/Node:** add 'Unrecognized options' tab [YTFRONT-3936] ([520916b](https://github.com/ytsaurus/ytsaurus-ui/commit/520916baddf345ff1c5f082dd6be57e4a3514fdb))

{% endcut %}


{% cut "**1.11.2**" %}

**Release date:** 2024-01-16


#### [1.11.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.11.1...ui-v1.11.2) (2024-01-16)


#### Bug Fixes

* **CHYT:** do not display CHYT-page when chyt_controller_base_url is empty [YTFRONT-3863] ([cb66484](https://github.com/ytsaurus/ytsaurus-ui/commit/cb664842ffdd6ae56461afdc85340ba6c9fbd602))

{% endcut %}


{% cut "**1.11.1**" %}

**Release date:** 2024-01-11


#### [1.11.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.11.0...ui-v1.11.1) (2024-01-11)


#### Bug Fixes

* **CHYT:** minor fixes [YTFRONT-3863] ([b71db09](https://github.com/ytsaurus/ytsaurus-ui/commit/b71db097642001cf21adc67493da0763443fa931))

{% endcut %}


{% cut "**1.11.0**" %}

**Release date:** 2024-01-09


#### [1.11.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.10.0...ui-v1.11.0) (2024-01-09)


#### Features

* **CHYT:** add CHYT page with list of cliques [YTFRONT-3683] ([de0c74a](https://github.com/ytsaurus/ytsaurus-ui/commit/de0c74a368ab37b5aa953e965efab8d8a4d9b2e1))
* update @gravity-ui/dialog-fields v4.3.0 ([5f61464](https://github.com/ytsaurus/ytsaurus-ui/commit/5f614647b3c70084d682cbf233df727107425eea))


#### Bug Fixes

* **PoolSuggestControl:** load all pools ([d56d0df](https://github.com/ytsaurus/ytsaurus-ui/commit/d56d0df20f1e39dd93074283ec5bdc9edc2622e0))

{% endcut %}


{% cut "**1.10.0**" %}

**Release date:** 2023-12-22


#### [1.10.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.9.0...ui-v1.10.0) (2023-12-22)


#### Features

* display location ids on data node table [[#204](https://github.com/ytsaurus/ytsaurus-ui/issues/204)] ([29ff849](https://github.com/ytsaurus/ytsaurus-ui/commit/29ff849f75af54056b471ea0546cce96b9f7037d))
* prevent user from closing the browser window with unsaved query text [[#226](https://github.com/ytsaurus/ytsaurus-ui/issues/226)] ([e3d12e8](https://github.com/ytsaurus/ytsaurus-ui/commit/e3d12e8e9c65639f6892b5be1ab9031581400c11))
* **query-tracker:** hide query history sidebar [[#211](https://github.com/ytsaurus/ytsaurus-ui/issues/211)] ([5602087](https://github.com/ytsaurus/ytsaurus-ui/commit/5602087899f0bf07eeb20312ee31cab66a055719))


#### Bug Fixes

* **Queries:** do not display empty progress tab [YTFRONT-3952] ([858b11f](https://github.com/ytsaurus/ytsaurus-ui/commit/858b11f1ba262456953a75121e430f686c6a6e36))
* render complex types in navigation [[#229](https://github.com/ytsaurus/ytsaurus-ui/issues/229)] ([1bef4ae](https://github.com/ytsaurus/ytsaurus-ui/commit/1bef4ae7493dfed7f30bb2a64ac81d65e189bb7c))

{% endcut %}


{% cut "**1.9.0**" %}

**Release date:** 2023-12-20


#### [1.9.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.8.0...ui-v1.9.0) (2023-12-20)


#### Features

* add SettingMenuItem.props.useSwitch ([c3f5154](https://github.com/ytsaurus/ytsaurus-ui/commit/c3f5154212c4d30b81fed8910996be686444e95b))

{% endcut %}


{% cut "**1.8.0**" %}

**Release date:** 2023-12-19


#### [1.8.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.7.3...ui-v1.8.0) (2023-12-19)


#### Features

* update @gravity-ui/navigation v1.8.0 ([e5530e1](https://github.com/ytsaurus/ytsaurus-ui/commit/e5530e16155d9bf124acf41d825093e92206462a))

{% endcut %}


{% cut "**1.7.3**" %}

**Release date:** 2023-12-18


#### [1.7.3](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.7.2...ui-v1.7.3) (2023-12-18)


#### Bug Fixes

* **main.js:** extract MonacoEditor to a separate chunk [YTFRONT-3814] ([2492c78](https://github.com/ytsaurus/ytsaurus-ui/commit/2492c78a187e1ba5be76b8254ffb5c2927b75c9d))

{% endcut %}


{% cut "**1.7.2**" %}

**Release date:** 2023-12-13


#### [1.7.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.7.1...ui-v1.7.2) (2023-12-13)


#### Bug Fixes

* sync packages/ui/package-lock.json ([381a97f](https://github.com/ytsaurus/ytsaurus-ui/commit/381a97f8ce0fde3ed85a316b133b0045d55af51e))

{% endcut %}


{% cut "**1.7.0**" %}

**Release date:** 2023-12-13


#### [1.7.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.6.0...ui-v1.7.0) (2023-12-13)


#### Features

* **query-tracker:** added file attachments to queries [[#221](https://github.com/ytsaurus/ytsaurus-ui/issues/221)] ([16d4138](https://github.com/ytsaurus/ytsaurus-ui/commit/16d41384621d368e83b34bfc5d1de933afc7d7b9))


#### Bug Fixes

* **Components/SetupModal:** fix Racks filter [YTFRONT-3944] ([0662e07](https://github.com/ytsaurus/ytsaurus-ui/commit/0662e07355402c5cbed8ffc3dd39997397532ea0))
* **RemoteCopy:** get rid of erasure_codec, compression_codec for a while [YTFRONT-3935] ([8518c96](https://github.com/ytsaurus/ytsaurus-ui/commit/8518c968200e92f6fe7242635515435fb70b1505))
* **Scheduling:** tree selector should be filterable [YTFRONT-3948] ([3102f5e](https://github.com/ytsaurus/ytsaurus-ui/commit/3102f5ea9a04d03f75420999dbbeb776e481afbf))
* **System/Chunks:** i.get is not a function [YTFRONT-3943] ([8c6a9e5](https://github.com/ytsaurus/ytsaurus-ui/commit/8c6a9e59f5f9ca31ab9f8fd7b881c847d130bd8a))

{% endcut %}


{% cut "**1.6.0**" %}

**Release date:** 2023-12-08


#### [1.6.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.5.2...ui-v1.6.0) (2023-12-08)


#### Features

* **Table/Schema:** add external title column [YTFRONT-3939] ([074f638](https://github.com/ytsaurus/ytsaurus-ui/commit/074f6386e104989531b0d2432c581f5296f3b60d))


#### Bug Fixes

* **query-tracker:** dynamic system columns' content is now displayed correctly [[#192](https://github.com/ytsaurus/ytsaurus-ui/issues/192)] ([271b1f6](https://github.com/ytsaurus/ytsaurus-ui/commit/271b1f6660627a5c10fbae1a60145e03c111acc6))

{% endcut %}


{% cut "**1.5.2**" %}

**Release date:** 2023-12-06


#### [1.5.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.5.1...ui-v1.5.2) (2023-12-05)


#### Bug Fixes

* pagination callback not rerendering when switching queries ([99db6af](https://github.com/ytsaurus/ytsaurus-ui/commit/99db6af890cbce0213223d605fe604e5ea64b19c))
* **query tracker:** row count and truncated flag are now displayed above results table [[#210](https://github.com/ytsaurus/ytsaurus-ui/issues/210)] ([fe200b9](https://github.com/ytsaurus/ytsaurus-ui/commit/fe200b9a2f964f484aa0e820ea9c68c7b12d8d32))
* **query-tracker:** Query results with more than 50 columns are not shown properly [#208](https://github.com/ytsaurus/ytsaurus-ui/issues/208) ([8e2ddc7](https://github.com/ytsaurus/ytsaurus-ui/commit/8e2ddc77b3b2691a346a3bd22be8b5d2558b61f5))

{% endcut %}


{% cut "**1.5.1**" %}

**Release date:** 2023-12-01


#### [1.5.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.5.0...ui-v1.5.1) (2023-12-01)


#### Bug Fixes

* readme Development section improve ([c06921b](https://github.com/ytsaurus/ytsaurus-ui/commit/c06921ba3956cd861411927c025607884d991f8b))

{% endcut %}


{% cut "**1.5.0**" %}

**Release date:** 2023-12-01


#### [1.5.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.4.0...ui-v1.5.0) (2023-12-01)


#### Features

* **QT:** add query list visible toggle button ([1ba0ccd](https://github.com/ytsaurus/ytsaurus-ui/commit/1ba0ccdc2c7e095c86660f472f72eee62210c710))
* **QT:** Progress and timeline component [YTFRONT-3840] ([a092966](https://github.com/ytsaurus/ytsaurus-ui/commit/a092966199317abc8a637569bd28e9249ab8c5ac))


#### Bug Fixes

* **QT:** fallback Loader center alignment ([3c1ad2b](https://github.com/ytsaurus/ytsaurus-ui/commit/3c1ad2bb88ca0c489de4d0afc511c28650b2d62a))
* **QT:** yql stage setting should not affect other engines requests ([1f2b253](https://github.com/ytsaurus/ytsaurus-ui/commit/1f2b253d7305ea870f27ed5cc28edfffe0fdeb43))

{% endcut %}


{% cut "**1.4.0**" %}

**Release date:** 2023-11-17


#### [1.4.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.3.1...ui-v1.4.0) (2023-11-17)


#### Features

* **Updater:** handle 'visibilitychange' events of window.document [YTFRONT-3835] ([76fe005](https://github.com/ytsaurus/ytsaurus-ui/commit/76fe0050e2d0c0c1fe969336cbfe57ba6c70432a))

{% endcut %}


{% cut "**1.3.1**" %}

**Release date:** 2023-11-16


#### [1.3.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.3.0...ui-v1.3.1) (2023-11-16)


#### Bug Fixes

* **Accounts,Bundles:** better defaults for accounting settings [YTFRONT-3891] ([8a79e4b](https://github.com/ytsaurus/ytsaurus-ui/commit/8a79e4b0bc1c2156e0dd10646cb069bd4b1e1dd0))
* **Accounts:** do not use cache after editing [YTFRONT-3920] ([0ab91a0](https://github.com/ytsaurus/ytsaurus-ui/commit/0ab91a0bdc80ef08c51d79894b4121afa6f9435e))
* **Jobs:** do not use uppercase for job-type [YTFRONT-3917] ([1f4e1bf](https://github.com/ytsaurus/ytsaurus-ui/commit/1f4e1bf45082c1e77a7a5f989b080e08196f069f))
* **Navigation/Consumer:** fix for Target Queue selector [YTFRONT-3910] ([5358127](https://github.com/ytsaurus/ytsaurus-ui/commit/5358127d57339867b2124239eeb48ac8ca47555d))
* **Navigation/MapNode:** truncate long names with ellipsis [YTFRONT-3913] ([67eddcb](https://github.com/ytsaurus/ytsaurus-ui/commit/67eddcb5d1a2529068610b7fdb7e60ab17506ed1))
* **Odin:** minor layout fix [YTFRONT-3909] ([8012884](https://github.com/ytsaurus/ytsaurus-ui/commit/8012884b26849902a19358937893695023c42b26))
* **reShortNameFromAddress:** better default [YTFRONT-3861] ([39f07ad](https://github.com/ytsaurus/ytsaurus-ui/commit/39f07ad0b9b98863033b2beccd3c6f8798a19006))
* **Scheduling:** fix for pool-tree selector [YTFRONT-3918] ([3fb86bb](https://github.com/ytsaurus/ytsaurus-ui/commit/3fb86bb7f192afda9a5103184f65403117a75d6b))

{% endcut %}


{% cut "**1.3.0**" %}

**Release date:** 2023-11-10


#### [1.3.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.2.0...ui-v1.3.0) (2023-11-10)


#### Features

* add UIFactory.getAllowedExperimentalPages method [YTFRONT-3912] ([fca2666](https://github.com/ytsaurus/ytsaurus-ui/commit/fca266621a7731db5dfe979efde095d0d2c6c4d5))

{% endcut %}


{% cut "**1.2.0**" %}

**Release date:** 2023-11-10


#### [1.2.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.1.2...ui-v1.2.0) (2023-11-10)


#### Features

* **QT:** add Progress component [YTFRONT-3840] ([69a787a](https://github.com/ytsaurus/ytsaurus-ui/commit/69a787a25d67f14d3a8687d65c35eb71f654a295))


#### Bug Fixes

* **QT:** fix result tabs switching when polling [YTFRONT-3840] ([b304c2b](https://github.com/ytsaurus/ytsaurus-ui/commit/b304c2bc96d4018a133fea41e3a5f2bfb37d413a))
* **QT:** Plan add table and operation urls to nodes [YTFRONT-3840] ([080acbc](https://github.com/ytsaurus/ytsaurus-ui/commit/080acbc879c34e3c979dc81c61b36a488ddff18b))

{% endcut %}


{% cut "**1.1.2**" %}

**Release date:** 2023-11-09


#### [1.1.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.1.1...ui-v1.1.2) (2023-11-09)


#### Bug Fixes

* babel config app-builder conflict ([e435a25](https://github.com/ytsaurus/ytsaurus-ui/commit/e435a259a040fbf09490873210cca5ad37ff3e9d))

{% endcut %}


{% cut "**1.1.1**" %}

**Release date:** 2023-10-27


#### [1.1.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.1.0...ui-v1.1.1) (2023-10-27)


#### Bug Fixes

* **Navigation/ACL**: allow column groups create form only for map_nodes [YTFRONT-3901] ([32a8bf0](https://github.com/ytsaurus/ytsaurus-ui/commit/32a8bf0b043881f574d60839c328bb65806c0a01))
* **GroupsPage:** get rid of updater from GroupsPage [YTFRONT-3835] ([548798b](https://github.com/ytsaurus/ytsaurus-ui/commit/548798ba7fc413d7e767d54e20aa89f5c276406b))
* **Users:** get rid of updater from UsersPage [YTFRONT-3835] ([78cd8e8](https://github.com/ytsaurus/ytsaurus-ui/commit/78cd8e849907a6c8bab7b9e2714844e51c8861da))

{% endcut %}


{% cut "**0.23.1**" %}

**Release date:** 2023-10-26


#### [0.23.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.23.0...ui-v0.23.1) (2023-10-26)


#### Bug Fixes

* allow column groups create form only for map_nodes [YTFRONT-3901] ([bb3d183](https://github.com/ytsaurus/ytsaurus-ui/commit/bb3d1834ca8112d0afd7e5788372580e06a40b74))
* **GroupsPage:** get rid of updater from GroupsPage [YTFRONT-3835] ([c68967c](https://github.com/ytsaurus/ytsaurus-ui/commit/c68967c4bb5fe7f0a75432ba1170ad041dac4a14))
* **Users:** get rid of updater from UsersPage [YTFRONT-3835] ([1e1e887](https://github.com/ytsaurus/ytsaurus-ui/commit/1e1e88788fedd9ce2b3ddd22f171c844210dedec))

{% endcut %}


{% cut "**1.1.0**" %}

**Release date:** 2023-10-20


#### [1.1.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.0.2...ui-v1.1.0) (2023-10-20)


#### Features

* **ClusterAppearance:** add ability to redefine cluster icons [YTFRONT-3879] ([61e27f7](https://github.com/ytsaurus/ytsaurus-ui/commit/61e27f71390f08c101dbfd0df1650ee27a4014c2))
* **ClusterConfig:** add 'externalProxy' field [YTFRONT-3890] ([c172097](https://github.com/ytsaurus/ytsaurus-ui/commit/c172097b4324fc84ec720e3ba786f0baa6b5f5d2))
* **Compoents/Nodes:** add 'Flavors' column [YTFRONT-3886] ([0256361](https://github.com/ytsaurus/ytsaurus-ui/commit/0256361235bf0b541f9cba4088408af97c27ede1))
* **UISettings:** add reShortNameFromAddress [YTFRONT-3861] ([fa433ba](https://github.com/ytsaurus/ytsaurus-ui/commit/fa433baaa65fa3debe895630e5727c8015d3d6f8))
* **unipika:** add UISettings.hidReferrerUrl (+e2e) [YTFRONT-3875] ([2ee7524](https://github.com/ytsaurus/ytsaurus-ui/commit/2ee75245d638694e5d61dccfc56e394960fddd81))
* **unipika:** add UISettings.reUnipikaAllowTaggedSources [YTFRONT-3875] ([6039c30](https://github.com/ytsaurus/ytsaurus-ui/commit/6039c3081c2d135ed45c2e330fec4cad1d3727b7))


#### Bug Fixes

* **ACL:** unrecognized roles should be highlighted [YTFRONT-3885] ([de284ca](https://github.com/ytsaurus/ytsaurus-ui/commit/de284ca31d4c1ccf0e7c79eb3263bb61a2197b22))
* **BundleEditorDialog:** key_filter_block_cache should affect value of 'Free' memory [YTFRONT-3825] ([2ffc449](https://github.com/ytsaurus/ytsaurus-ui/commit/2ffc44916afe804fd1c5c89727eb2d0ffb78f3db))
* **Components/Nodes:** User/system tags should not be wider than its table cell ([0562e48](https://github.com/ytsaurus/ytsaurus-ui/commit/0562e48a0e7600bcc974dbfce9dd682a0b051c1c))
* **controllers/home:** get rid of 'Strict-Transport-Security' header [YTFRONT-3896] ([1fd14b0](https://github.com/ytsaurus/ytsaurus-ui/commit/1fd14b0b3ab76f193730c2909c150dfeaa5e2e19))
* **Markdown:** YFM should not duplicate headers [YTFRONT-3897] ([cc92e5b](https://github.com/ytsaurus/ytsaurus-ui/commit/cc92e5bcd688aaa601e3cc4bdfe31f21549efefc))

{% endcut %}


{% cut "**1.0.2**" %}

**Release date:** 2023-10-09


#### [1.0.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.0.1...ui-v1.0.2) (2023-10-09)


#### Bug Fixes

* update @gravity-ui/charkit v4.7.2, @gravity-ui/yagr v3.10.4 ([e647df2](https://github.com/ytsaurus/ytsaurus-ui/commit/e647df2cb980ae60f73afe7cbefd6bd1478f6e38))

{% endcut %}


{% cut "**1.0.1**" %}

**Release date:** 2023-10-09


#### [1.0.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v1.0.0...ui-v1.0.1) (2023-10-09)


#### Bug Fixes

* try to rerun release ([ebcb80f](https://github.com/ytsaurus/ytsaurus-ui/commit/ebcb80f09df5a8ddd479f295dc701c01558748a3))

{% endcut %}


{% cut "**1.0.0**" %}

**Release date:** 2023-10-09


#### [1.0.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.23.0...ui-v1.0.0) (2023-10-09)


#### ⚠ BREAKING CHANGES

* update @gravity-ui/uikit v5

#### Features

* update @gravity-ui/uikit v5 ([1c89981](https://github.com/ytsaurus/ytsaurus-ui/commit/1c8998151fc8053bdbb4486359b6c53b53476dc3))

{% endcut %}


{% cut "**0.23.0**" %}

**Release date:** 2023-10-02


#### [0.23.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.22.0...ui-v0.23.0) (2023-10-02)


#### Features

* **Components/HttpProxies,RPCProxies:** Add NodeMaintenance modal [YTFRONT-3792] ([f1a68bd](https://github.com/ytsaurus/ytsaurus-ui/commit/f1a68bdb534af5279e80a8fd703d09f8eaac77af))
* **Components/Nodes:** add NodeMaintenanceModal [YTFRONT-3792] ([1b01b70](https://github.com/ytsaurus/ytsaurus-ui/commit/1b01b70dcafde2beee6fcadc60beec6555085d4b))

{% endcut %}


{% cut "**0.22.0**" %}

**Release date:** 2023-09-29


#### [0.22.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.21.1...ui-v0.22.0) (2023-09-29)


#### Features

* **QT:** add SPYT engine option [YTFRONT-3872] ([ec04fe3](https://github.com/ytsaurus/ytsaurus-ui/commit/ec04fe33bafe280d376d8ccc457f6a15726b87de))


#### Bug Fixes

* **Scheduling:** Ephemeral pools should be visible [YTFRONT-3708] ([65dd571](https://github.com/ytsaurus/ytsaurus-ui/commit/65dd571d9fa46fbfbfdf2fa2a149b08e556c92ec))
* uncaught error from browser's console ([9bfe4a4](https://github.com/ytsaurus/ytsaurus-ui/commit/9bfe4a489ec67d1e7556d1c815217823234a45f8))

{% endcut %}


{% cut "**0.21.1**" %}

**Release date:** 2023-09-26


#### [0.21.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.21.0...ui-v0.21.1) (2023-09-26)


#### Bug Fixes

* **Components/Versions:** fix state,banned columns for Details [YTFRONT-3854] ([eed0e91](https://github.com/ytsaurus/ytsaurus-ui/commit/eed0e9137460c27caf6b7303bbab1270c6a4217e))
* **Components/Versions:** use 'cluster_node' instead of 'node' [YTFRONT-3854] ([d60950d](https://github.com/ytsaurus/ytsaurus-ui/commit/d60950d4be467830dd924ea7f883eefffae000a8))

{% endcut %}


{% cut "**0.21.0**" %}

**Release date:** 2023-09-19


#### [0.21.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.20.0...ui-v0.21.0) (2023-09-19)


#### Features

* **Navigation:** use [@effective_expiration](https://github.com/effective) attribute [YTFRONT-3665] ([6eafe35](https://github.com/ytsaurus/ytsaurus-ui/commit/6eafe35452282cfaff96123e808da59db02964f3))


#### Bug Fixes

* **System/Nodes:** minor fix for firefox/safari [YTFRONT-3297] ([c537054](https://github.com/ytsaurus/ytsaurus-ui/commit/c537054a752b423bac9204348fe86de25fdabcc4))

{% endcut %}


{% cut "**0.20.0**" %}

**Release date:** 2023-09-15


#### [0.20.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.19.1...ui-v0.20.0) (2023-09-15)


#### Features

* **System/Nodes,HttpProxies,RPCProxies:** new design for details [YTFRONT-3297] ([5fd5795](https://github.com/ytsaurus/ytsaurus-ui/commit/5fd5795c72a9643ab1a4cb6e17f328d00111110a))

{% endcut %}


{% cut "**0.19.1**" %}

**Release date:** 2023-09-14


#### [0.19.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.19.0...ui-v0.19.1) (2023-09-14)


#### Bug Fixes

* **System/Masters:** minor fixes for 'voting' flag [YTFRONT-3832] ([0b7df45](https://github.com/ytsaurus/ytsaurus-ui/commit/0b7df45f2feba18ded63b9020d89a578c52fbf09))

{% endcut %}


{% cut "**0.19.0**" %}

**Release date:** 2023-09-13


#### [0.19.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.18.0...ui-v0.19.0) (2023-09-13)


#### Features

* **System/Master:** display 'nonvoting'-flag [YTFRONT-3832] ([77a5953](https://github.com/ytsaurus/ytsaurus-ui/commit/77a5953d07ce5f53fd56f1267c8888a3e6cd2e6a))


#### Bug Fixes

* **AccountQuotaEditor:** better handling for /[@allow](https://github.com/allow)_children_limit_overcommit [YTFRONT-3839] ([d53ba9a](https://github.com/ytsaurus/ytsaurus-ui/commit/d53ba9a533f0f299b0bf2a1636c88c39000248a3))
* do not load '[@alerts](https://github.com/alerts)' from Components/Nodes ([38e4a90](https://github.com/ytsaurus/ytsaurus-ui/commit/38e4a90bb877033525a9d729c1c772e2b701cef9))
* **Navigation/Jobs:** use direct links for commands: read_file, get_job_input, get_job_stderr, get_job_fail_context [YTFRONT-3833] ([7f549b2](https://github.com/ytsaurus/ytsaurus-ui/commit/7f549b2e591d7da1d5e0370b979d62cbab903881))
* **Navigation:** add ability to remove table from current path [YTFRONT-3837] ([ad016ac](https://github.com/ytsaurus/ytsaurus-ui/commit/ad016ac17e0c51f33b2d9bb2364d7d1b00fa6e07))
* **PoolEditorDialog:** remove 'Burst RAM', 'Flow RAM' fields [YTFRONT-3838] ([29541f4](https://github.com/ytsaurus/ytsaurus-ui/commit/29541f4981183a70fa8e8777bd4b8d736ea89c7f))

{% endcut %}


{% cut "**0.18.0**" %}

**Release date:** 2023-09-11


#### [0.18.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.17.0...ui-v0.18.0) (2023-09-07)


#### Features

* **BundleEditor:** check user permissions for 'write' [YTFRONT-3785] ([35dc1d0](https://github.com/ytsaurus/ytsaurus-ui/commit/35dc1d099956876a826d1f9088baae85169260b6))
* **Tablet:** turn off StoresDialog for tablet with more than 200 stores [YTFRONT-3766] ([d1f64d1](https://github.com/ytsaurus/ytsaurus-ui/commit/d1f64d1c9dce8e283b3c4ccde35857ac3217efca))


#### Bug Fixes

* **Navigation:** display cyrillic nodes caption and breadcrumbs [YTFRONT-3784] ([715c1ad](https://github.com/ytsaurus/ytsaurus-ui/commit/715c1ad985d18076436cbe24c16d9707c41e9ace))
* **QT:** refactor polling, fix endless running state [YTFRONT-3852] ([2c7b283](https://github.com/ytsaurus/ytsaurus-ui/commit/2c7b283e0020e72ae81156917c83d5a051ce80a3))

{% endcut %}


{% cut "**0.17.0**" %}

**Release date:** 2023-08-31


#### [0.17.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.16.1...ui-v0.17.0) (2023-08-30)


#### Features

* **QT:** check and display query results meta with errors [YTFRONT-3797] ([d9a6d15](https://github.com/ytsaurus/ytsaurus-ui/commit/d9a6d152233513486aba0c9f9e1818862af7f344))
* **QT:** decorate errors in monaco-editor [YTFRONT-3797] ([7c74a8d](https://github.com/ytsaurus/ytsaurus-ui/commit/7c74a8d0e39e5f6dd49db48c77e7a1a771948c31))


#### Bug Fixes

* **QT:** results tab data update dependencies ([baf8166](https://github.com/ytsaurus/ytsaurus-ui/commit/baf81669223f9274d3a98d7e2739db314f06d1a6))
* url params encoding, show escaped symbols for paths [YTFRONT-3784] ([6ff0a63](https://github.com/ytsaurus/ytsaurus-ui/commit/6ff0a6324bbd86e9e660149a4385e224e2db4350))

{% endcut %}


{% cut "**0.16.1**" %}

**Release date:** 2023-08-21


#### [0.16.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.16.0...ui-v0.16.1) (2023-08-21)


#### Bug Fixes

* update monaco versions ([c54b83a](https://github.com/ytsaurus/ytsaurus-ui/commit/c54b83ad02cfbf873a5e3080c03417a6e166572e))

{% endcut %}


{% cut "**0.16.0**" %}

**Release date:** 2023-08-16


#### [0.16.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.15.0...ui-v0.16.0) (2023-08-15)


#### Features

* **Job/Specification:** read specification from 'job_spec_ext' [YTFRONT-3802] ([108f2e9](https://github.com/ytsaurus/ytsaurus-ui/commit/108f2e9be5bc68b63296f8f2dac1831aa528597a))

{% endcut %}


{% cut "**0.15.0**" %}

**Release date:** 2023-08-08


#### [0.15.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.14.2...ui-v0.15.0) (2023-08-07)


#### Features

* **CreateDirectoryModal:** add 'recursive' parameter [YTFRONT-3805] ([6ffd436](https://github.com/ytsaurus/ytsaurus-ui/commit/6ffd4361210b4687e15ca75eec289d83207c90aa))
* **Navigation/Tablets:** add overlapping_store_count to dynTable Histogram [YTFRONT-3380] ([4232709](https://github.com/ytsaurus/ytsaurus-ui/commit/423270902484a3c578cf7178ca04c6a8265f5f4d))
* **OperationJobsTable:** format jobs type coloumn value [YTFRONT-3746] ([dba10c8](https://github.com/ytsaurus/ytsaurus-ui/commit/dba10c871a7c265d6a402e6cdf1255e8feb36d02))


#### Bug Fixes

* a fix for misprint [YTFRONT-3804] ([daae1a9](https://github.com/ytsaurus/ytsaurus-ui/commit/daae1a9ed0fb6933d6ef4748778fdc53e8bf09a5))
* **OperationDetails:** better layout for 'Environment' [YTFRONT-3781] ([c28804e](https://github.com/ytsaurus/ytsaurus-ui/commit/c28804e5e2c7005cfa3751865b01e3177d4e1245))
* replace //sys/proxies with //sys/http_proxies [YTFRONT-3799] ([595e8fe](https://github.com/ytsaurus/ytsaurus-ui/commit/595e8fe86e3afbf7243b05ec1c2eda2e18ef299c))
* sort state parsing for url-mapping [YTFRONT-3707] ([b3c4e66](https://github.com/ytsaurus/ytsaurus-ui/commit/b3c4e665c86064102be2f9a44faad519cff27b6d))
* **Table/Dynamic:** search by keys does not work [YTFRONT-3808] ([98341af](https://github.com/ytsaurus/ytsaurus-ui/commit/98341af2f8358b094defdba6daeedb0013552226))

{% endcut %}


{% cut "**0.14.2**" %}

**Release date:** 2023-07-28


#### [0.14.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.14.1...ui-v0.14.2) (2023-07-28)


#### Bug Fixes

* **deploy:** minor fix for superviord ([1d49499](https://github.com/ytsaurus/ytsaurus-ui/commit/1d494998b880a06c91cb01b36e7ca9cf049fe4ff))

{% endcut %}


{% cut "**0.14.1**" %}

**Release date:** 2023-07-27


#### [0.14.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.14.0...ui-v0.14.1) (2023-07-27)


#### Bug Fixes

* **Dockerfile:** minor fix for building image ([ee00ecd](https://github.com/ytsaurus/ytsaurus-ui/commit/ee00ecdb9eebf4878737aae5d94a7c792b27dea7))

{% endcut %}


{% cut "**0.14.0**" %}

**Release date:** 2023-07-27


#### [0.14.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.13.1...ui-v0.14.0) (2023-07-27)


#### Features

* **dev:** use nodejs 18 ([9af6662](https://github.com/ytsaurus/ytsaurus-ui/commit/9af666268fd7e0c2e56317503a06edc86d792172))

{% endcut %}


{% cut "**0.13.1**" %}

**Release date:** 2023-07-27


#### [0.13.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.13.0...ui-v0.13.1) (2023-07-27)


#### Bug Fixes

* **Components/Node/MemoryUsage:** use virtualized table [YTFRONT-3796] ([267eeef](https://github.com/ytsaurus/ytsaurus-ui/commit/267eeeffbfa3a559d1cf74296492dbfb3644289d))
* **Components/Node:** bring back missing locations [YTFRONT-3796] ([d3686b3](https://github.com/ytsaurus/ytsaurus-ui/commit/d3686b31c76cd8ac365f54337ebe41434c420ea3))

{% endcut %}


{% cut "**0.13.0**" %}

**Release date:** 2023-07-26


#### [0.13.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.12.0...ui-v0.13.0) (2023-07-26)


#### Features

* **QT:** add QT request settings override with UI [YTFRONT-3790] ([95479bb](https://github.com/ytsaurus/ytsaurus-ui/commit/95479bbabdd260e148879a3be2623ec9f008979f))

{% endcut %}


{% cut "**0.12.0**" %}

**Release date:** 2023-07-21


#### [0.12.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.11.3...ui-v0.12.0) (2023-07-21)


#### Features

* **Components/Nodes:** use attributes.paths and attributes.keys for list of nodes [YTFRONT-3378] ([a60ec5e](https://github.com/ytsaurus/ytsaurus-ui/commit/a60ec5e191a14221400b610c94aa15cf4fe670da))
* enable query name editing [YTFRONT-3649] ([f375ea4](https://github.com/ytsaurus/ytsaurus-ui/commit/f375ea468543299b7a18d2b92417b9966c8e664c))


#### Bug Fixes

* **PoolEditorDialog:** weight field remove request when value was not changed [YTFRONT-3748] ([1d25e5b](https://github.com/ytsaurus/ytsaurus-ui/commit/1d25e5bd0fabb720a5121fdc11ade5692e2fccd2))
* QT format decimal number results [YTFRONT-3782] ([58d6f66](https://github.com/ytsaurus/ytsaurus-ui/commit/58d6f66ac684774e1a45d656b8dfda9d9a9e5af8))

{% endcut %}


{% cut "**0.11.3**" %}

**Release date:** 2023-07-14


#### [0.11.3](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.11.2...ui-v0.11.3) (2023-07-14)


#### Bug Fixes

* README ([cd2d3db](https://github.com/ytsaurus/ytsaurus-ui/commit/cd2d3dbe2e75341274a9c0a71f36db1ee34878f2))

{% endcut %}


{% cut "**0.11.2**" %}

**Release date:** 2023-07-14


#### [0.11.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.11.1...ui-v0.11.2) (2023-07-14)


#### Bug Fixes

* ui lock ([03940a6](https://github.com/ytsaurus/ytsaurus-ui/commit/03940a6c2240cabc78f5592b99e7540d32c1531e))

{% endcut %}


{% cut "**0.11.1**" %}

**Release date:** 2023-07-14


#### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @ytsaurus/javascript-wrapper bumped from ^0.2.1 to ^0.3.0

{% endcut %}


{% cut "**0.11.0**" %}

**Release date:** 2023-07-06


#### [0.11.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.10.0...ui-v0.11.0) (2023-07-06)


#### Features

* Add UISettings.accountsMonitoring config option [YTFRONT-3698] ([71a8902](https://github.com/ytsaurus/ytsaurus-ui/commit/71a8902344892881bad6cd8d43f56a04efad3ebc))
* Add UISettings.bundlesMonitoring config option [YTFRONT-3698] ([ff7f90a](https://github.com/ytsaurus/ytsaurus-ui/commit/ff7f90ae7eb4404332ed627e5f34e8eeb3a109df))
* Add UISettings.operationsMonitoring config option [YTFRONT-3698] ([893f716](https://github.com/ytsaurus/ytsaurus-ui/commit/893f71618dd6929fb2eef8d3bb5d87e46f67e950))
* Add UISettings.schedulingMonitoring config option [YTFRONT-3698] ([eb1959b](https://github.com/ytsaurus/ytsaurus-ui/commit/eb1959bf5e9bb75c967c1c2cbff0ca84f70a4f59))
* **Components/Nodes:** Add 'Chaos slots' view mode [YTFRONT-3333] ([9aa0461](https://github.com/ytsaurus/ytsaurus-ui/commit/9aa046177d4a25f1dd7d6ea10c1f58a628bc4e51))


#### Bug Fixes

* improve QT engine switching behaviour / rework Open Query Tracker button [YTFRONT-3713] ([0453125](https://github.com/ytsaurus/ytsaurus-ui/commit/045312528754dde84bc4fcc7f9156248c7db2348))

{% endcut %}


{% cut "**0.10.0**" %}

**Release date:** 2023-07-04


#### [0.10.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.9.1...ui-v0.10.0) (2023-07-04)


#### Features

* **BundleControllerEditor:** add 'Reserved' field [YTFRONT-3673] ([4e497e1](https://github.com/ytsaurus/ytsaurus-ui/commit/4e497e1b7f7f508d771ee54027dc2c627f706edf))

{% endcut %}


{% cut "**0.9.1**" %}

**Release date:** 2023-06-26


#### [0.9.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.9.0...ui-v0.9.1) (2023-06-26)


#### Bug Fixes

* Changelog tiny fix ([21ad9cf](https://github.com/ytsaurus/ytsaurus-ui/commit/21ad9cf5a4e4c9c8499eb54d5b9d2d1c8492e863))

{% endcut %}


{% cut "**0.9.0**" %}

**Release date:** 2023-06-20


#### [0.9.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.8.0...ui-v0.9.0) (2023-06-20)


#### Features

* enable delete action for OS [YTFRONT-3721] ([8f6d7ed](https://github.com/ytsaurus/ytsaurus-ui/commit/8f6d7ede82bbd41fdfd0fc9c562777c5409c952b))
* enable ManageAcl form [YTFRONT-3721] ([6a49956](https://github.com/ytsaurus/ytsaurus-ui/commit/6a49956e0c97a4e972335bd068e43ea342cf5793))
* enable PERMISSIONS_SETTINGS override with UIFactory [YTFRONT-3721] ([99ab661](https://github.com/ytsaurus/ytsaurus-ui/commit/99ab6610385d34816e700a4838fbb24a624b077f))
* enable Request Permission form for os version [YTFRONT-3721] ([6634f50](https://github.com/ytsaurus/ytsaurus-ui/commit/6634f50c51b4f78ebbd54fbcc2fe0bdd5f8875c9))

{% endcut %}


{% cut "**0.8.0**" %}

**Release date:** 2023-06-19


#### [0.8.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.7.0...ui-v0.8.0) (2023-06-19)


#### Features

* remote copy modal -> suggest transfer_* pool if exists [YTFRONT-3511] ([19674ea](https://github.com/ytsaurus/ytsaurus-ui/commit/19674eade06d19adf6ab141a5662b2f922e305f1))


#### Bug Fixes

* (PoolEditorDialog) add number validation to Weight field [YTFRONT-3748] ([84b4fde](https://github.com/ytsaurus/ytsaurus-ui/commit/84b4fde9605b1cb693478d1b0706c050da5c3ecb))

{% endcut %}


{% cut "**0.7.0**" %}

**Release date:** 2023-06-16


#### [0.7.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.6.4...ui-v0.7.0) (2023-06-16)


#### Features

* **Navigation/AttributesEditor:** allow to edit '/[@expiration](https://github.com/expiration)_time' and '/[@expiration](https://github.com/expiration)_timout' [YTFRONT-3665] ([9983381](https://github.com/ytsaurus/ytsaurus-ui/commit/9983381cb7a4eaa09e5d82b5e8ed6232e49cd0b1))
* **System/Nodes:** add 'Node type' filter [YTFRONT-3163] ([9e7a956](https://github.com/ytsaurus/ytsaurus-ui/commit/9e7a9564dcded3044f866d4ab55bb118a3a50a40))


#### Bug Fixes

* ACL page tables styles [YTFRONT-3758] ([0c97d70](https://github.com/ytsaurus/ytsaurus-ui/commit/0c97d70b5504258e1a22eccc7ca4e4ac9f3b55d8))

{% endcut %}


{% cut "**0.6.4**" %}

**Release date:** 2023-06-02


#### [0.6.4](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.6.3...ui-v0.6.4) (2023-06-02)


#### Bug Fixes

* get rid of unnecessary console.log ([6d06778](https://github.com/ytsaurus/ytsaurus-ui/commit/6d06778b6f9eea1ab834807ee4e68061d362b5a9))

{% endcut %}


{% cut "**0.6.3**" %}

**Release date:** 2023-06-02


#### [0.6.3](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.6.2...ui-v0.6.3) (2023-06-02)


#### Bug Fixes

* add @gravity-ui/dialog-fields to peerDeps ([7a23bce](https://github.com/ytsaurus/ytsaurus-ui/commit/7a23bce132fbf479728b9ae85f1e88f3efc174e4))
* increase jobsCount limit for JobsMonitor tab [YTFRONT-3752] ([9e61525](https://github.com/ytsaurus/ytsaurus-ui/commit/9e61525d601ccc42ecb94ed326dbee6e03f71728))

{% endcut %}


{% cut "**0.6.2**" %}

**Release date:** 2023-06-01


#### [0.6.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.6.1...ui-v0.6.2) (2023-06-01)


#### Bug Fixes

* **Navigation:** Do not load pool tree unless necessary [YTFRONT-3747] ([61192df](https://github.com/ytsaurus/ytsaurus-ui/commit/61192dfa7d2c38a0ace6a2bc0c80ae178a4ebedc))

{% endcut %}


{% cut "**0.6.1**" %}

**Release date:** 2023-06-01


#### [0.6.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.6.0...ui-v0.6.1) (2023-06-01)


#### Bug Fixes

* **JobsMonitor:** fix a misprint in warning ([6945d1e](https://github.com/ytsaurus/ytsaurus-ui/commit/6945d1e5f052281ff08e92a8b924ea224be1a2eb))

{% endcut %}


{% cut "**0.6.0**" %}

**Release date:** 2023-05-25


#### [0.6.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.5.1...ui-v0.6.0) (2023-05-25)


#### Features

* Add 'register_queue_consumer', 'register_queue_consumer_vital' permissions [YTFRONT-3327] ([d6bd889](https://github.com/ytsaurus/ytsaurus-ui/commit/d6bd8890c2e62c96448043ac44ffa70a83178142))
* **Navigation/Consumer:** Model is changed from 'many-to-one' to 'many-to-many' [YTFRONT-3327] ([2014422](https://github.com/ytsaurus/ytsaurus-ui/commit/2014422b5797000fdda66feb51ca441874b03e38))


#### Bug Fixes

* **Account/General:** minor fix for styles [YTFRONT-3741] ([7eea79a](https://github.com/ytsaurus/ytsaurus-ui/commit/7eea79adc103ecddd773144acfbe6e74e3f58863))
* **Scheduling/PoolSuggest:** better order of items [YTFRONT-3739] ([150db4f](https://github.com/ytsaurus/ytsaurus-ui/commit/150db4fc1c33cb448a1f9ba3faa6e88c1b67c33c))

{% endcut %}


{% cut "**0.5.1**" %}

**Release date:** 2023-05-19


#### [0.5.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.5.0...ui-v0.5.1) (2023-05-19)


#### Bug Fixes

* (OperationsArchiveFilter) input styles specificity [3728] ([b587906](https://github.com/ytsaurus/ytsaurus-ui/commit/b587906c21552ba584fdff35a6b48a2582ddde70))
* (OperationsArchiveFilter) reseting time on date change and custom date initial value on toggle modes [3728] ([fdfd045](https://github.com/ytsaurus/ytsaurus-ui/commit/fdfd0456d8fcf49bc24316f533d511c1b8275147))
* **TabletCellBundle:** better layout for MetaTable [YTFRONT-3716] ([6904a4c](https://github.com/ytsaurus/ytsaurus-ui/commit/6904a4cd7574c1061cedcba9ba4454cf04791aec))

{% endcut %}


{% cut "**0.5.0**" %}

**Release date:** 2023-05-12


#### [0.5.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.4.2...ui-v0.5.0) (2023-05-10)


#### Features

* ACL add object permissions own filters [YTFRONT-3720] ([d9dfed1](https://github.com/ytsaurus/ytsaurus-ui/commit/d9dfed146bd72c248003350dc0f1a3c228801dfc))


#### Bug Fixes

* get path from attributes for Schema component [YTFRONT-3722] ([97bca2c](https://github.com/ytsaurus/ytsaurus-ui/commit/97bca2cea18c582697a7375396c7b17c89499e67))

{% endcut %}


{% cut "**0.4.2**" %}

**Release date:** 2023-05-03


#### [0.4.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.4.1...ui-v0.4.2) (2023-05-03)


#### Bug Fixes

* **JobDetails/StatisticsIO:** total row should be displayed propertly [YTFRONT-3723] ([980a4fc](https://github.com/ytsaurus/ytsaurus-ui/commit/980a4fc19564e36aee4716dd35259af986b78ff0))
* **Navigation:TableMeta:** hide dyn-table attributes for static tables [3725] ([4fa79f7](https://github.com/ytsaurus/ytsaurus-ui/commit/4fa79f7b9e3182c542510060d682f2421fe9ca85))
* **Navigation/Table:** fix error for specific value of localStorage.SAVED_COLUMN_SETS [YTFRONT-3710] ([529d8bf](https://github.com/ytsaurus/ytsaurus-ui/commit/529d8bf9577171335e42fcd72378c358c7a38a62))
* **Scheduling/Overview:** add more levels to stylets [YTFRONT-3724] ([d3dca2b](https://github.com/ytsaurus/ytsaurus-ui/commit/d3dca2b6323ce24dbe18b6cf978cdbc1843ddf8a))
* **TabletCellBundle:** better layout for meta-table [YTFRONT-3716] ([f1073b8](https://github.com/ytsaurus/ytsaurus-ui/commit/f1073b82480a13d64e40aae460da73290de09e36))

{% endcut %}


{% cut "**0.4.1**" %}

**Release date:** 2023-04-28


#### [0.4.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.4.0...ui-v0.4.1) (2023-04-28)


#### Bug Fixes

* **Navigation/MapNode:** Names should not be cut with ellipsis [YTFRONT-3711] ([8a48398](https://github.com/ytsaurus/ytsaurus-ui/commit/8a48398007ca289881668032f8b17dabda2dafde))

{% endcut %}


{% cut "**0.4.0**" %}

**Release date:** 2023-04-27


#### [0.4.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.3.1...ui-v0.4.0) (2023-04-27)


#### Features

* add 'Stale' flag to Job's metadata [YTFRONT-3712] ([6ed4597](https://github.com/ytsaurus/ytsaurus-ui/commit/6ed45979195ca638b99cd895c3aa0a80fe07b561))
* add inherited popover tip ([d4c76ab](https://github.com/ytsaurus/ytsaurus-ui/commit/d4c76ab893db9a4bacf77b1eb245408644194a37))
* correct subjects filtering for groups ([93d8500](https://github.com/ytsaurus/ytsaurus-ui/commit/93d85003639d426a2835949f482fec088cf19f0b))
* remove highlighting ([dc74075](https://github.com/ytsaurus/ytsaurus-ui/commit/dc74075ab1ab795c3ef935e15ed01f267e432459))
* split and filter objectPermissions ([93d8500](https://github.com/ytsaurus/ytsaurus-ui/commit/93d85003639d426a2835949f482fec088cf19f0b))
* **Table:** Add 'Combine chunks' flag to Merge/Erase modal ([aeec0ca](https://github.com/ytsaurus/ytsaurus-ui/commit/aeec0cabd87d4ec896f54972240a7708cfa9f531))


#### Bug Fixes

* ACL grid column sizes ([e0bd03b](https://github.com/ytsaurus/ytsaurus-ui/commit/e0bd03bcab24913f6fac82c3438bdb6db39e893f))
* acl subject column ellipsis ([7a7fd4e](https://github.com/ytsaurus/ytsaurus-ui/commit/7a7fd4e2e91e0911100dc2c5c2070797c60783bc))
* **BundleController:** handle properly case when bundle controller is unavailable [YTFRONT-3636] ([940a441](https://github.com/ytsaurus/ytsaurus-ui/commit/940a44155aac3624567ba0c709b962ee9957c717))
* **Navigation:** add 'disabled'-flag for 'More actions' button [YTFRONT-3705] ([fa4226a](https://github.com/ytsaurus/ytsaurus-ui/commit/fa4226a082521c7ef693178fbf53a599e31a49b0))
* **Operation/Statistics:** fix for strange behavior of 'Collapse all' button [YTFRONT-3719] ([e4d55aa](https://github.com/ytsaurus/ytsaurus-ui/commit/e4d55aacfb60c8d0b319c8098c1485c3e46a7b0a))

{% endcut %}


{% cut "**0.3.1**" %}

**Release date:** 2023-04-19


#### [0.3.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.3.0...ui-v0.3.1) (2023-04-19)


#### Bug Fixes

* **Table/Schema:** minor fix for width of columns [YTFRONT-3667] ([0abe89d](https://github.com/ytsaurus/ytsaurus-ui/commit/0abe89d7d570c662ae6646622b904f98f9297e7f))

{% endcut %}


{% cut "**0.3.0**" %}

**Release date:** 2023-04-18


#### [0.3.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.2.3...ui-v0.3.0) (2023-04-18)


#### Features

* **Operation/Statistics:** add pool-tree filter (statistics-v2) [YTFRONT-3598] ([8b03968](https://github.com/ytsaurus/ytsaurus-ui/commit/8b039687f2e9025baa9bdaec861866ac2c3443ef))

{% endcut %}


{% cut "**0.2.3**" %}

**Release date:** 2023-04-17


#### [0.2.3](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.2.2...ui-v0.2.3) (2023-04-17)


#### Bug Fixes

* bring back telemetry ([b24d977](https://github.com/ytsaurus/ytsaurus-ui/commit/b24d977b78273105f8e3f49b1ad3d0946160320b))

{% endcut %}


{% cut "**0.2.2**" %}

**Release date:** 2023-04-14


#### [0.2.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.2.1...ui-v0.2.2) (2023-04-14)


#### Bug Fixes

* add  for tablets with 0 Cells [YTFRONT-3696] ([63acb21](https://github.com/ytsaurus/ytsaurus-ui/commit/63acb214a5f25ad6458daddc8db9d5fa93eed91f))
* rework font select [YTFRONT-3691] ([717fa89](https://github.com/ytsaurus/ytsaurus-ui/commit/717fa89ad5aceca74e6587d444b672d50ba2ed07))

{% endcut %}


{% cut "**0.2.1**" %}

**Release date:** 2023-04-07


#### [0.2.1](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.2.0...ui-v0.2.1) (2023-04-07)


#### Bug Fixes

* remove unnecessary files ([f4b51c2](https://github.com/ytsaurus/ytsaurus-ui/commit/f4b51c2a5a79705913adf3377e1590ad5368d1fb))

{% endcut %}


{% cut "**0.2.0**" %}

**Release date:** 2023-04-06


#### [0.2.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.1.0...ui-v0.2.0) (2023-04-06)


#### Features

* Add tutorials list ([8241d3a](https://github.com/ytsaurus/ytsaurus-ui/commit/8241d3a933877113b4a1b3a452e84a46417bbebe))


#### Bug Fixes

* typos and abc missing link Edit Bundle Form [YTFRONT-3676] ([aa617d3](https://github.com/ytsaurus/ytsaurus-ui/commit/aa617d3fad7ad1bfd14e0217d44599dd895bc24b))

{% endcut %}


{% cut "**0.1.0**" %}

**Release date:** 2023-04-05


#### [0.1.0](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.0.4...ui-v0.1.0) (2023-04-05)


#### Features

* Add button to create a query from table ([7c94ee5](https://github.com/ytsaurus/ytsaurus-ui/commit/7c94ee5286d96c0ffb617d16e41020b4e92e08d7))
* add QT proxy ([c624e5d](https://github.com/ytsaurus/ytsaurus-ui/commit/c624e5d847d96dbd9045bb38019811c367ea666c))


#### Bug Fixes

* add settings queryTrackerCluster for ya-env. Reset screen setting after close QTWidget. ([215a72b](https://github.com/ytsaurus/ytsaurus-ui/commit/215a72b6c5ca023c97ded4208d4d50c8f1d8642a))
* EditableAsText with controls (used in QT TopRowElement) ([574dbae](https://github.com/ytsaurus/ytsaurus-ui/commit/574dbae73de87f726152ffba809a03629c4a7d3a))
* fix encoding in query text and results ([d3e2780](https://github.com/ytsaurus/ytsaurus-ui/commit/d3e2780852a82b90ad0af2aace6c7a697220ad5c))
* fix for broken layout of bundles editor ([b389ea8](https://github.com/ytsaurus/ytsaurus-ui/commit/b389ea85ce0d5e46318c8e1f00081a56fec02851))
* Fix for default CHYT-alias ([9231085](https://github.com/ytsaurus/ytsaurus-ui/commit/9231085899ef64dd73add88989771e509fa43346))
* style draft queries. Fix queries without finish_time. Read scheme from get_result_table ([fdd121a](https://github.com/ytsaurus/ytsaurus-ui/commit/fdd121a589090f045bc805b1106f02444539c609))

{% endcut %}


{% cut "**0.0.4**" %}

**Release date:** 2023-03-24


#### [0.0.4](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.0.3...ui-v0.0.4) (2023-03-24)


#### Bug Fixes

* **ui:** add 'files' field to package.json ([cb51d75](https://github.com/ytsaurus/ytsaurus-ui/commit/cb51d756af502f25fab413ff26c20b5e2ce90abf))

{% endcut %}


{% cut "**0.0.3**" %}

**Release date:** 2023-03-24


#### [0.0.3](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.0.2...ui-v0.0.3) (2023-03-24)


#### Bug Fixes

* ytsaurus/ui docker image fixed for localmode ([5033eb9](https://github.com/ytsaurus/ytsaurus-ui/commit/5033eb9c1c5ab4aaf8029c678847231eb7a6bd18))

{% endcut %}


{% cut "**0.0.2**" %}

**Release date:** 2023-03-24


#### [0.0.2](https://github.com/ytsaurus/ytsaurus-ui/compare/ui-v0.0.1...ui-v0.0.2) (2023-03-24)


#### Bug Fixes

* add missing config ([e391c59](https://github.com/ytsaurus/ytsaurus-ui/commit/e391c59fdf5c0ee72e8899daae0dfd3d4e34f4e7))

{% endcut %}

