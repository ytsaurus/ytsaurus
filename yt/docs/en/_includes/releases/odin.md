## Odin


Is released as a docker image. Installation details are available in the [Odin installation guide](https://ytsaurus.tech/docs/ru/admin-guide/install-odin).




**Releases:**

{% cut "**0.0.9**" %}

**Release date:** 2026-04-20


**Release page:** [0.0.9](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/odin/0.0.9)


**Docker image:** [0.0.9](https://github.com/orgs/ytsaurus/packages/container/odin-chart/810778603?tag=0.0.9)


#### Features
- Add new odin checks: `cypress_commands`, `read_static_table_commands`, `write_static_table_commands` [19f4319f81ebe8541cbbb1c2e90c3624f29c0a8e, 8d758f30284fb63c24aa1a4f354121db985ff1d3]

#### Fixes:
- Fixed IPv4/IPv6 dual-stack binding issues [545338d48c649f9248fb9fd4a131297771410c60]

#### Breaking Changes
- Value `host: "::"` is no longer valid. To listen on all interfaces, you must use `host: "*"`. If you haven't explicitly overridden the `host` parameter in your `values.yaml`, no action is required [545338d48c649f9248fb9fd4a131297771410c60]
- The `config.webservice.debug` parameter has been removed from `values.yaml` [545338d48c649f9248fb9fd4a131297771410c60]


{% endcut %}


{% cut "**0.0.8**" %}

**Release date:** 2026-03-23


**Release page:** [0.0.8](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/odin/0.0.8)


**Docker image:** [0.0.8](https://github.com/orgs/ytsaurus/packages/container/odin-chart/752466721?tag=0.0.8)


#### Fixes
- Fixed an issue with creating non-reusable sockets [c0d18d14808f9e086ba0b2d568f26174d9387fb9]
- Fixed the incorrect `check_virtual_map_size` behavior on failed Odin checks [399dcfe92e222fe2fae5a963323e81a511f18510]
- Fixed multiple issues in Odin virtual map checks [3e812ec48ba2016e1d73a8db7c2c9b1601517b66, c37a2a70c6e55e62348bec5cbf07d54375129498, ca3698d07e7ddac28194cbc2e69f8ff583018c50]
- Added retries for `all writes disabled` exceptions during storage writes to prevent Odin restarts caused by tablet cell bundle overloads [178f0a3df344e341ad420c479a3e06fad131e9d6]
- Fixed the insufficient timeout for the `stuck_missing_part_chunks` Odin check [9eca40a6f830a144023544ccd5b0bb07cb6d046b]


{% endcut %}


{% cut "**0.0.7**" %}

**Release date:** 2026-02-02


**Release page:** [0.0.7](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/odin/0.0.7)


**Docker image:** [0.0.7](https://github.com/orgs/ytsaurus/packages/container/odin-chart/667861164?tag=0.0.7)


#### Fixes
- Fix `Missing Part Chunks` check
- Enable `Quorum Missing Chunks` check by default
- Enable `Inconsistently Placed Chunks` check by default


{% endcut %}


{% cut "**0.0.6**" %}

**Release date:** 2026-01-26


**Release page:** [0.0.6](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/odin/0.0.6)


**Docker image:** [0.0.6](https://github.com/orgs/ytsaurus/packages/container/odin-chart/657705318?tag=0.0.6)


#### Features
- Add `system_quotas_yt_job_logs` check
- Add Odin checks for chunk samples

#### Fixes
- Fix `tmp_node_count` check when directory is missing
- Fix inability to rise immediately after falling
- Fix wrong attribute name in `queue_agent_alerts`


{% endcut %}


{% cut "**0.0.5**" %}

**Release date:** 2025-11-19


**Release page:** [0.0.5](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/odin/0.0.5)


**Docker image:** [0.0.5](https://github.com/orgs/ytsaurus/packages/container/odin-chart/582216203?tag=0.0.5)


We are happy to announce the first public release of **Odin** — a monitoring service for YTsaurus clusters.

Step-by-step installation guide is available here: [Odin Deployment Guide](https://ytsaurus.tech/docs/en/admin-guide/install-odin)

{% endcut %}

