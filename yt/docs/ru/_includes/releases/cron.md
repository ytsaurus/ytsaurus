## Cron


Release notes for this component.




**Releases:**

{% cut "**0.0.4**" %}

**Release date:** 2026-02-13


**Release page:** [0.0.4](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/cron/0.0.4)


**Helm chart:** [0.0.4](https://github.com/orgs/ytsaurus/packages/container/cron-chart/689408263?tag=0.0.4)


#### Features
- Introduce snapshot processing cron tasks. See [documentation](https://ytsaurus.tech/docs/en/admin-guide/install-cron#process_master_snapshot) for details. [fdacc5428e1cfc107c71dc1dc0212ec23c708edd]

#### Fixes
- Fix short timeout in `clear-tmp` cron task [511aa1ce1f053263182382aa2a6a33b4e2989ff3]


{% endcut %}


{% cut "**0.0.2**" %}

**Release date:** 2025-04-24


**Release page:** [0.0.2](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/cron/0.0.2)


**Helm chart:** [0.0.2](https://github.com/orgs/ytsaurus/packages/container/cron-chart/401688677?tag=0.0.2)


#### Fixes

- Renamed the `prune_offline_servers` script to `prune_offline_cluster_nodes`.

{% endcut %}


{% cut "**0.0.1**" %}

**Release date:** 2025-04-11


**Release page:** [0.0.1](https://github.com/ytsaurus/ytsaurus/releases/tag/docker/cron/0.0.1)


**Helm chart:** [0.0.1](https://github.com/orgs/ytsaurus/packages/container/cron-chart/393601221?tag=0.0.1)


Initial release of the YTsaurus Cron.

#### Features
- Installs a configurable set of cron jobs for YTsaurus cluster maintenance
- Built-in jobs include:
    - clear_tmp_location
    - clear_tmp_files
    - clear_tmp_trash
    - prune_offline_servers
- Support for custom job definitions via additionalJobs
- Secure token configuration via direct value or Kubernetes Secret
- Customizable resource settings, schedule policies, and concurrency control

{% endcut %}

