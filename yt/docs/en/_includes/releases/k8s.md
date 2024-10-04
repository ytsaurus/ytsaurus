## Kubernetes operator

Is released as helm charts on [Github Packages](https://github.com/ytsaurus/ytsaurus-k8s-operator/pkgs/container/ytop-chart).

**Current release:** {{k8s-operator-version}}

**All releases:**

{% cut "**0.16.2**" %}

**Bugfix:**
- Fix strawberry controller image for 2nd job by [@l0kix2](https://github.com/l0kix2). [#345](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/345)

**Contributors:**
- [@l0kix2](https://github.com/l0kix2)

{% endcut %}

{% cut "**0.16.1**" %}

{% note warning "Warning" %}

This release has a bug if Strawberry components is enabled. Use 0.16.2 instead.

{% endnote %}

**Bugfix:**
- Revert job image override for UI/strawberry by [@l0kix2](https://github.com/l0kix2) in [#344](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/344) — the bug was introduced in 0.16.0.

**Contributors:**
- [@l0kix2](https://github.com/l0kix2)

{% endcut %}

{% cut "**0.16.0**" %}

{% note warning "Warning" %}

This release has a bug for a configuration where UI or Strawberry components are enabled and some of their images were overridden (k8s init jobs will fail for such components).

Use 0.16.2 instead.

{% endnote %}

**Minor changes:**
- Add observedGeneration field to the YtsaurusStatus by [@wilwell](https://github.com/wilwell) in [#333](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/333).
- Set statistics for job low cpu usage alerts by [@koct9i](https://github.com/koct9i) in [#335](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/335).
- Add nodeSelector for UI and Strawberry by [@l0kix2](https://github.com/l0kix2) in [#338](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/338).
- Init job creates from InstanceSpec image if specified by [@wilwell](https://github.com/wilwell) in [#336](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/336).
- Add tolerations and nodeselectors to jobs by [@l0kix2](https://github.com/l0kix2) in [#342](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/342).

**Contributors:**
- [@koct9i](https://github.com/koct9i), [@l0kix2](https://github.com/l0kix2), [@wilwell](https://github.com/wilwell)

{% endcut %}

{% cut "**0.15.0**" %}

**Backward incompatible changes:**

- Component pod labels were refactored in [#326](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/326) and changes are:
    - `app.kubernetes.io/instance` was removed.
    - `app.kubernetes.io/name` was Ytsaurus before, now it contains component type.
    - `app.kubernetes.io/managed-by` is "ytsaurus-k8s-operator" instead of "Ytsaurus-k8s-operator".
- Deprecated chyt field in the main YTsaurus spec was removed, use strawberry field with the same schema instead.

**Minor changes:**
- Added tolerations for Strawberry by [@qurname2](https://github.com/qurname2) in [#328](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/328).
- Refactor label names for components by [@achulkov2](https://github.com/achulkov2) in [#326](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/326).

**Experimental:**
- RemoteDataNodes by [@qurname2](https://github.com/qurname2) in [#330](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/330).

**Contributors:**
- [@achulkov2](https://github.com/achulkov2), [@qurname2](https://github.com/qurname2)

{% endcut %}

{% cut "**0.14.0**" %}

**Backward incompatible changes:**

- Before this release StrawberryController was unconditionally configured with `{address_resolver={enable_ipv4=%true;enable_ipv6=%true}}` in its static config. From now on it respects common useIpv6 and useIpv4 fields, which can be set in the [YtsaurusSpec](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/docs/api.md#ytsaurusspec).

    If for some reason it is required to have configuration different from

    ```
    useIpv6: true
    useIpv4: true
    ```

    for the main Ytsaurus spec and at the same time `enable_ipv4=%true;enable_ipv6=%true` for the StrawberryController, it is possible to achieve that by using `configOverrides` ConfigMap with:

    ```
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

**Minor changes:**
- Add no more than one ytsaurus spec per namespace validation by [@qurname2](https://github.com/qurname2) in [#305](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/305).
- Add strategy, nodeSelector, affinity, tolerations by [@sgburtsev](https://github.com/sgburtsev) in [#321](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/321).
- Add forceTcp and keepSocket options by [@leo-astorsky](https://github.com/leo-astorsky) in [#324](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/324).

**Bugfixes:**

- Fix empty volumes array in sample config by [@koct9i](https://github.com/koct9i) in [#318](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/318).

**Contributors:**
- [@koct9i](https://github.com/koct9i), [@qurname2](https://github.com/qurname2), [@leo-astorsky](https://github.com/leo-astorsky), [@sgburtsev](https://github.com/sgburtsev)

{% endcut %}


{% cut "**0.13.1**" %}

**Bugfixes:**
- Revert deprecation of useInsecureCookies in [#310](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/310). by [@sgburtsev](https://github.com/sgburtsev) in [#317](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/317).
- The field useInsecureCookies was deprecated in the previous release in a not backwards compatible way, this release fixes it. It is now possible to configure the secureness of UI cookies (via the useInsecureCookies field) and the secureness of UI and HTTP proxy interaction (via the secure field) independently.

**Contributors:**
- [@sgburtsev](https://github.com/sgburtsev)

{% endcut %}


{% cut "**0.13.0**" %}

**Features:**
- Add per-component terminationGracePeriodSeconds by [@koct9i](https://github.com/koct9i) in [#304](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/304).
- Added externalProxy parameter for UI by [@sgburtsev](https://github.com/sgburtsev) in [#308](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/308).
- Size as Quantity in LogRotationPolicy by [@sgburtsev](https://github.com/sgburtsev) in [#309](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/309).
- Use `secure` instead of `useInsecureCookies`, pass caBundle to UI by [@sgburtsev](https://github.com/sgburtsev) in [#310](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/310).

**Minor changes:**
- Add all YTsaurus CRD into category "ytsaurus-all" "yt-all" by [@koct9i](https://github.com/koct9i) in [#311](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/311).

**Bugfixes:**
- Operator should detect configOverrides updates by [@l0kix2](https://github.com/l0kix2) in [#314](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/314).

**Contributors:**
- [@koct9i](https://github.com/koct9i), [@l0kix2](https://github.com/l0kix2), [@sgburtsev](https://github.com/sgburtsev)

{% endcut %}


{% cut "**0.12.0**" %}

**Features:**
- More options for store locations. by [@sgburtsev](https://github.com/sgburtsev) in [#294](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/294).
- data nodes upper limit for low_watermark increased from 5 to 25Gib.
- data nodes' `trash_cleanup_watermark` will be set equal to the lowWatermark value from spec
- `max_trash_ttl` can be configured in spec
- Add support for directDownload to UI Spec by [@kozubaeff](https://github.com/kozubaeff) in [#257](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/257).
- `directDownload` for UI can be configured in the spec now. If omitted or set to `true`, UI will have current default behaviour (use proxies for download), if set to `false` — UI backend will be used for downloading.

**Contributors:**
- [@kozubaeff](https://github.com/kozubaeff), [@sgburtsev](https://github.com/sgburtsev)

{% endcut %}


{% cut "**0.11.0**" %}

**Features:**
- SetHostnameAsFQDN option is added to all components. Default is true by [@qurname2](https://github.com/qurname2) in [#302](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/302).
- Add per-component option hostNetwork by [@koct9i](https://github.com/koct9i) in [#287](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/287).

**Minor changes:**
- Add option for per location disk space quota by [@koct9i](https://github.com/koct9i) in [#279](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/279).
- Add into exec node pods environment variables for CRI tools by [@koct9i](https://github.com/koct9i) in [#283](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/283).
- Add per-instance-group podLabels and podAnnotations by [@koct9i](https://github.com/koct9i) in [#289](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/289).
- Sort status conditions for better readability by [@koct9i](https://github.com/koct9i) in [#290](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/290).
- Add init containers for exec node by [@koct9i](https://github.com/koct9i) in [#288](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/288).
- Add loglevel "warning" by [@koct9i](https://github.com/koct9i) in [#292](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/292).
- Remove mutating/defaulter webhooks by [@koct9i](https://github.com/koct9i) in [#296](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/296).

**Bugfixes:**
- fix exec node resource calculation on non-isolated CRI-powered job environment by [@kruftik](https://github.com/kruftik) in [#277](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/277).

**Contributors:**
- [@koct9i](https://github.com/koct9i), [@kruftik](https://github.com/kruftik), [@qurname2](https://github.com/qurname2)

{% endcut %}

{% cut "**0.10.0**" %}

Minor
- Add everyone-share QT ACO by [@Krisha11](https://github.com/Krisha11) in [#272](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/272).
- Add channel in qt config by [@Krisha11](https://github.com/Krisha11) in [#273](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/273).
- Add option for per location disk space quota [#279](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/279).

**Bugfixes:**
- Fix exec node resource calculation on non-isolated CRI-powered job environment [#277](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/277).

**Contributors:**
- [@Krisha11](https://github.com/Krisha11)

{% endcut %}

{% cut "**0.9.1**" %}

**Minor changes:**

- Add 'physical_host' to `cypress_annotations` for CMS and UI сompatibility. [#252](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/252)
- Add `WATCH_NAMESPACE` env and `LeaderElectionNamespace`. [#168](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/168)
- Add configuration for solomon exporter: specify host and some instance tags. [#258](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/258)
- Add sidecars support to primary masters containers. [#259](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/259)
- Add option for containerd registry config path. [#264](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/264)

**Bugfixes:**

- Fix CRI job environment for remote exec nodes. [#261](https://github.com/ytsaurus/ytsaurus-k8s-operator/pull/261)

{% endcut %}

{% cut "**0.9.0**" %}

**Features:**
- Add experimental (behaviour may change) `UpdateSelector` field to be able to update components separately. [#211](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/211)

**Minor changes:**

- Enable TmpFS when possible. [#235](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/235)
- Disable disk quota for slot locations. [#236](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/236)
- Forward docker image environment variables to user job. [#248](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/248)

**Bugfixes:**
- Fix flag `doNotSetUserId`. [#243](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/243)

{% endcut %}

{% cut "**0.8.0**" %}

**Minor changes:**
- Increased default value for `MaxSnapshotCountToKeep` and `MaxChangelogCountToKeep`.
- Tune default bundle replication factor. [#210](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/210)
- Set `EnableServiceLinks=false` for all pods. [#218](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/218)

**Bugfixes:**
- Fix authentication configuration for RPC Proxy. [#207](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/207)
- Job script updated on restart. [#224](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/224)
- Use secure random and base64 for tokens. [#202](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/202)
- Fix running jobs with custom docker_image when default job image is not set. [#217](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/217)

{% endcut %}

{% cut "**0.7.0**" %}

**Features:**
- Add Remote exec nodes support. [#75](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/75)
- Add MasterCaches support. [#122](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/122)
- Enable TLS certificate auto-update for http proxies. [#167](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/167)
- CRI containerd job environment. [#105](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/105)

**Minor changes:**
- Support `RuntimeClassName` in InstanceSpec.
- Configurable monitoring port. [#146](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/146)
- Not triggering full update for data nodes update.
- Add `ALLOW_PASSWORD_AUTH` to UI. [#162](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/162)
- Readiness checks for strawberry & UI.
- Medium is called domestic medium now. [#88](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/88)
- Tune tablet changelog/snapshot initial replication factor according to data node count. [#185](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/185)
- Generate markdown API docs.
- Rename operations archive. [#116](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/116)
- Configure cluster to use jupyt. [#149](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/149)
- Fix QT ACOs creation on cluster update. [#176](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/176)
- Set ACLs for QT ACOs, add everyone-use ACO. [#181](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/181)
- Enable rpc proxy in job proxy. [#197](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/197)
- Add yqla token file in container. [#140](https://github.com/ytsaurus/ytsaurus-k8s-operator/issues/140)

**Bugfixes:**
- Replace YQL Agent default monitoring port 10029 -> 10019.

{% endcut %}

{% cut "**0.6.0**" %}

**Features:**
- Added support for updating masters of 23.2 versions.
- Added the ability to bind masters to the set of nodes by node hostnames.
- Added the ability to configure the number of stored snapshots and changelogs in master spec.
- Added the ability for users to create access control objects.
- Added support for volume mount with mountPropagation = Bidirectional mode in execNodes.
- Added access control object namespace "queries" and object "nobody". They are necessary for query\_tracker versions 0.0.5 and higher.
- Added support for the new Cliques CHYT UI.
- Added the creation of a group for admins (admins).
- Added readiness probes to component statefulset specs.
- Improved ACLs on master schemas.
- Master and scheduler init jobs do not overwrite existing dynamic configs anymore.
- `exec_agent` was renamed to `exec_node` in exec node config, if your specs have `configOverrides` please rename fields accordingly.

Fixes:
- Improved ACLs on master schemas.
- Master and scheduler init jobs do not overwrite existing dynamic configs anymore.

Tests:
- Added flow to run tests on Github resources
- Added e2e to check that updating from 23.1 to 23.2 works
- Added config generator tests for all components
- Added respect KIND_CLUSTER_NAME env variable in e2e tests
- Supported local k8s port forwarding in e2e

**Backward incompatible changes:**
- `exec_agent` was renamed to `exec_node` in exec node config. If your specs have configOverrides, please rename fields accordingly.

{% endcut %}

{% cut "**0.5.0**" %}

- added minReadyInstanceCount into Ytsaurus components which allows not to wait when all pods are ready;
- support queue agent;
- added postprocessing of generated static configs;
- introduced separate UseIPv4 option to allow dualstack configurations;
- support masters in host network mode;
- added spyt engine in query tracker by default;
- enabled both ipv4 and ipv6 by default in chyt controllers;
- default CHYT clique creates as tracked instead of untracked;
- don't run full update check if full update is not enabled (enable_full_update flag in spec);
- update cluster algorithm was improved. If full update is needed for already running components and new components was added, operator will run new components at first, and only then start full update. Previously such reconfiguration was not supported;
- added optional TLS support for native-rpc connections;
- added possibility to configure job proxy loggers.
- changed how node resource limits are calculated from resourceLimits and resourceRequests;
- enabled debug logs of YTsaurus go client for controller pod;
- supported dualstack clusters in YQL agent;
- supported new config format of YQL agent;
- supported NodePort specification for HTTP proxy (http, https), UI (http) and RPC proxy (rpc port). For TCP proxy NodePorts are used implicitly when NodePort service is chosen. Port range size and minPort are now customizable;
- fixed YQL agents on ipv6-only clusters;
- fixed deadlock in case when UI deployment is manually deleted.

{% endcut %}

{% cut "**0.4.1**" %}

- поддержали возможность переопределять статические конфиги per-instance, а не только на всю компоненту;
- поддержали TLS для RPC-проксей;
- починили падение оператора при поднятии CHYT-клики по-умолчанию - `ch_public`.

{% endcut %}

{% cut "**0.4.0**" %}

- архив операций теперь обновляется при изменении образа;
- возможность указать разные образы для разных компонент;
- поддержано обновление кластера без полного даунтайма для stateless компонент;
- поддержано обновление статических конфигов компонент при необходимости;
- улучшен SPYT-контроллер. Добавлен статус инициализации (`ReleaseStatus`);
- добавлен CHYT-контроллер и возможность загружать несколько разных версия на один кластер. Подробности можно узнать [по ссылке](../../admin-guide/install-additional-components/#chyt);
- добавлена возможность указать формат логов (`yson`, `json` или `plain_text`), а также возможность включить запись структурированных логов. Подробнее можно почитать на странице [Логирование](../../admin-guide/logging);
- добавлено больше диагностики о поднятии компонент в статусе `Ytsaurus`;
- добавлена возможность отключить запуск полного обновления (`enableFullUpdate`);
- поле `chyt` спеки переименовано в `strawberry`. Для обратной совместимости оно остается в `crd`, но рекомендуется его переименовать;
- размер `description` в `crd` теперь ограничивается 80 символами, что сильно уменьшило размер `crd`;
- таблицы состояния `Query Tracker` теперь автоматически мигрируются при его обновлениях;
- добавлена возможность задать привилегированный режим для контейнеров `exec node`;
- добавлена `TCP proxy`;
- добавлено больше валидации спеки, а именно проверки того, что пути в локациях принадлежат какому-то из вольюмов, а также некоторые проверки, что для каждой указанной компоненты есть все компоненты, необходимые для ее успешной работы;
- теперь `strawberry controller` и `ui` также можно обновлять;
- добавлена возможность поднимать http-proxy с TLS;
- можно указать адрес Odin сервиса для UI;
- добавлена возможность конфигурировать `tags` и `rack` для нод;
- поддержена конфигурация OAuth сервиса в спеке;
- добавлена возможность передавать дополнительные переменные окружение в UI, а также задавать тему и окружение (`testing`, `production` итп) для UI;
- медиумы локаций data node создаются автоматически при первичном разворачивании кластера.

{% endcut %}

{% cut "**0.3.1**" %}

- добавлена возможность настроить автоматическую ротацию логов;
- добавлена возможность указать `tolerations` и `node selector` в спеке инстансов компонент;
- исправлена ошибка в наименовании поля `medium_name` в статических конфигах;
- добавлен вызов `Reconcile` оператора при изменении созданных им объектов;
- ConfigMap-ы хранят данные в текстовом виде вместо бинарного, чтобы позволяет смотреть содержимое конфигов через `kubectl describe configmap <configmap-name>`;
- добавлено вычисление и установка `disk_usage_watermark` и `disk_quota` для `exec node`;
- добавлен SPYT-контроллер и возможность загружать в кипарис необходимое для `spyt` с помощью отдельного ресурса, что позволяет иметь несколько версий `SPYT` на одном кластере. Подробности можно узнать [на отдельной странице](../../admin-guide/install-additional-components/#spyt);

{% endcut %}