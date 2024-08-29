## Kubernetes operator

Is published as helm charts on [Github Packages](https://github.com/ytsaurus/ytsaurus-k8s-operator/pkgs/container/ytop-chart).

**Current release:** 0.6.0

**All releases:**

{% cut "**0.6.0**" %}

- added support for updating masters of 23.2 versions;
- added the ability to bind masters to the set of nodes by node hostnames;
- added the ability to configure the number of stored snapshots and changelogs in master spec;
- added the ability for users to create access control objects;
- added support for volume mount with mountPropagation = Bidirectional mode in execNodes;
- added access control object namespace "queries" and object "nobody". They are necessary for query\_tracker versions 0.0.5 and higher;
- added support for the new Cliques CHYT UI;
- added the creation of a group for admins (admins);
- added readiness probes to component statefulset specs;
- improved ACLs on master schemas;
- master and scheduler init jobs do not overwrite existing dynamic configs anymore;
- `exec_agent` was renamed to `exec_node` in exec node config, if your specs have `configOverrides` please rename fields accordingly.

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