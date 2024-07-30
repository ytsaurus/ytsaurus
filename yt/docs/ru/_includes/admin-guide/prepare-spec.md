# Подготовка спецификации {{product-name}}

Пример минимальной спецификации можно найти [по ссылке](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/config/samples/cluster_v1_demo.yaml).

{% cut "Пример спецификации" %}

```yaml
apiVersion: cluster.ytsaurus.tech/v1
kind: Ytsaurus
metadata:
  name: ytdemo
spec:
  coreImage: ghcr.io/ytsaurus/ytsaurus:stable-{{yt-server-version}}-relwithdebinfo
  uiImage: ghcr.io/ytsaurus/ui:stable

  adminCredentials:
    name: ytadminsec

  discovery:
    instanceCount: 1

  primaryMasters:
    instanceCount: 3
    cellTag: 1
    volumeMounts:
      - name: master-data
        mountPath: /yt/master-data
    locations:
      - locationType: MasterChangelogs
        path: /yt/master-data/master-changelogs
      - locationType: MasterSnapshots
        path: /yt/master-data/master-snapshots

    volumeClaimTemplates:
      - metadata:
          name: master-data
        spec:
          accessModes: [ "ReadWriteOnce" ]
          resources:
            requests:
              storage: 20Gi

  httpProxies:
    - serviceType: NodePort
      instanceCount: 3

  rpcProxies:
    - serviceType: LoadBalancer
      instanceCount: 3

  dataNodes:
    - instanceCount: 3
      volumeMounts:
        - name: node-data
          mountPath: /yt/node-data

      locations:
        - locationType: ChunkStore
          path: /yt/node-data/chunk-store

      volumeClaimTemplates:
        - metadata:
            name: node-data
          spec:
            accessModes: [ "ReadWriteOnce" ]
            resources:
              requests:
                storage: 50Gi

  execNodes:
    - instanceCount: 3
      resources:
        limits:
          cpu: 3
          memory: 5Gi

      volumeMounts:
        - name: node-data
          mountPath: /yt/node-data

      volumes:
        - name: node-data
          emptyDir:
            sizeLimit: 40Gi

      locations:
        - locationType: ChunkCache
          path: /yt/node-data/chunk-cache
        - locationType: Slots
          path: /yt/node-data/slots

  tabletNodes:
    - instanceCount: 3

  queryTrackers:
    instanceCount: 1

  yqlAgents:
    instanceCount: 1

  schedulers:
    instanceCount: 1

  controllerAgents:
    instanceCount: 1

  ui:
    serviceType: NodePort
    instanceCount: 1
```

{% endcut %}

В таблице 1 приведены некоторые общие настройки `Ytsaurus`. Полное описание: [YtsaurusSpec](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/docs/api.md#ytsaurusspec).

<small>Таблица 1 — Базовые поля спецификации `Ytsaurus` </small>

| **Поле**            | **Тип**         | **Описание**                                                 |
| ------------------- | --------------- | ------------------------------------------------------------ |
| `coreImage`         | `string`        | Образ для основных серверных компонент, например, `ghcr.io/ytsaurus/ytsaurus:stable-{{yt-server-version}}-relwithdebinfo`. |
| `uiImage`         | `string` | Образ для UI, например, `ghcr.io/ytsaurus/ui:stable`. |
| `imagePullSecrets` | `array<LocalObjectReference>` | Секреты, необходимые для скачивания образов из private registry. Подробности можно узнать [по ссылке](https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry/). |
| `configOverrides`  | `optional<LocalObjectReference>` | Конфигмапа для переопределения генерируемых статических конфигов. Нужно использовать только в редких случаях. |
| `adminCredentials` | `optional<LocalObjectReference>` | Секрет с логином/паролем для админского аккаунта. |
| `isManaged`         | `bool` | Флаг, позволяющий отключить все действия оператора над данным кластером, чтобы совершить с кластерам ручные действия при необходимости. |
| `enableFullUpdate` | `bool` | Флаг, позволяющий запретить запуск полного обновления кластера. |
| `useIpv6` | `bool` | Использовать IPv6 или IPv4 |
| `bootstrap` | BootstrapSpec | Настройки для первичного поднятия кластера, например, параметры [таблет-селл бандлов](../../user-guide/dynamic-tables/concepts#tablet_cell_bundles) |

## Выбор набора компонент

Кластер можно поднимать с различными наборами компонент. Рассмотрим кратко, какие компоненты можно настроить в спецификации `Ytsaurus`.

Подробнее про компоненты можно прочитать [в отдельном разделе](../../admin-guide/components.md).

Как минимум в кластере должны быть мастера и discovery-сервисы, они настраиваются в полях `primaryMasters` и `discovery` соответственно.

Для запуска операций необходимы планировщики и контроллер-агенты, которые настраиваются соответственно в полях `schedulers` и `controllerAgents`.

Для выполнения запросов к кластеру из `cli` и различных `SDK` необходимы прокси. Прокси бывают двух типов: `HTTP` и `RPC`. Прокси настраиваются соответственно в полях `httpProxies` и `rpcProxies`.

Для того чтобы иметь удобный UI для работы с кластером, необходимо настроить его в поле `ui`.

Для хранения данных используются `dataNodes`, а для запуска джобов операций — `execNodes`.

Если планируется задавать запросы к данным с помощью SQL-like [языка запросов](../../yql), необходимо добавить в спецификацию `queryTrackers` и `yqlAgents`.

Для использования [CHYT](../../user-guide/data-processing/chyt/about-chyt) необходимо запустить специальный контроллер. Контроллер конфигурируется в поле `strawberry`.

Для работы динамических таблиц (которые необходимы в том числе для системных таблиц некоторых компонент, например, для query tracker-а), необходимо поднять `tabletNodes`.

## Докер-образ

На первом шаге необходимо выбрать основной docker-образ для серверных компонент.

Большинство серверных компонент релизятся из отдельной (релизной) ветки. На данный момент последняя стабильная ветка — `stable/{{yt-stable-branch}}`. Настоятельно рекомендуется использовать образ, собранный из стабильной релизной ветки.

Докер-образ, собранный из релизной ветки, имеет вид `ghcr.io/ytsaurus/ytsaurus:stable-{{yt-stable-branch}}.N` или `ghcr.io/ytsaurus/ytsaurus:stable-{{yt-stable-branch}}.N-relwithdebinfo`. Отличия указанных образов в том, что во втором образе все бинарные файлы собраны с debug-символами.

В случае падения серверных компонент в stderr компоненты будет напечатан stacktrace, а также на k8s-ноде будет отложен `coredump` (если это настроено в вашем k8s-кластере). Такие меры позволят понять, что именно произошло с компонентой. По этой причине  рекомендуется использовать образы `relwithdebinfo`, несмотря на то что они занимают больше места. Без debug-символов команда {{product-name}} скорее всего не сможет помочь вам в случае проблем.

В приведённом образе есть всё необходимое для практически всех компонент. Для компонент, не входящих в основной docker-образ, выкладываются отдельные образы. В Таблице 2 приведены рекомендуемые образы для всех компонент.

Образ каждой компоненты берется в первую очередь из поля `image` компоненты. Если образ не указан, то берется `coreImage` с верхнего уровня спецификации.


<small>Таблица 2 — Образы компонент </small>

| **Поле**            | **Docker-репозиторий** | **Рекомендуемый тег стабильного релиза** |
| ------------------- | --------------- | ----------------------------- |
| `discovery` | [ghcr.io/ytsaurus/ytsaurus](https://github.com/ytsaurus/ytsaurus/pkgs/container/ytsaurus) | `stable-{{yt-server-version}}-relwithdebinfo`      |
| `primaryMasters` | [ghcr.io/ytsaurus/ytsaurus](https://github.com/ytsaurus/ytsaurus/pkgs/container/ytsaurus) | `stable-{{yt-server-version}}-relwithdebinfo` |
| `httpProxies` | [ghcr.io/ytsaurus/ytsaurus](https://github.com/ytsaurus/ytsaurus/pkgs/container/ytsaurus) | `stable-{{yt-server-version}}-relwithdebinfo` |
| `rpcProxies` | [ghcr.io/ytsaurus/ytsaurus](https://github.com/ytsaurus/ytsaurus/pkgs/container/ytsaurus) | `stable-{{yt-server-version}}-relwithdebinfo` |
| `dataNodes` | [ghcr.io/ytsaurus/ytsaurus](https://github.com/ytsaurus/ytsaurus/pkgs/container/ytsaurus) | `stable-{{yt-server-version}}-relwithdebinfo` |
| `execNodes` | [ghcr.io/ytsaurus/ytsaurus](https://github.com/ytsaurus/ytsaurus/pkgs/container/ytsaurus) | `stable-{{yt-server-version}}-relwithdebinfo` |
| `tabletNodes` | [ghcr.io/ytsaurus/ytsaurus](https://github.com/ytsaurus/ytsaurus/pkgs/container/ytsaurus) | `stable-{{yt-server-version}}-relwithdebinfo` |
| `schedulers` | [ghcr.io/ytsaurus/ytsaurus](https://github.com/ytsaurus/ytsaurus/pkgs/container/ytsaurus) | `stable-{{yt-server-version}}-relwithdebinfo`  |
| `controllerAgents` | [ghcr.io/ytsaurus/ytsaurus](https://github.com/ytsaurus/ytsaurus/pkgs/container/ytsaurus) | `stable-{{yt-server-version}}-relwithdebinfo` |
| `queryTrackers` | [ghcr.io/ytsaurus/query-tracker](https://github.com/ytsaurus/ytsaurus/pkgs/container/query-tracker) | `{{qt-version}}-relwithdebinfo` |
| `yqlAgents` | [ghcr.io/ytsaurus/query-tracker](https://github.com/ytsaurus/ytsaurus/pkgs/container/query-tracker) | `{{qt-version}}` |
| `strawberry` | [ghcr.io/ytsaurus/strawberry](https://github.com/ytsaurus/ytsaurus/pkgs/container/strawberry) | `{{strawberry-version}}` |
| `ui` | [ghcr.io/ytsaurus/ui](https://github.com/ytsaurus/ytsaurus-ui/pkgs/container/ui) | `stable` |

Помимо указанных образов выкладывается общий образ для всех серверных компонент сразу (кроме `ui`). Такой образ достаточно указать один раз в `coreImage`, не указывая ничего в поле `image` компонент явно.

## Логирование
Корректная настройка логирования очень важна для диагностики проблем и при обращениях в поддержку. Рекомендации по настройке логирования собраны на отдельной [странице](../../admin-guide/logging.md).

## Локации

Рекомендации по разметке дисков и конфигурации локаций собраны на отдельной [странице](../../admin-guide/locations.md).

## Среда исполнения операций

`Exec Nodes` могут запускать джобы в изолированных контейнерах для обработки опции операции `docker_image`. Требуемые настройки заключены в секциях `jobResources` и `jobEnvironment` в [ExecNodeSpec](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/docs/api.md#execnodesspec). Пример [настройки кластера](https://github.com/ytsaurus/yt-k8s-operator/blob/main/config/samples/cluster_v1_cri.yaml).

## Настройка таблет-селл бандлов

Оператор автоматически создает несколько [таблет-селл бандлов](../../user-guide/dynamic-tables/concepts#tablet_cell_bundles) — `sys` и `default`.

Для таблет-селл бандлов можно настроить медиумы, где будут храниться журналы и снепшоты. По умолчанию журналы и снепшоты хранятся в медиуме `default`.

Рекомендуется настраивать бандлы так, чтобы журналы и снепшоты хранились на `SSD`, иначе бандлы могут прийти в нерабочее состояние.

Для уже созданных бандлов можно установить атрибуты `@options/snapshot_primary_medium` и `@options/changelog_primary_medium`:

```bash
yt set //sys/tablet_cell_bundles/<bundle-name>/@options/snapshot_primary_medium '<medium-name>'
yt set //sys/tablet_cell_bundles/<bundle-name>/@options/changelog_primary_medium '<medium-name>'
```

При инициализации кластера оператор может настроить медиумы для бандлов автоматически. Для настройки бандла укажите названия медиумов в секции `bootstrap` на верхнем уровне спецификации. В той же секции можно указать количество таблет-селлов в бандле. После инициализации кластера количество таблет-селлов можно изменить, установив атрибут `//sys/tablet_cell_bundles/<bundle-name>/@tablet_cell_count`.

Пример `bootstrap` секции:

```yaml
bootstrap:
    tabletCellBundles:
        sys:
            snapshotMedium: ssd_medium
            changelogMedium: ssd_medium
            tabletCellCount: 3
        default:
            snapshotMedium: ssd_medium
            changelogMedium: ssd_medium
            tabletCellCount: 5
```

После разворачивания кластера оператор не будет обрабатывать изменения в поле `bootstrap`. Дальнейшую настройку необходимо выполнять вручную с помощью указанных атрибутов.
