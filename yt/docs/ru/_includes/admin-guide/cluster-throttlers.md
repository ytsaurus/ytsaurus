# Квотирование межкластерной сетевой полосы

Cluster throttlers — это механизм квотирования входящего межкластерного трафика на MR-кластерах {{product-name}}. Квотирование затрагивает операции [RemoteCopy](../../user-guide/data-processing/operations/remote-copy.md) и операции MapReduce с удалённым чтением входных таблиц.

Подробнее о том, как квотирование влияет на операции пользователей, см. в разделе [Квотирование межкластерной сетевой полосы](../../user-guide/data-processing/operations/cluster-throttlers.md).

## Механизм распределённого тротлера { #distributed-throttler }

На каждой `exec`-ноде кластера работает фабрика распределённых тротлеров. Каждый тротлер ограничивает входящий поток данных с конкретного удалённого кластера. Например, тротлер `bandwidth_remote` на кластере `local` ограничивает входящий поток с кластера `remote`.

Среди всех `exec`-нод gossip-подобным способом выбирается лидер. Лидер:
- Собирает состояние тротлеров со всех `exec`-нод.
- Ведёт учёт суммарного использования межкластерной полосы.
- Раздаёт `exec`-нодам их индивидуальные ограничения.
- Отправляет контроллер-агентам информацию о доступности межкластерных полос.

Если лидер недоступен, gossip-подобным способом выбирается новый лидер.

## Конфигурация { #configuration }

Конфигурация cluster throttlers хранится в узле `//sys/cluster_throttlers` дерева метаинформации кластера. Если нужно ограничивать скорость удалённого чтения с внешнего кластера, нужно добавить его в эту конфигурацию.

### Поля конфигурации { #config-fields }

- `enabled` — включить/выключить квотирование. При значении `%false` тротлеры не применяются.
- `update_period` — интервал (в мс) опроса узла `//sys/cluster_throttlers` для обновления конфигурации.
- `distributed_throttler` — настройки распределённого тротлера:
  - `limit_update_period` — как часто (в мс) `exec`-нода отправляет своё состояние лидеру и получает от него локальную квоту.
  - `leader_update_period` — как часто (в мс) `exec`-нода обновляет информацию о текущем лидере.
  - `local_throttlers_attribute_update_period` — как часто (в мс) обновляется атрибут `local_throttlers` в сервисе discovery (используется для интроспекции).
  - `throttler_expiration_time` — время (в мс), после которого лидер считает тротлер неактивным, если от него не поступало обновлений.
- `cluster_limits` — квоты для каждого удалённого кластера (ключ — имя кластера):
  - `bandwidth` — ограничение на входящую пропускную способность:
    - `limit` — максимальный поток в байтах в секунду.
  - `rps` — ограничение на количество запросов на чтение в секунду:
    - `limit` — максимальное число запросов в секунду.

### Пример конфигурации { #config-example }

```
{
    "enabled" = %true;
    "update_period" = 5000;
    "distributed_throttler" = {
        "leader_update_period" = 5000;
        "throttler_expiration_time" = 60000;
        "limit_update_period" = 1000;
        "local_throttlers_attribute_update_period" = 5000;
    };
    "cluster_limits" = {
        "remote_1" = {
            "bandwidth" = {
                "limit" = 549755813888;  // 512 GB/s
            };
        };
        "remote_2" = {
            "bandwidth" = {
                "limit" = 4294967296;  // 4 GB/s
            };
        };
    };
}
```

## Управление конфигурацией с помощью CLI { #cli }

Для управления конфигурацией cluster throttlers через CLI выполните следующие шаги:

1. Создайте файл конфигурации с нужными настройками (см. пример выше).
2. Создайте узел `//sys/cluster_throttlers` в дереве метаинформации кластера:
   ```bash
   yt create document //sys/cluster_throttlers
   ```
3. Запишите конфигурацию в созданный узел:
   ```bash
   yt set //sys/cluster_throttlers < config_file
   ```

{% if audience == "internal" %}
## Управление конфигурацией через ytdyncfgen { #ytdyncfgen }

Шаблоны конфигурации хранятся в репозитории по пути `yt/admin/ytdyncfgen/templates/components/cluster_throttlers/` — по одному yaml-файлу на каждый кластер.

Чтобы изменить конфигурацию тротлеров на кластере (например, `arnold`):

1. Отредактируйте файл `yt/admin/ytdyncfgen/templates/components/cluster_throttlers/arnold.yaml`.
2. Соберите `ytdyncfgen` и примените конфиг:
   ```bash
   ya make yt/admin/ytdyncfgen/bin -r
   ./yt/admin/ytdyncfgen/bin/ytdyncfgen -c arnold -s cluster_throttlers -p
   ```
3. Переканонизируйте тесты:
   ```bash
   ya make yt/admin/ytdyncfgen/tests -tAZ
   ```
4. Закоммитьте изменения и создайте PR:
   ```bash
   arc commit -m "Update cluster_throttlers on arnold" yt
   arc pr c
   ```
{% endif %}

## Интроспекция { #introspection }

### Состояние тротлеров на нодах { #throttler-state }

Состояние тротлеров на `exec`-нодах хранится в группе `remote_cluster_throttlers_group` сервиса discovery. Каждая нода публикует атрибут `local_throttlers` со следующими полями:

- `rate` — текущий поток в байтах в секунду.
- `limit` — квота в байтах в секунду, выданная лидером данной ноде.
- `queue_byte_size` — размер очереди в байтах.
- `quota_exceeded` — превышена ли квота.
- `period` — период обновления лимитов в миллисекундах.

Чтобы посмотреть состояние тротлеров на кластере `local` с помощью CLI, можно выполнить следующую команду на кластере `local`:

```bash
yt --proxy local list \
  "//sys/discovery_servers/<discovery-server>/orchid/discovery_server/remote_cluster_throttlers_group/@members" \
  --attribute local_throttlers --format json | jq -r '.[] | .["$attributes"]["local_throttlers"]'
```

Команда выведет состояние тротлеров для всех `exec`-нод кластера в виде:

```json
{
  "bandwidth_remote_1": {
    "rate": 19324406.5,
    "limit": 21496801.3,
    "queue_byte_size": 0,
    "quota_exceeded": false,
    "period": 1000
  },
  "bandwidth_remote_2": {
    "rate": 1832910.1,
    "limit": 1935680.6,
    "queue_byte_size": 0,
    "quota_exceeded": false,
    "period": 1000
  }
}
```

Чтобы посмотреть утилизацию сетевого канала на кластере `local` с кластера `remote` с помощью CLI, можно выполнить следующую команду на кластере `local`:

```
yt --proxy local list \
  "//sys/discovery_servers/<discovery-server>/orchid/discovery_server/remote_cluster_throttlers_group/@members" \
  --attribute local_throttlers --format json | jq '[.[] | ."$attributes"."local_throttlers"."bandwidth_remote"."rate"] | add'
```

### Проверка лидера { #leader-check }

Чтобы убедиться, что в группе тротлинга выбран единственный лидер, выполните команду:

```bash
yt list \
  "//sys/discovery_servers/<discovery-server>/orchid/discovery_server/remote_cluster_throttlers_group/@members" \
  --attribute leader_id --attribute address --format json \
  | jq -r '.[] | [.["$attributes"]["leader_id"], .["$attributes"]["address"]] | @tsv' \
  | cut -f 1 | sort -u
```

Если команда выводит более одного уникального `leader_id`, это означает split-brain — нужно разобраться с причиной (например, сетевая изоляция части нод).

{% if audience == "internal" %}
### Мониторинг { #monitoring }

В мониторинге есть дашборд [cluster-throttlers](https://monitoring.yandex-team.ru/projects/yt/dashboards/cluster-throttlers) со всеми кластерами.

Ключевые метрики тротлера:
- `queue_byte_size` — размер тротлер-очереди на `exec`-ноде. Рост этой метрики означает, что нода накапливает запросы быстрее, чем лидер выдаёт квоту.
- `queue_estimated_overrun_duration` — оценочное время (в секундах) обработки текущей тротлер-очереди при текущей квоте.
{% endif %}
