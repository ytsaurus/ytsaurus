# Установка Odin

## Описание

Odin - сервис, осуществляющий качественный мониторинг работы {{product-name}}-кластера. Подробнее можно почитать в разделе [Мониторинг](../../admin-guide/monitoring.md#odin).

## Предварительные требования

На данном этапе у вас должны быть:

* Helm 3.x;
* запущенный кластер {{product-name}} и внутренний адрес HTTP прокси;
* специальный пользователь-робот `robot-odin` с выписанным для него токеном (см. раздел [Управление токенами](../../user-guide/storage/auth.md#token-management)):

  ```
  yt create user --attr "{name=robot-odin}"
  yt issue-token robot-odin
  ```

## Настройка

При желании Odin может быть развернут для мониторинга сразу нескольких кластеров. В таком случае нужно выбрать один из кластеров как основной, где Odin будет хранить свое состояние, а на каждом кластере создать соответствующего пользователя.

#### Выдача прав

Назначьте минимально необходимые права (ACL) для пользователя-робота (команды предполагают, что пользователь - это `robot-odin`):

```bash
yt set //sys/@acl/end '{action=allow; subjects=[robot-odin]; permissions=[read; write; create; remove; mount]}'
yt set //sys/accounts/sys/@acl/end '{action=allow; subjects=[robot-odin]; permissions=[use]}'
yt set //sys/tablet_cell_bundles/sys/@acl/end '{subjects=[robot-odin];permissions=[use];action=allow}'
yt set //sys/operations/@acl/end '{subjects=[robot-odin];permissions=[read];action=allow}'
```

#### Создание Kubernetes Secret с токенами

Создайте секрет с токенами для доступа Odin к YT.

```bash
kubectl create secret generic odin-secrets \
  --from-literal=YT_TOKEN="<robot-odin-token>" \
  -n <namespace>
```

#### Подготовка `values.yaml` {#prepare-values}

Минимальный пример конфигурации Odin для подключения к {{product-name}} через HTTP‑прокси и публикации веб‑сервиса Одина на порту 9002:

```yaml
config:
  odin:
    db:
      proxy: "http://http-proxies.default.svc.cluster.local"
    clusters:
      - proxy: "http://http-proxies.default.svc.cluster.local"
        cluster: minisaurus
        tokenEnvVariable: "YT_TOKEN"

webservice:
  service:
    port: 9002
```

**Пояснения к параметрам:**

* `config.odin.db.proxy` — адрес HTTP‑прокси {{product-name}}, где Odin будет хранить свое состояние;
* `config.odin.clusters[]` — список {{product-name}}‑кластеров, которые Odin мониторит. Для каждого:

  * `proxy` — адрес HTTP‑прокси внутри кластера Kubernetes (или внешний адрес, если вне кластера);
  * `cluster` — имя кластера {{product-name}};
  * `tokenEnvVariable` — имя переменной окружения в контейнере, откуда будет прочитан токен (см. секреты выше);
* `webservice.service.port` — порт веб-сервиса Odin.

> Проверьте корректность DNS‑имён сервисов: `http-proxies.default.svc.cluster.local` — пример для сервиса `http-proxies` в пространстве имен `default`.

По умолчанию, будет запускаться Init Job, создающая нужные таблицы для хранения состояния. Отключить ее запуск можно, указав `config.odin.db.initialize: false`.

Odin умеет отдавать метрики в формате Prometheus. По умолчанию создаются сервисы, на которых публикуются метрики (отключить можно, указав `metrics.enable: false`), и `ServiceMonitor`, по которому Prometheus Operator автоматически собирает эти метрики (отключить можно, указав `metrics.serviceMonitor.enable: false`). Список метрик и проверок Odin приведён в разделе [Мониторинг](../../admin-guide/monitoring.md). Помимо сбора метрик, чарт умеет генерировать на их основе правила алертинга (`PrometheusRule`) — см. раздел [Алертинг (Prometheus)](#alerting).


## Установка Helm‑чарта

```bash
helm install odin oci://ghcr.io/ytsaurus/odin-chart \
  --version {{odin-version}} \
  -f values.yaml \
  -n <namespace>
```

Helm-chart сначала запустит Init Job, которая создаст на кластере необходимую таблице для хранения там состояния Odin. Затем поднимет два deployment - с самим Odin и с web-сервисом для него.

## Проверки после установки

1. **Статус ресурсов:**

```bash
kubectl get pods,svc,deploy,cm,secret -n <namespace> | grep -i odin
```

2. **Логи пода:**

```bash
kubectl logs deploy/odin-odin-chart -n <namespace> --tail=200
```

## Настройка UI

В интерфейсе {{product-name}} есть страница, на которой можно смотреть результаты выполнения проверок Odin. Чтобы она появилась, необходимо указать адрес веб-сервиса Odin в конфиге UI.

UI должен быть установлен как helm-chart (см. [инструкцию по установке](../../admin-guide/install-ytsaurus#ui)).

Адрес веб-сервиса нужно указать в values.yaml в поле `.settings.odinBaseUrl`. Пример адреса, когда odin установлен в неймспейсе `default` и поднять на порту 9002 (это порт по-умолчанию): `"http://odin-odin-chart-web.default.svc.cluster.local:9002"`.


## Как включать и отключать проверки

Список проверок задаётся в секции `config.checks` в `values.yaml`.
Каждая проверка описывается структурой следующего вида:

```yaml
sort_result:
  displayName: Sort Result
  enable: true
  config: {...}
```

- `enable: true` — проверка будет добавлена в итоговую конфигурацию и выполняться Odin;
- `enable: false` — проверка будет пропущена (не попадёт в `checks/config.json`).

> В некоторых проверках встречается дополнительный флаг `config.enable`.
> Он управляет логикой внутри самой проверки. Даже если `enable: true`, но `config.enable: false`, то проверка попадёт в конфигурацию и будет видна в интерфейсе Odin, но запускаться она не будет.

#### Пример: отключение проверки

```yaml
suspicious_jobs:
  displayName: Suspicious Jobs
  enable: false
  config:
    options:
      critical_suspicious_job_inactivity_timeout: 420
```

В этом случае проверка **Suspicious Jobs** полностью исключается из конфигурации.

### Пример: частичное отключение через `config.enable`

```yaml
operations_snapshots:
  displayName: Operations Snapshots
  enable: true
  config:
    enable: false
    options:
      critical_time_without_snapshot_threshold: 3600
```

Здесь проверка попадёт в конфигурацию, но сам запуск будет отключен внутри Odin.

## Алертинг (Prometheus) {#alerting}

Помимо сбора метрик, начиная с версии 0.0.10, чарт умеет создавать ресурс `PrometheusRule` (требуется установленный [Prometheus Operator](https://github.com/prometheus-operator/prometheus-operator)) с правилами алертинга, построенными на основе метрик проверок Odin. По умолчанию эта возможность отключена (`rule.enable: false`). Для её работы необходимо, чтобы был включён сбор метрик (`metrics.enable: true`).

#### Включение

```yaml
rule:
  enable: true
  # Метки, по которым Prometheus Operator подхватывает правило.
  labels:
    release: kube-prom
    yt_alerts: "true"
  # Имя группы правил.
  groupName: odin.checks
```

#### Какие алерты генерируются

Для каждой включённой проверки создаётся до трёх алертов на основе её состояния:

- `OdinCheck<Name>Failed` — проверка завершилась с ошибкой (состояние `0`, `2` или `3`);
- `OdinCheck<Name>Partial` — проверка частично доступна (состояние `0.5`);
- `OdinCheck<Name>Absent` — метрика проверки отсутствует. Создаётся отдельно для каждого кластера из `config.odin.clusters`.

#### Настройка по умолчанию (`rule.defaults`)

Параметры всех алертов задаются в секции `rule.defaults`. Ниже приведены значения по умолчанию:

```yaml
rule:
  defaults:
    # Время, в течение которого условие должно выполняться, прежде чем алерт сработает.
    failureFor: 5m   # для алертов *Failed
    partialFor: 5m   # для алертов *Partial
    absentFor: 3m    # для алертов *Absent
    # Уровень важности (метка severity у алерта).
    severityFail: critical
    severityPartial: warning
    severityAbsent: warning
    # Дополнительные метки, добавляемые ко всем алертам.
    extraLabels: {}
    # Шаблоны заголовка (summary) и описания (description) алертов.
    summaryFail: 'Odin check "{{ .displayName }}" failed on cluster {{`{{ $labels.cluster }}`}}'
    summaryPartial: 'Odin check "{{ .displayName }}" is partially available on cluster {{`{{ $labels.cluster }}`}}'
    summaryAbsent: 'Odin is not reporting check "{{ .displayName }}" on cluster {{ .cluster }}'
    descriptionFail: |
      yt_odin_{{ .name }} state is {{`{{ $value }}`}} on cluster {{`{{ $labels.cluster }}`}}.
    descriptionPartial: |
      yt_odin_{{ .name }} state is 0.5 on cluster {{`{{ $labels.cluster }}`}}.
    descriptionAbsent: |
      No samples for yt_odin_{{ .name }}{cluster="{{ .cluster }}"}.
```

В шаблонах `summary*` / `description*` доступны переменные чарта:

- `{{ .name }}` — техническое имя проверки (например, `scheduler`);
- `{{ .displayName }}` — отображаемое имя проверки (`displayName`);
- `{{ .cluster }}` — имя кластера (доступно только в шаблонах `*Absent`).

Конструкции вида `{{`{{ $labels.cluster }}`}}` и `{{`{{ $value }}`}}` экранируются и попадают в итоговый `PrometheusRule` как есть — их во время алертинга подставляет уже сам Prometheus.

#### Переопределение для конкретной проверки (`<check>.alert`)

Любой параметр из `rule.defaults` можно переопределить в блоке `alert` конкретной проверки, а также полностью или частично отключить её алерты:

```yaml
config:
  checks:
    scheduler:
      displayName: Scheduler
      enable: true
      alert:
        # Переопределение значений по умолчанию для этой проверки.
        failureFor: 1m
        partialFor: 1m
        absentFor: 5m
        severityFail: critical
        severityPartial: warning
        severityAbsent: warning
        extraLabels: {}
        # Переопределение шаблонов текстов (необязательно).
        summaryFail: "Scheduler is down on {{`{{ $labels.cluster }}`}}"
        # аналогично summaryPartial / summaryAbsent / descriptionFail / descriptionPartial / descriptionAbsent.
    lost_vital_chunks:
      displayName: Lost Vital Chunks
      enable: true
      alert:
        failureFor: 1m
        # Флаги отключения отдельных алертов:
        disable: false          # true — отключить все алерты проверки
        disableFail: false      # true — не создавать алерт *Failed
        disablePartial: false   # true — не создавать алерт *Partial
        disableAbsent: true     # true — не создавать алерты *Absent
```

Значения из блока `config.checks.<check>.alert` имеют приоритет над `rule.defaults`. Полный перечень параметров и их значения по умолчанию смотрите в [`values.yaml`](https://github.com/ytsaurus/ytsaurus/blob/main/yt/docker/charts/odin-chart/values.yaml).

## Обновление и удаление

**Обновление конфигурации:**

```bash
helm upgrade odin oci://ghcr.io/ytsaurus/odin-chart \
  --version {{odin-version}} \
  -f values.yaml \
  -n <namespace>
```

> Мы не рекомендуем использовать флаг `--reuse-values` при обновлении релиза. Этот флаг сохраняет старые значения параметров (в частности, `$Values.image.tag`) из предыдущих версий чарта, что может привести к неопределенному поведению, конфликтам версий и непредвиденным ошибкам в работе. Всегда явно передавайте актуальный файл конфигурации с помощью `-f` или `--set`.

**Удаление релиза:**

```bash
helm uninstall odin -n <namespace>
```

## Типичные ошибки и диагностика

* **Неверный адрес `proxy`:** ошибки соединения/авторизации в логах Odin. Проверьте DNS‑имя сервиса, пространство имен и доступность HTTP‑прокси {{product-name}};
* **Проблемы с токеном:** 401/403 в логах Odin. Убедитесь, что переменная окружения ссылается на корректный ключ из секрета и ACL выданы пользователю `robot-odin`;
* **Недостаточные ACL:** операции `create/remove/mount` завершаются ошибкой. Пересмотрите выдачу прав на `//sys` и/или нужные директории/таблицы;
* **Конфликт порта сервиса:** при экспонировании через Ingress/NodePort проверьте, что порт `9002` свободен и соответствующие ресурсы созданы.

## Пример быстрой самопроверки токена вне Helm

```bash
curl -sS -H "Authorization: OAuth $YT_TOKEN" http://http-proxies.default.svc.cluster.local/auth/whoami
```

## Примечания по безопасности

* Ограничивайте права пользователя `robot-odin` принципом наименьших привилегий; при необходимости создайте отдельные ACL на целевые пути.
* Ротируйте токены по внутренним политикам и своевременно обновляйте секреты (через `kubectl apply -f` или `kubectl create secret ... --dry-run=client -o yaml | kubectl apply -f -`).
