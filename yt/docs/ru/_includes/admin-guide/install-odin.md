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

Odin умеет отдавать метрики в формате Prometheus. Сервисы для мониторинга создаются по умолчанию (отключить можно, указав `metrics.enable`). По умолчанию ServiceMonitor не создается, но можно его включить, указав `metrics.serviceMonitor.enable`. Подробнее про настройку сбора метрик можно почитать в разделе [Мониторинг](../../admin-guide/monitoring.md).


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

## Обновление и удаление

**Обновление конфигурации:**

```bash
helm upgrade odin oci://ghcr.io/ytsaurus/odin-chart \
  --version {{odin-version}} \
  -f values.yaml \
  -n <namespace>
```

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
