# Установка Tutorial

## Описание

Tutorial — это набор данных и примеры запросов для обучения работе с {{product-name}}. Установка осуществляется с помощью helm-chart. Установка данного helm-chart создает на кластере {{product-name}} таблицы с демонстрационными данными и загружает примеры запросов в раздел Queries для изучения различных возможностей системы.

## Предварительные требования

На данном этапе у вас должны быть:

* Helm 3.x;
* запущенный кластер {{product-name}} и адрес HTTP прокси (`http_proxy`);
* специальный пользователь с выписанным для него токеном (см. раздел [Управление токенами](../../user-guide/storage/auth.md#token-management));
* работающие Query Tracker и YQL Agent.

> Типично, адрес http-proxy имеет шаблонный вид: http://http-proxies.<namespace>.svc.cluster.local

## Настройка

### Подготовка директорий

```bash
yt create map_node //home/tutorial
```

#### Выдача прав

Назначьте минимально необходимые права (ACL) для пользователя (команды предполагают, что пользователь имеет права на создание таблиц и выполнение запросов):

```bash
yt add-member robot-tutorial superusers
yt set //home/tutorial/@acl/end '{action=allow; subjects=[robot-tutorial]; permissions=[read; write; create; remove; mount]}'
yt set //sys/tablet_cell_bundles/sys/@acl/end '{subjects=[robot-tutorial];permissions=[use];action=allow}'
yt set //sys/accounts/sys/@acl/end '{action=allow; subjects=[robot-tutorial]; permissions=[use]}'
```

> Предполагается, что вы создали пользователя `robot-tutorial`.

#### Создание Kubernetes Secret с токеном

Создайте секрет с токеном для доступа к YT:

```bash
kubectl create secret generic yt-tutorial-secret \
  --from-literal=yt-token="<yt**-****-****************>" \
  -n <namespace>
```

> По умолчанию чарт ожидает секрет с именем `yt-tutorial-secret` и ключом `yt-token`. Эти значения можно изменить в `values.yaml` (см. секцию `tutorial.secret`) или переопределить через `--set`.

## Установка Helm‑чарта

Типично, установку можно осуществить так:
```bash
helm install ytsaurus-tutorial oci://ghcr.io/ytsaurus/tutorial-chart \
     --version {{tutorial-version}} \
     --set tutorial.args.proxy="http://http-proxies.<namespace>.svc.cluster.local" \
     -n <namespace>
```

Список всех доступных параметров для переопределения как через `--set`, так и через `values.yaml`

```yaml
tutorial:
  secret:
    name: "yt-tutorial-secret"
    value: "yt-token"
  args:
    proxy: "http-proxies.default.svc.cluster.local"
    ytDirectory: "//home/tutorial"
    nomenclatureCount: 5000
    daysToGenerate: 14
    desiredOrderSize: 3000
    force: true
```

**Пояснения к параметрам:**

* `tutorial.secret.name` — имя Kubernetes Secret с токеном доступа к YT;
* `tutorial.secret.value` — ключ в секрете, содержащий токен;
* `tutorial.args.proxy` — адрес HTTP‑прокси {{product-name}} (внутри кластера Kubernetes или внешний адрес);
* `tutorial.args.ytDirectory` — директория в {{product-name}}, где будут созданы таблицы туториала (по умолчанию `//home/tutorial`);
* `tutorial.args.nomenclatureCount` — количество записей для генерации (по умолчанию `5000`). Влияет на размер таблицы `nomenclature`;
* `tutorial.args.daysToGenerate` — количество дней данных для генерации в таблицах `prices` и `orders` (по умолчанию `14`);
* `tutorial.args.desiredOrderSize` — желаемое количество строк в таблице `orders` (по умолчанию `3000`);
* `tutorial.args.force` — принудительная перезапись существующих таблиц (по умолчанию `true`).

## Проверки после установки

1. **Статус Init Job:**

```bash
kubectl get jobs -n <namespace> | grep tutorial
```

2. **Логи Init Job:**

```bash
kubectl logs job/ytsaurus-tutorial-tutorial-chart-init -n <namespace> --tail=200
```

3. **Проверка созданных таблиц в {{product-name}}:**

```bash
yt list //home/tutorial
```

## Обновление и удаление

**Обновление конфигурации:**

```bash
helm upgrade ytsaurus-tutorial oci://ghcr.io/ytsaurus/tutorial-chart \
  --version <new_version> \
  -f values.yaml \
  -n <namespace>
```

> При обновлении Init Job будет запущена заново (если `force: true`, существующие таблицы будут перезаписаны).

**Удаление релиза:**

```bash
helm uninstall ytsaurus-tutorial -n <namespace>
```

> **Внимание:** Удаление Helm-чарта не удаляет созданные таблицы в {{product-name}}. Если необходимо удалить данные туториала, выполните:

```bash
yt remove //home/tutorial
```

## Типичные ошибки и диагностика

* **Неверный адрес `proxy`:** ошибки соединения в логах Init Job. Проверьте DNS‑имя сервиса, namespace и доступность HTTP‑прокси {{product-name}};
* **Проблемы с токеном:** 401/403 в логах. Убедитесь, что переменная окружения ссылается на корректный ключ из секрета и ACL выданы пользователю-роботу;
* **Недостаточные ACL:** операции `create/remove` завершаются ошибкой. Пересмотрите выдачу прав на `//home/tutorial`;
* **Директория не пуста:** если `force: false` и директория `ytDirectory` не пуста, Init Job завершится с ошибкой. Установите `force: true` или очистите директорию вручную.

## Пример быстрой самопроверки токена вне Helm

```bash
curl -sS -H "Authorization: OAuth $YT_TOKEN" http://http-proxies.default.svc.cluster.local/auth/whoami
```

## Примечания по безопасности

* Ограничивайте права пользователя-робота принципом наименьших привилегий; выдавайте права только на необходимые директории;
* Ротируйте токены по внутренним политикам и своевременно обновляйте секреты (через `kubectl apply -f` или `kubectl create secret ... --dry-run=client -o yaml | kubectl apply -f -`).
