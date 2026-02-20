# Установка Task-прокси

## Описание {#task-proxy-desc}

Task-прокси предоставляет доступ к веб-сервисам, развёрнутым в операциях {{product-name}}. Подробнее можно почитать в разделе [Task-прокси](../../user-guide/proxy/task.md).

## Предварительные требования

На данном этапе у вас должны быть:

- Helm 3.x;
- запущенный кластер {{product-name}} и внутренний адрес HTTP прокси;
- специальный пользователь-робот `robot-task-proxy` с выписанным для него токеном (см. раздел [Управление токенами](../../user-guide/storage/auth.md#token-management)):
   ```sh
   yt create user --attr "{name=robot-task-proxy}"
   yt issue-token robot-task-proxy > robot-task-proxy-token
   ```

## Настройка {#setup}

### Выдача прав {#permissions}

Назначьте минимально необходимые права (ACL) для пользователя-робота (команды предполагают, что пользователь — это `robot-task-proxy`):

```sh
yt set //sys/operations/@acl/end '{subjects=[robot-task-proxy];permissions=[read];action=allow}'
yt set //sys/accounts/sys/@acl/end '{action=allow; subjects=[robot-task-proxy]; permissions=[use]}'

yt create map_node //sys/task_proxies
yt set //sys/task_proxies/@acl/end '{action=allow; subjects=[robot-task-proxy]; permissions=[read; write; create; remove;]}'
```

### Создание Kubernetes Secret с токенами {#create-secret}

Создайте секрет с токенами для доступа Task-прокси к {{product-name}}.

```bash
kubectl create secret generic task-proxy-token \
    --from-file robot-task-proxy-token \
    -n ${NAMESPACE}
```

### Подготовка `values.yaml` {#prepare-values}

Пример конфигурации Task-прокси:

```yaml
baseDomain: my-cluster.ytsaurus.example.net
replicas: 2
proxy:
  resources:
    requests:
      cpu: "1"
      memory: 1Gi
server:
  resources:
    requests:
      cpu: "1"
      memory: 1Gi
nodeSelector:
  yt-group: tp
```

**Пояснения к параметрам:**

- Основные параметры:
  - `dirPath` – путь к директории Task-прокси в Кипарисе. По умолчанию путь к таблице с данными сервисов будет `//sys/task_proxies/services`.
  - `baseDomain` – домены сервисов используют данный как базовый, добавляя хэш сервиса, например, `645236d8.my-cluster.ytsaurus.example.net` для примера конфигурации выше.
  - `tokenSecretRef` – имя k8s секрета с токеном для доступа к Task-прокси, по умолчанию имя – `task-proxy-token`.
  - `discoveryPeriodSeconds` – периодичность запуска обнаружения сервисов (task discovery) в секундах.
- Параметры безопасности:
  - `auth`
    - `auth.enabled` – включить контроль доступа к операции
    - `auth.cookieName` – имя куки аутентификации, по умолчанию `YTCypressCookie`
  - `tls`
    - `tls.enabled` – включить TLS для Task-прокси
    - `tls.certSecretRef` – имя k8s секрета с TLS сертификатом
- Параметры развертывания:
  - Task-прокси состоит из двух приложений, расположенных в одном поде:
    - [Envoy](https://www.envoyproxy.io/), осуществляющий проксирование запросов к джобам и контроль доступа.
    - Сервер, обнаруживающий веб-сервисы и снабжающий Envoy таблицей маршрутизации через xDS протокол.
  - `replicas` – число реплик (подов) Task-прокси
  - `proxy`
    - `proxy.resources` – ресурсы для Envoy
    - `proxy.image`
      - `proxy.image.repository` – репозиторий образа Envoy
      - `proxy.image.tag` – тег образа Envoy
  - `server`
    - `server.resources` – ресурсы для сервера Task-прокси
    - `server.image`
      - `server.image.repository` – репозиторий образа сервера Task-прокси
      - `server.image.tag` – тег образа сервера Task-прокси
  - `nodeSelector` – селектор k8s нод, укажите в нём свои метки
  - `affinity` – вы можете установить pods anti-affinity, чтобы предотвратить аллокацию подов на одних и тех же k8s нодах.

Дефолтные значения параметров указаны в [`values.yaml`](https://github.com/ytsaurus/ytsaurus-task-proxy/blob/main/chart/values.yaml).

## Установка Helm-чарта {#helm-chart-install}

```bash
helm install task-proxy oci://ghcr.io/ytsaurus/task-proxy-chart  \
    --version ${VERSION} \
    -f values.yaml \
    -n ${NAMESPACE}
```

## Настройка ingress контроллера {#ingress-install}

Ниже приведен пример настройки вашего ingress контроллера для Task-прокси, а именно объектов gateway и routes.

Добавьте маршруты (routes) для HTTP и gRPC протоколов:
```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: task-proxy-http-https-route
  namespace: {{.Namespace}}
spec:
  hostnames:
    - "{{.TaskProxyPublicFQDN}}"
  parentRefs:
    - name: gateway
      sectionName: yt-task-proxy-http-https-listener
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /
      backendRefs:
        - kind: Service
          name: task-proxy
          port: 80
---
apiVersion: gateway.networking.k8s.io/v1
kind: GRPCRoute
metadata:
  name: task-proxy-grpc-https-route
  namespace: {{.Namespace}}
spec:
  hostnames:
    - "{{.TaskProxyPublicFQDN}}"
  parentRefs:
    - name: gateway
      sectionName: yt-task-proxy-grpc-https-listener
  rules:
    - backendRefs:
        - kind: Service
          name: task-proxy
          port: 80
```

Добавьте соответствующие маршрутам listener-ы в секцию `spec.listeners` объекта gateway:
```yaml
...
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: gateway
  ...
spec:
  listeners:
    ...
    - name: yt-task-proxy-http-https-listener
      protocol: HTTPS
      port: 443
      hostname: "{{.TaskProxyPublicFQDN}}"
      tls:
        certificateRefs:
          - ...
    - name: yt-task-proxy-grpc-https-listener
      protocol: HTTPS
      port: 9090
      hostname: "{{.TaskProxyPublicFQDN}}"
      tls:
        certificateRefs:
          - ...
```

## Поддержка TLS {#tls}

Если вам нужно, чтобы Task-прокси работала с использованием TLS, добавьте следующие параметры для Helm-чарта:
```yaml
tls:
  enabled: true
  secretName: yt-domain-cert
```

Ниже приведен пример создания секрета `yt-domain-cert` с использованием файлов приватного ключа и цепочки сертификатов:

```sh
kubectl create secret tls yt-domain-cert -n yt --cert=cert.pem --key=key.pem
```
