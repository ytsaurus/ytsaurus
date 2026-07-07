# Настройка мониторинга кластера

{{product-name}} позволяет выгружать метрики объектов кластера в различные системы мониторинга.

Для сбора метрик используется [Prometheus](https://prometheus.io/). Для просмотра метрик можно использовать [Grafana](https://grafana.com/), а также встроенные дашборды в UI {{product-name}}.

Автоматизированная настройка дашбордов и алертов осуществляется с помощью специального Helm-чарта `monitoring-chart`.

## Установка и настройка Prometheus {#setup-prometheus}

Для сбора метрик используется [Prometheus Operator](https://github.com/prometheus-operator/prometheus-operator). Компоненты {{product-name}} и [Odin](../../admin-guide/install-odin.md) автоматически размечаются метками для сбора метрик. Helm-чарт Odin при установке самостоятельно создает ресурс ServiceMonitor для своих метрик. Для сбора метрик с компонентов кластера мы создадим отдельный ServiceMonitor вручную.

1. Установите Prometheus-оператор согласно [инструкции](https://github.com/prometheus-operator/prometheus-operator?tab=readme-ov-file#quickstart).

2. Убедитесь, что под оператора находится в состоянии `Running`:

    ```bash
    kubectl get pods -l app.kubernetes.io/name=prometheus-operator
    ```

3. Создайте файл `prometheus.yaml`:

    {% cut "prometheus.yaml" %}

    ```yaml
    apiVersion: monitoring.coreos.com/v1
    kind: Prometheus
    metadata:
      name: prometheus
    spec:
      serviceAccountName: prometheus
      resources:
        requests:
          memory: 400Mi
      enableAdminAPI: true

      storage:
        volumeClaimTemplate:
          spec:
            accessModes: ["ReadWriteOnce"]
            resources:
              requests:
                storage: 10Gi

      serviceMonitorSelector:
        matchLabels:
          yt_metrics: "true"

      additionalArgs:
        - name: log.level
          value: debug

    ---
    apiVersion: v1
    kind: ServiceAccount
    metadata:
      name: prometheus

    ---
    apiVersion: rbac.authorization.k8s.io/v1
    kind: ClusterRole
    metadata:
      name: prometheus
    rules:
      - apiGroups: [""]
        resources:
          - services
          - endpoints
          - pods
          - namespaces
        verbs: ["get", "list", "watch"]
      - apiGroups:
          - "discovery.k8s.io"
        resources:
          - endpointslices
        verbs:
          - "get"
          - "list"
          - "watch"

    ---
    apiVersion: rbac.authorization.k8s.io/v1
    kind: ClusterRoleBinding
    metadata:
      name: prometheus
    roleRef:
      apiGroup: rbac.authorization.k8s.io
      kind: ClusterRole
      name: prometheus
    subjects:
      - kind: ServiceAccount
        name: prometheus
        namespace: default

    ---
    apiVersion: monitoring.coreos.com/v1
    kind: ServiceMonitor
    metadata:
      name: ytsaurus-metrics
      labels:
        yt_metrics: "true"
    spec:
      namespaceSelector:
        any: true
      selector:
        matchLabels:
          yt_metrics: "true"
      endpoints:
        - port: ytsaurus-metrics
          path: /solomon/all
          relabelings:
            - sourceLabels: [__meta_kubernetes_pod_label_ytsaurus_tech_cluster_name]
              targetLabel: cluster
          metricRelabelings:
            - targetLabel: service
              sourceLabels:
                - service
              regex: (.*)-monitoring
              replacement: ${1}
    ```

    {% endcut %}

    При необходимости можно модифицировать [ServiceMonitor](https://github.com/prometheus-operator/prometheus-operator/blob/main/Documentation/api-reference/api.md#servicemonitor), исходя из ваших требований.

    Для `ClusterRoleBinding` в разделе `subjects[0].namespace` надо указать то пространство имен, в котором планируется разворачивать Prometheus.

4. Примените файл `prometheus.yaml`:

    ```bash
    kubectl -n <namespace> apply -f prometheus.yaml
    ```

5. Убедитесь, что под Prometheus'а находится в состоянии `Running`:

    ```bash
    kubectl -n <namespace> get pods -l app.kubernetes.io/name=prometheus
    ```

6. Убедитесь, что сервис Prometheus'а создан:

    ```bash
    kubectl -n <namespace> get svc -l managed-by=prometheus-operator
    ```

7. Выполните простейший запрос и посмотрите, с каких подов собираются метрики:

    Откройте доступ к сервису Prometheus:

    ```bash
    kubectl -n <namespace> port-forward service/prometheus-operated 9090:9090
    ```

    {% list tabs group=prometheus-ui-curl %}

    - Через UI Prometheus

      Если имеется возможность, откройте UI Prometheus: [http://localhost:9090](http://localhost:9090). Если возможность отсутствует, воспользуйтесь подходом с использованием `curl`.

      В разделе `Query` выполните простейший запрос:

      ```promql
      yt_accounts_chunk_count{account="sys"}
      ```

      Видим для аккаунта "sys" количество чанков:

      ![Prometheus UI](../../../images/monitoring-install-prometheus-ui-example.png)

      Рис. 1. Результат запроса количества чанков для аккаунта "sys" в UI Prometheus.

      Важно убедиться, что в метриках проставляется `cluster`.

      В разделе `Status` -> `Target health` можно найти список всех отслеживаемых компонентов.

    - Через `curl`

      Выполните простейший PromQL запрос:

      ```bash
      curl 'http://localhost:9090/api/v1/query?query=yt_accounts_chunk_count\{account="sys"\}' | jq
      ```

      Видим для аккаунта "sys" количество чанков:

      ```json
      {
        "status": "success",
        "data": {
          "resultType": "vector",
          "result": [
            {
              "metric": {
                "__name__": "yt_accounts_chunk_count",
                "account": "sys",
                "cluster": "ytsaurus",
                "container": "ytserver",
                "endpoint": "ytsaurus-metrics",
                "instance": "10.244.0.178:10010",
                "job": "yt-master-monitoring",
                "namespace": "ytsaurus-dev",
                "pod": "ms-0",
                "service": "yt-master"
              },
              "value": [
                1766656488.985,
                "605"
              ]
            }
          ]
        }
      }
      ```

      Также выполним запрос на получение списка подов, с которых собираются метрики:

      ```bash
      curl 'http://localhost:9090/api/v1/targets?state=active' | jq '
      {
        target_count: (.data.activeTargets | length),
        targets: [
          .data.activeTargets[] | {
            pod: .labels.pod,
            namespace: .labels.namespace,
            job: .labels.job,
            health: .health,
            lastError: .lastError,
            scrapeUrl: .scrapeUrl,
            scrapePool: .scrapePool
          }
        ]
      }'
      ```

      Пример ожидаемого результата:
      
      ```json
      {
        "target_count": 16,
        "targets": [
          {
            "pod": "end-0",
            "namespace": "ytsaurus-dev",
            "job": "yt-exec-node-monitoring",
            "health": "up",
            "lastError": "",
            "scrapeUrl": "http://10.244.0.200:10029/solomon/all",
            "scrapePool": "serviceMonitor/default/ytsaurus-metrics/0"
          },
          ...
        ]
      }
      ```

    {% endlist %}

8. Если у вас установлен Odin, проверьте, собираются ли его метрики:

    Сбор качественных метрик [Odin](../../admin-guide/install-odin.md) осуществляется через отдельный `ServiceMonitor`, создаваемый самим чартом Odin.

    {% list tabs group=prometheus-ui-curl %}

    - Через UI Prometheus

      В разделе `Target health` он будет отображаться так:

      ![Odin service in Prometheus UI](../../../images/monitoring-install-prometheus-ui-odin-example.png)

      Рис. 2. Пример отображения сервиса Odin в Prometheus.

    - Через `curl`

      Выполним запрос на получение списка подов, содержащих в названии `odin`, с которых собираются метрики:

      ```bash
      curl 'http://localhost:9090/api/v1/targets?state=active' | jq '
      .data.activeTargets 
      | map(select(.labels.pod | contains("odin"))) 
      | {
          targets: map({
              pod: .labels.pod,
              namespace: .labels.namespace,
              job: .labels.job,
              health: .health,
              lastError: .lastError,
              scrapeUrl: .scrapeUrl,
              scrapePool: .scrapePool
          })
      }'
      ```

      Пример ожидаемого результата:
      
      ```json
      {
        "targets": [
          {
            "pod": "odin-odin-chart-web-6f8f5cbb7f-n5slb",
            "namespace": "default",
            "job": "odin-odin-chart-web-monitoring",
            "health": "up",
            "lastError": "",
            "scrapeUrl": "http://10.244.0.33:9002/prometheus",
            "scrapePool": "serviceMonitor/default/odin-odin-chart-metrics/0"
          }
        ]
      }
      ```

    {% endlist %}

    Если он не отображается - проверьте наличие `ServiceMonitor` в том же пространстве имен, что и Prometheus:

    ```bash
    kubectl -n <namespace> get servicemonitor -l app.kubernetes.io/name=odin-chart
    ```

    Если он отсутствует - в [настройках чарта](../../admin-guide/install-odin.md#prepare-values) необходимо включить создание `ServiceMonitor`.

Готово! Prometheus установлен и настроен для сбора качественных и количественных метрик с Odin и компонентов {{product-name}}.

## Установка и настройка Grafana {#setup-grafana}

1. Создайте файл `grafana.yaml`:

    {% cut "grafana.yaml" %}

    ```yaml
    ---
    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
      name: grafana-pvc
      labels:
        app: grafana
    spec:
      accessModes:
        - ReadWriteOnce
      resources:
        requests:
          storage: 1Gi
    ---
    apiVersion: v1
    kind: Secret
    metadata:
      name: grafana-secret
      labels:
        app: grafana
    stringData:
      admin-user: admin
      admin-password: password
    type: Opaque
    ---
    apiVersion: v1
    kind: ConfigMap
    metadata:
      name: grafana-datasources
      labels:
        app: grafana
    data:
      prometheus.yaml: |-
        apiVersion: 1
        datasources:
          - name: Prometheus
            type: prometheus
            url: http://prometheus-operated.<namespace>.svc.cluster.local:9090
            access: proxy
            isDefault: true
            editable: true
    ---
    apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: grafana
      labels:
        app: grafana
    spec:
      replicas: 1
      selector:
        matchLabels:
          app: grafana
      template:
        metadata:
          labels:
            app: grafana
        spec:
          securityContext:
            fsGroup: 472
          containers:
            - name: grafana
              image: grafana/grafana:12.1.4
              ports:
                - containerPort: 3000
                  name: http
              env:
                - name: GF_SECURITY_ADMIN_USER
                  valueFrom:
                    secretKeyRef:
                      name: grafana-secret
                      key: admin-user
                - name: GF_SECURITY_ADMIN_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: grafana-secret
                      key: admin-password
              volumeMounts:
                - mountPath: /var/lib/grafana
                  name: grafana-storage
                - mountPath: /etc/grafana/provisioning/datasources
                  name: grafana-datasources
                  readOnly: true
              resources:
                requests:
                  cpu: 250m
                  memory: 750Mi
                limits:
                  cpu: 250m
                  memory: 750Mi
          volumes:
            - name: grafana-storage
              persistentVolumeClaim:
                claimName: grafana-pvc
            - name: grafana-datasources
              configMap:
                name: grafana-datasources
    ---
    apiVersion: v1
    kind: Service
    metadata:
      name: grafana
      labels:
        app: grafana
    spec:
      type: ClusterIP
      ports:
        - port: 3000
          targetPort: http
      selector:
        app: grafana
    ```

    {% endcut %}

    Стоит указать безопасный пароль в Secret и (или) создать его через `kubectl create secret` вместо `apply`.

    В `ConfigMap` в поле `url` необходимо заменить `<namespace>` на тот, который вы используете.

2. Примените файл `grafana.yaml`:

    ```bash
    kubectl -n <namespace> apply -f grafana.yaml
    ```

3. Убедитесь, что под и сервис для Grafana запущены:

    ```bash
    kubectl -n <namespace> get all -l app=grafana
    ```

4. Перейдите в интерфейс Grafana, выполните простейший запрос и создайте сервисный аккаунт:

    Откройте доступ к UI:

    ```bash
    kubectl -n <namespace> port-forward service/grafana 3000:3000
    ```

    Зайдите в UI: [http://localhost:3000](http://localhost:3000).

    В левом скрывающимся окне переходим в раздел `Connections` -> `Data sources`.

    Если datasource `Prometheus` уже существует, переходим в него и в самом низу нажимаем на `Save & test`. Если в ответ видим "Successfully queried the Prometheus API.", то Grafana успешно связалась с Prometheus.

    Если произошла какая-либо ошибка, стоит проверить указанный "Prometheus server URL". Далее стоит обновить ConfigMap с прошлого этапа, чтобы URL и другие параметры в нем оказались правильными. Также для этого datasource сохраним uid:

    {% cut "Как получить UID datasource?" %}

    Переходим на страницу с UID:

    ```
    http://localhost:3000/connections/datasources/edit/prometheus
    ```

    Последняя часть URL, а именно `prometheus` в данном случае, и будет являться нужным нам UID.

    {% endcut %}

    Генератор дашбордов взаимодействует с Grafana при помощи сервисного аккаунта. Получить токен можно в `Administration` -> `Users and access` -> `Service accounts`. Роль сервисного аккаунта должна быть не ниже "Editor". Сохраните токен сервисного аккаунта в переменную окружения (он понадобится на [шаге создания Secret](#create-secret)):

    ```bash
    export GRAFANA_TOKEN="<ваш-grafana-token>" # например, glsa_bk1LYYY
    ```

5. Чтобы пользователи вашего кластера могли переходить в интерфейс Grafana, настройте к ней сетевой доступ:

    {% note warning %}

    Дашборды Grafana, в отличие от дашбордов в UI {{product-name}}, не проверяют права доступа к объектам кластера. Настраивайте права доступа и учетные записи на стороне самой Grafana.

    Если вы ограничили круг пользователей Grafana, вы можете скрыть саму кнопку Grafana в UI {{product-name}} для всех остальных, чтобы не перегружать интерфейс. Как настроить видимость кнопки по ACL — описано в разделе [Отображение кнопки перехода в Grafana](#grafana-button).

    {% endnote %}

    - Через Ingress / LoadBalancer:

        Настройте публичный доступ к сервису `grafana` средствами вашего кластера (например, привяжите домен `https://grafana.ytsaurus.tech`).

    - Локально:
    
        Если вы просто проверяете работу системы локально, публичным адресом будет являться тот же адрес, что и при пробросе портов: `http://localhost:3000/`. При этом проброс портов должен быть активен во время использования UI.

## Настройка интерфейса {{product-name}} {#setup-ui}

Для корректной работы мониторинга и интеграции с Grafana необходимо передать нужные адреса в UI {{product-name}}, установленный через [Helm-чарт](../../admin-guide/install-ytsaurus.md#ui). При установке чарта мониторинга будет производиться автоматическая проверка наличия этих переменных.

Укажите переменные `PROMETHEUS_BASE_URL` и `GRAFANA_BASE_URL` в секцию `ui.env` вашего файла `values.yaml` для helm-чарта UI {{product-name}}:

```yaml
ui:
  env:
    # Внутренний адрес Prometheus, к которому будет обращаться UI
    - name: PROMETHEUS_BASE_URL
      value: "http://prometheus-operated.<namespace>.svc.cluster.local:9090/"
    # Публичный адрес Grafana для перехода из UI
    - name: GRAFANA_BASE_URL
      value: "https://grafana.ytsaurus.tech" # или http://localhost:3000
```

Обновите настройки helm-чарта UI {{product-name}}.

## Установка дашбордов и алертов (`monitoring-chart`) {#setup-monitoring-chart}

Для автоматической загрузки предсобранных дашбордов в Кипарис и Grafana, а также для создания стандартных алертов, используется Helm-чарт `monitoring-chart`.

### Шаг 1: Подготовка пользователя и выдача прав

Создайте пользователя-робота `robot-monitoring` в {{product-name}}:

```bash
yt create user --attr "{name=robot-monitoring}"
```

Создайте директорию в Кипарисе, куда будут загружены дашборды (по умолчанию `//sys/interface_monitoring`), и выдайте права на запись и использование аккаунта, владеющего директорией (по умолчанию `sys`) пользователю `robot-monitoring`:

{% note info %}

Если у вас версия UI 3.12.2 и ниже, то вместо `interface_monitoring` необходимо создавать `interface-monitoring`.

{% endnote %}

```bash
yt create map_node //sys/interface_monitoring --ignore-existing
yt set //sys/interface_monitoring/@acl/end '{action=allow; subjects=[robot-monitoring;]; permissions=[read;write;remove;];}'
yt set //sys/accounts/sys/@acl/end '{action=allow; subjects=[robot-monitoring]; permissions=[use]}'
```

### Шаг 2: Создание Kubernetes Secret с токенами {#create-secret}

Чарт использует Kubernetes Job для загрузки дашбордов. Ему требуются токены для авторизации в Grafana и {{product-name}}.

Создайте Secret, подставив токен {{product-name}} и токен Grafana (сохранённый в переменную `GRAFANA_TOKEN` на этапе [настройки Grafana](#setup-grafana)):

```bash
kubectl create secret generic ytsaurus-monitoring \
    --from-literal=YT_TOKEN=$(yt issue-token robot-monitoring) \
    --from-literal=GRAFANA_TOKEN="$GRAFANA_TOKEN" \
    -n <namespace>
```

### Шаг 3: Подготовка `values.yaml` для monitoring-chart

Создайте файл `values.yaml` со следующими настройками. Укажите ваши данные:

```yaml
cluster:
  # Внутренний адрес HTTP-прокси кластера для загрузки дашбордов
  proxy: "http://http-proxies.<namespace>.svc.cluster.local"
  name: "ytsaurus"

dashboards:
  grafana:
    # Внутренний адрес для загрузки дашбордов
    url: "http://grafana.<namespace>.svc.cluster.local:3000"
    datasource:
      # UID, полученный на этапе настройки Grafana
      uid: <ваш-uid, например, PBFA97CFB590B2093>

ui:
  chart:
    name: "ytsaurus-ui"
    namespace: "default"

  # Данные адреса используются для проверки корректности настроек UI
  prometheusInternalUrl: "http://prometheus-operated.<namespace>.svc.cluster.local:9090/"
  grafanaPublicUrl: "https://grafana.ytsaurus.tech"
```

Полный список параметров можно посмотреть в [`values.yaml`](https://github.com/ytsaurus/ytsaurus/blob/main/yt/docker/charts/monitoring-chart/values.yaml).

{% note info %}

Если вы используете директорию `//sys/interface-monitoring` (для версий UI 3.12.2 и ниже), то в `values.yaml` необходимо дополнительно указать путь до нее:

```yaml
dashboards:
  cypress:
    path: "//sys/interface-monitoring"
```

{% endnote %}

### Шаг 4: Установка чарта

Установите чарт мониторинга:

```bash
helm install ytsaurus-monitoring oci://ghcr.io/ytsaurus/monitoring-chart \
    --version {{monitoring-version}} \
    -f values.yaml \
    -n <namespace>
```

При установке чарт автоматически создаст ресурсы `PrometheusRule` с алертами и запустит `Job`, который загрузит все дашборды в Grafana и UI {{product-name}}. После этого вы сможете видеть вкладки мониторинга в интерфейсе кластера.

{% note info %}

Для отображения некоторых дашбордов необходимы права доступа к просматриваемым объектам. Например, для работы дашборда `master-accounts` требуется право `use` на запрашиваемый аккаунт.

{% endnote %}

## Настройка алертов {#alerts-configuration}

Чарт `monitoring-chart` поставляется с готовым набором алертов (PrometheusRule).

Вы можете использовать значения по умолчанию или точечно переопределять их в `values.yaml`. Полный список алертов можно посмотреть в [`values.yaml`](https://github.com/ytsaurus/ytsaurus/blob/main/yt/docker/charts/monitoring-chart/values.yaml).

Например, чтобы отключить группу алертов целиком или изменить порог срабатывания для конкретного правила:

```yaml
alerts:
  groups:
    master:
      rules:
        # Отключение конкретного правила
        MasterAutomatonThreadOverload:
          enabled: false  
        # Переопределение времени срабатывания (for)
        MediumAlmostOutOfSpace:
          for: 30m  
    # Отключение всей группы алертов для Controller Agent
    controllerAgent:
      enabled: false  
```

## Отображение кнопки перехода в Grafana {#grafana-button}

Так как на этапе настройки UI вы уже добавили переменную `GRAFANA_BASE_URL`, в интерфейсе {{product-name}} (справа от выбора временного диапазона) появится кнопка **Grafana**. 

Перейдя по ней, пользователь попадет на тот же самый дашборд с теми же параметрами за тот же промежуток времени напрямую в интерфейсе Grafana.

![{{product-name}} UI](../../../images/monitoring-install-redirects-to-grafana-1.png)

Рис. 3. Демонстрация кнопки во внутреннем UI кластера.

![Grafana UI](../../../images/monitoring-install-redirects-to-grafana-2.png)

Рис. 4. Интерфейс Grafana с теми же параметрами, что и во внутреннем UI с рис. 3.

По умолчанию кнопка доступна всем пользователям кластера. Если создать документ `//sys/interface_monitoring/allow_grafana_url` (или `//sys/interface-monitoring/allow_grafana_url` для версий UI 3.12.2 и ниже), то кнопка будет видна только пользователям, которые имеют право `use` на этот документ.

## Поддержанные дашборды {#supported-dashboards}

В данный момент автоматически загружаются и поддерживаются следующие дашборды:

<!-- TODO: Добавить описания дашбордов -->

- `master-accounts`
- `scheduler-operation`
- `bundle-ui-user-load`
- `bundle-ui-resource`
- `bundle-ui-cpu`
- `bundle-ui-memory`
- `bundle-ui-disk`
- `bundle-ui-lsm`
- `bundle-ui-network`
- `bundle-ui-efficiency`
- `bundle-ui-rpc-proxy-overview`
- `bundle-ui-rpc-proxy`
- `scheduler-internal`
- `scheduler-pool`
- `cluster-resources`
- `master-global`
- `master-local`
- `queue-metrics`
- `queue-consumer-metrics`
- `http-proxies`
