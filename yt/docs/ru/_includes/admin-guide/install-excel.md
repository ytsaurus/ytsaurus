# Интеграция с Excel

{% if audience == "internal" %}

Инструкции ниже описывают, как развернуть {{product-name}} Excel-интеграцию в Kubernetes. Подробнее про функционал можно прочитать в разделе по [работе с данными Microsoft Excel](../../other/excel.md).

{% else %}

Инструкции ниже описывают, как развернуть {{product-name}} Excel-интеграцию в Kubernetes. Подробнее про функционал можно прочитать в разделе по [работе с данными Microsoft Excel](../../user-guide/excel.md).

{% endif %}

## Описание {#description}

Интеграция включает в себя два микросервиса:
- Exporter — скачивает данные из схематизированных статических таблиц {{product-name}} или результатов Query Tracker в виде Excel-таблицы.
- Uploader — загружает данные из Excel-таблиц в статические таблицы {{product-name}}.

Весь код микросервиса находится в отдельном репозитории [ytsaurus/ytsaurus-excel-integration](https://github.com/ytsaurus/ytsaurus-excel-integration).

## Установка Helm-чарта {#helm-install}

Для установки обоих сервисов используется единый Helm-чарт.

1. Подготовьте файл `values.yaml`.

   Минимальная конфигурация:

   ```yaml
   settings:
     clusters:
       - proxy: http-proxies.default.svc.cluster.local
         # Определяет псевдоним прокси, используемый в пути API:
         #   <http_addr>/<api_path_prefix>/<api_endpoint_name>/api/
         # По умолчанию: совпадает с proxy.
         api_endpoint_name: <cluster-name> # Имя вашего кластера (см. в //sys/@cluster_connection/cluster_name)
     cors:
       allowed_hosts:
         - "yt.example.com" # Адрес вашего UI
   ```

2. Установите чарт:

   ```bash
   helm install ytsaurus-excel oci://ghcr.io/ytsaurus/ytsaurus-excel-chart \
     --version {{excel-version}} \
     -f values.yaml \
     -n <namespace>
   ```

## Настройка сетевого доступа и включение в UI {#network-access}

Для того чтобы функции экспорта и импорта Excel появились в интерфейсе, необходимо настроить доступность сервисов для браузера пользователя и указать их адреса в конфигурации UI.

### Production {#network-access-prod}

Рекомендуемый способ для production-окружения — настроить Ingress.

1. Пример `values.yaml` с настройкой CORS для вашего домена UI:

   ```yaml
   settings:
     clusters:
       - proxy: http-proxies.default.svc.cluster.local
         api_endpoint_name: ytsaurus
     exporter:
       api_path_prefix: "api/excel/exporter"
     uploader:
       api_path_prefix: "api/excel/uploader"
     cors:
       allowed_hosts:
         - "yt.example.com"
   ```

2. Пример Ingress-манифеста:

   ```yaml
   apiVersion: networking.k8s.io/v1
   kind: Ingress
   metadata:
     name: ui-ingress
     namespace: <namespace>
   spec:
     rules:
       - host: <your-ui-domain> # Укажите здесь ваш домен UI
         http:
           paths:
             - backend:
                 service:
                   name: ytsaurus-excel-uploader-svc
                   port:
                     name: http
               path: /api/excel/uploader
               pathType: Prefix
             - backend:
                 service:
                   name: ytsaurus-excel-exporter-svc
                   port:
                     name: http
               path: /api/excel/exporter
               pathType: Prefix
             - backend:
                 service:
                  name: ytsaurus-ui-ytsaurus-ui-chart
                  port:
                     name: http
               path: /
               pathType: Prefix
   ```

3. Настройка UI:

   В `values.yaml` чарта `ytsaurus-ui` необходимо добавить следующие настройки:

   ```yaml
   settings:
     exportTableBaseUrl: "https://yt.example.com/api/excel/exporter"
     uploadTableExcelBaseUrl: "https://yt.example.com/api/excel/uploader"
   ```

   И обновить чарт UI:

   ```bash
   helm upgrade ytsaurus-ui oci://ghcr.io/ytsaurus/ytsaurus-ui-chart \
     --version "0.5.0" \
     -f values.yaml \
     -n <namespace>
   ```

### Testing  {#network-access-testing}

Для локальной разработки или тестирования можно использовать `port-forward`:

1. Пример `values.yaml`:

   ```yaml
   settings:
     clusters:
       - proxy: http-proxies.default.svc.cluster.local
         api_endpoint_name: minisaurus
     cors:
       allowed_hosts:
         - "localhost:8080" # Адрес локального UI
   ```

2. Откройте доступ к микросервисам:

   ```bash
   kubectl port-forward service/ytsaurus-excel-uploader-svc 9095:80
   ```

   ```bash
   kubectl port-forward service/ytsaurus-excel-exporter-svc 9096:80
   ```

3. Настройка UI:

   В `values.yaml` чарта `ytsaurus-ui` необходимо добавить следующие настройки:

   ```yaml
   settings:
     uploadTableExcelBaseUrl: "http://localhost:9095"
     exportTableBaseUrl: "http://localhost:9096"
   ```

  И обновить чарт UI:

  ```bash
  helm upgrade ytsaurus-ui oci://ghcr.io/ytsaurus/ytsaurus-ui-chart \
    --version "0.5.0" \
    -f values.yaml \
    -n <namespace>
  ```

## Проверка результата {#check-result}

<!-- (пока что UI проверяет наличие схемы, YTFRONT-5525)  -->

1. В {{product-name}} UI откройте схематизированную статическую таблицу.

2. Нажмите на кнопку `Download`.

3. Убедитесь, что вкладка `Excel` отображается:

   ![Вкладка экспорта Excel в UI](../../../images/install_excel_01_excel_tab.png)

Вкладка должна отображаться, если с вашего браузера запрос `<exportTableBaseUrl>/<cluster-name>/api/ready` вернёт статус 200 OK. Если этого не происходит, обратите внимание на настройки: UI Helm Chart, Excel Helm Chart и Ingress.

## Детальная конфигурация {#configuration}

Ниже приведены параметры, которые может потребоваться переопределить:

```yaml
replicaCount: 1

settings:
  cors:
    # Разрешенные имена хостов, например yt.example.com. Проверяется точное совпадение: origin.Host == "yt.example.com"
    allowed_hosts: []
    # Разрешенные суффиксы имен хостов, например .yt.example.com. Проверяется через HasSuffix(origin.Host, ".yt.example.com")
    allowed_host_suffixes: []

  # Список кластеров с настройками, специфичными для кластера.
  clusters:
    - proxy: http-proxies.default.svc.cluster.local
      # Определяет псевдоним прокси, используемый в пути API:
      #   <http_addr>/<api_path_prefix>/<api_endpoint_name>/api/
      # По умолчанию: совпадает с proxy.
      api_endpoint_name: minisaurus

  # Имя cookie запроса, которое сервис пересылает в {{product-name}}.
  # {{product-name}}-прокси использует этот cookie для авторизации запросов.
  auth_cookie_name: "YTCypressCookie"
  sso_cookie_name: "yt_oauth_access_token"

  exporter:
    # Максимальное время обработки запроса.
    http_handler_timeout: 2m

    # Максимальный размер входного файла.
    max_excel_file_size_bytes: 104857600 # (100 МБ)

    # Определяет глобальный префикс пути, используемый в пути API:
    #   <http_addr>/<api_path_prefix>/<api_endpoint_name>/api/
    # По умолчанию: пусто.
    api_path_prefix: ""

  uploader:
    # Максимальное время обработки запроса.
    http_handler_timeout: 2m

    # Максимальный размер входного файла.
    max_excel_file_size_bytes: 104857600 # (100 МБ)

    # Определяет глобальный префикс пути, используемый в пути API:
    #   <http_addr>/<api_path_prefix>/<api_endpoint_name>/api/
    # По умолчанию: пусто.
    api_path_prefix: ""
```

Все параметры можно посмотреть в [исходном коде](https://github.com/ytsaurus/ytsaurus-excel-integration/blob/docker/excel/{{excel-version}}/deployments/ytsaurus-excel-chart/values.yaml).
