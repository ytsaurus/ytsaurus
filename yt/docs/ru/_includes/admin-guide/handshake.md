# Шифрование в нативном протоколе {{product-name}}

{{product-name}} поддерживает шифрование внутреннего трафика между компонентами кластера с использованием TLS. В документе описана архитектура шифрования и инструкции по настройке для различных сценариев развертывания.

## Обзор {#overview}

Все компоненты кластера {{product-name}} — мастера, узлы данных, прокси и планировщики — общаются между собой по внутреннему нативному RPC-протоколу. По умолчанию данные передаются в открытом виде, что подходит для доверённых сетей. Если вы работаете в публичной сети или требуется защита конфиденциальных данных, используйте шифрование трафика.

{{product-name}} поддерживает два режима защиты соединений:
- [Одностороннее шифрование](#example-one-way) — клиент проверяет подлинность сервера
- [Взаимная аутентификация (mTLS)](#example-mtls) — обе стороны проверяют сертификаты друг друга

Механизм ротации сертификатов позволяет обновлять их без остановки сервисов. При развёртывании в Kubernetes можно использовать cert-manager для автоматического управления сертификатами.

{% note info %}

Возможность шифрования в нативном протоколе доступна начиная с версии {{product-name}} 25.2.

{% endnote %}

## Архитектура шифрования {#architecture}

Чтобы правильно настроить шифрование, важно понимать, как устроено взаимодействие компонентов в кластере {{product-name}}. Ниже рассмотрена архитектура транспортного уровня и процесс установки защищенного соединения.

### Что такое уровень bus {#what-is-bus-layer}

`bus` — это транспортный уровень {{product-name}}, который обеспечивает передачу сообщений между компонентами: планировщиком, прокси, дата-нодами и т.д. `bus` работает с сообщениями, что позволяет чётко определять их размер, в отличие от обычного TCP, который работает с потоком байт.

<div style="max-width: 600px; margin: 0 auto;">

```mermaid
%%{init: {'theme':'base', 'themeVariables': { 'fontSize': '12px' }}}%%
sequenceDiagram
    participant RPC as RPC
    participant Bus as Bus
    participant TCP as TCP

    Note over RPC: Создает protobuf сообщение
    rect rgb(232, 245, 232)
    RPC->>Bus: Передает сообщение как набор байт
    end
    rect rgb(232, 245, 232)
    Bus->>Bus: Определяет размер сообщения
    end
    rect rgb(232, 245, 232)
    Bus->>TCP: Отправляет байты через TCP
    end
    rect rgb(232, 245, 232)
    TCP->>TCP: Передача по сети
    end
    rect rgb(232, 245, 232)
    TCP->>Bus: Получает байты
    end
    rect rgb(232, 245, 232)
    Bus->>Bus: Дожидается полного сообщения<br/>по известному размеру
    end
    rect rgb(232, 245, 232)
    Bus->>RPC: Передает собранное сообщение
    end
    Note over RPC: Обрабатывает protobuf сообщение
```

</div>

Над уровнем `bus` работает RPC-слой, который собирает из переданных байт protobuf-сообщения, а под уровнем `bus` находится TCP для сетевой передачи данных. Bus-слой точно знает размер каждого сообщения и дожидается получения всех байт перед передачей наверх.

<div style="max-width: 600px; margin: 0 auto;">

```mermaid
%%{init: {'theme':'base', 'themeVariables': { 'fontSize': '12px'}}}%%
graph TB
    subgraph cluster["YTsaurus кластер"]
        HTTP[HTTP Proxy<br/>bus_client + bus_server<br/>точка входа для HTTP API]
        Master[Master Server<br/>bus_client + bus_server<br/>хранит метаданные]
        Data[Data Nodes<br/>bus_client + bus_server<br/>хранят данные]
    end

    Client[Внешний клиент]

    Client -->|HTTP запрос| HTTP
    HTTP -.->|bus_client → bus_server| Master
    Master -.->|bus_client → bus_server| Data

    %% Обратные соединения
    Master -.->|bus_server ← bus_client| HTTP
    Data -.->|bus_server ← bus_client| Master

    style Client fill:#e1f5fe
    style HTTP fill:#f3e5f5
    style Master fill:#e8f5e8
    style Data fill:#fff3e0
    style cluster fill:#b9b9b9
```
</div>

Каждый компонент (мастер, планировщик, дата-нода и др.) выступает одновременно в двух ролях:

- `bus_client` — инициирует исходящие соединения к другим компонентам.
- `bus_server` — принимает входящие соединения от других компонентов.

Например, когда HTTP Proxy запрашивает данные у Master Server:
- HTTP Proxy выступает как `bus_client` (инициатор соединения)
- Master Server выступает как `bus_server` (принимающая сторона)

При этом тот же Master Server может одновременно выступать как `bus_client` при обращении к другим компонентам кластера.

### Процесс установки защищенного соединения {#secure-connection-process}

Установка шифрованного соединения между компонентами происходит в несколько этапов:

1. Установка TCP соединения — клиент и сервер устанавливают обычное TCP соединение;
1. Обмен рукопожатиями (Handshake) — клиент отправляет свой Handshake серверу, затем сервер отправляет свой Handshake клиенту;
1. Решение о шифровании — каждая сторона узнает из Handshake противоположной стороны, нужно ли устанавливать SSL соединение;
1. Обмен SslAck — если шифрование требуется, клиент и сервер обмениваются пакетами SslAck фиксированного размера;
1. Переключение в SSL режим — после обмена SslAck стороны переключаются на использование SSL библиотеки.

<div style="max-width: 600px; margin: 0 auto;">

```mermaid
%%{init: {'theme':'base', 'themeVariables': { 'fontSize': '12px' }}}%%
sequenceDiagram
    participant C as Клиент<br/>(bus_client)
    participant S as Сервер<br/>(bus_server)

    Note over C,S: 1. Установка TCP соединения
    rect rgb(232, 245, 232)
    C->>S: TCP соединение установлено
    end

    Note over C,S: 2. Обмен Handshake
    rect rgb(232, 245, 232)
    C->>S: Handshake клиента<br/>(encryption_mode, verification_mode)
    end
    rect rgb(232, 245, 232)
    S->>C: Handshake сервера<br/>(encryption_mode, verification_mode)
    end

    Note over C,S: 3. Принятие решения о шифровании
    rect rgb(232, 245, 232)
    C->>C: Анализ Handshake сервера<br/>и своей конфигурации
    end
    rect rgb(232, 245, 232)
    S->>S: Анализ Handshake клиента<br/>и своей конфигурации
    end

    Note over C,S: 4. Обмен SslAck (если нужно шифрование)
    rect rgb(232, 245, 232)
    C->>S: SslAck (фиксированного размера)
    end
    rect rgb(232, 245, 232)
    S->>C: SslAck (фиксированного размера)
    end

    Note over C,S: 5. Переключение в SSL режим
    rect rgb(232, 245, 232)
    C->>S: Зашифрованные данные через TLS
    end
    rect rgb(232, 245, 232)
    S->>C: Зашифрованные данные через TLS
    end
```

</div>


**Handshake** — это protobuf-сообщение, которым обмениваются компоненты сразу после установки TCP-соединения. Оно содержит:
- `encryption_mode` — требования к шифрованию (`disabled`/`optional`/`required`)
- `verification_mode` — требования к проверке сертификатов (`none`/`ca`/`full`)

Ключевые особенности Handshake:
- Размер может меняться — поскольку это protobuf, размер сообщения может изменяться при добавлении новых полей.
- Строгая последовательность — сначала клиент отправляет свой Handshake, сервер его получает и только потом отправляет свой.
- Принятие решения — каждая сторона анализирует Handshake противоположной стороны и свою конфигурацию для решения о включении шифровании.

### Принятие решения о шифровании {#encryption-decision}

Каждая компонента принимает решение о шифровании на основе:
- своей конфигурации (`encryption_mode`, `verification_mode`);
- конфигурации противоположной стороны (из Handshake).

Если хотя бы одна сторона настроена на `required`, а другая на `disabled`, соединение не будет установлено.

## Как включить шифрование {#configuration}

Для включения шифрования необходимо настроить соответствующие параметры в конфигурации компонентов кластера. Рассмотрим доступные параметры и сценарии их применения.

### Параметры конфигурации {#configuration-parameters}

Шифрование настраивается через параметры `bus_client` и `bus_server` в конфигурации каждого компонента:

#|
|| **Параметр** {.cell-align-center}| **Описание** {.cell-align-center}||
|| `encryption_mode` {#encryption_mod}| Режим шифрования:
- `disabled` — шифрование отключено. Если другая сторона требует шифрование (`required`), соединение не будет установлено;
- `optional` — шифрование по запросу. Соединение будет с шифрованием, если у другой стороны режим `required`;
- `required` — обязательное шифрование. Если у другой стороны режим `disabled`, соединение завершится ошибкой. ||
|| `verification_mode` | Режим проверки сертификатов:
- `none` — аутентификация другой стороны не выполняется;
- `ca` — другая сторона аутентифицируется по CA файлу (проверяется, что сертификат подписан доверенным CA);
- `full` — другая сторона аутентифицируется по CA и по соответствию сертификата имени хоста (самый строгий режим). ||
|| `cipher_list` | Набор шифров через двоеточие. Пример: `"AES128-GCM-SHA256:PSK-AES128-GCM-SHA256"` ||
|| `ca` | CA сертификат или путь к файлу. Пример: `{ "file_name" = "/etc/yt/certs/ca.pem" }` ||
|| `cert_chain` | Сертификат или путь к файлу. Пример: `{ "file_name" = "/etc/yt/certs/cert.pem" }` ||
|| `private_key` | Приватный ключ или путь к файлу. Пример: `{ "file_name" = "/etc/yt/certs/key.pem" }` ||
|| `load_certs_from_bus_certs_directory` | Загружать сертификаты из директории с bus сертификатами. При значении `true` параметры `ca`, `cert_chain`, `private_key` интерпретируются как имена файлов, а не пути. Удобно для внешних кластеров. ||
|#

### Совместимость режимов шифрования {#encryption-modes-compatibility}

При установке соединения результат зависит от комбинации режимов в настройках параметра [encryption_mode](#configuration-parameters) для `bus_client` и `bus_server`:

#|
|| **Клиент** {.cell-align-center}| **Сервер** {.cell-align-center}| **Результат** {.cell-align-center}||
|| `disabled` | `disabled` | Соединение без шифрования ||
|| `disabled` | `optional` | Соединение без шифрования ||
|| `disabled` | `required` | Ошибка соединения ||
|| `optional` | `disabled` | Соединение без шифрования ||
|| `optional` | `optional` | Соединение без шифрования ||
|| `optional` | `required` | Соединение с шифрованием ||
|| `required` | `disabled` | Ошибка соединения ||
|| `required` | `optional` | Соединение с шифрованием ||
|| `required` | `required` | Соединение с шифрованием ||
|#

### Компоненты для настройки {#components-list}

Шифрование можно настроить для следующих компонентов кластера:

{% cut "Полный список компонентов" %}

- controller_agent
- data_node
- discovery
- exec_node
- master
- master_cache
- proxy
- rpc_proxy
- scheduler
- tablet_node
- timestamp_provider
- clock_provider

{% endcut %}

## Сценарии настройки {#deployment-scenarios}

Способ настройки шифрования зависит от того, как развернут ваш кластер {{product-name}}. Рассмотрим инструкции для основных сценариев развертывания.

{% list tabs dropdown %}

- K8s с оператором {selected}

  #### Развертывание нового кластера с шифрованием {#k8s-new-cluster}

  Самый простой способ — использовать готовый пример конфигурации с включенным TLS:

  1. Установите cert-manager (если еще не установлен):
     ```bash
     kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.0/cert-manager.yaml
     ```

  2. Примените конфигурацию кластера с TLS:
     ```bash
     kubectl apply -f https://raw.githubusercontent.com/ytsaurus/ytsaurus-k8s-operator/main/config/samples/cluster_v1_tls.yaml
     ```

  3. Проверьте статус развертывания:
     ```bash
     kubectl get ytsaurus
     kubectl get certificates
     kubectl get secrets | grep ytsaurus
     ```

  Эта конфигурация автоматически создает самоподписанный CA, выпускает сертификаты и настраивает компоненты для работы шифрования с взаимной аутентификаций (mTLS).

  #### Включение шифрования на существующем кластере {#k8s-existing-cluster}

  Для включения шифрования на работающем кластере добавьте в спецификацию YTsaurus следующие параметры:

  ```yaml
  spec:
    # CA сертификат для проверки
    caBundle:
      kind: Secret
      name: ytsaurus-ca-secret
      key: tls.crt

    # Настройки TLS для внутреннего транспорта
    nativeTransport:
      tlsSecret:
        name: ytsaurus-native-cert
      tlsRequired: true
      tlsInsecure: true
      tlsPeerAlternativeHostName: "interconnect.ytsaurus-dev.svc.cluster.local"
  ```

  {% note warning %}

  Параметр `tlsInsecure: true` отключает проверку клиентских сертификатов. Для полноценной взаимной аутентификации (mTLS) установите `tlsInsecure: false` и укажите `tlsClientSecret`.

  {% endnote %}

  #### Параметры TLS в спецификации оператора {#k8s-tls-parameters}

  #|
  || **Параметр** {.cell-align-center}| **Описание** {.cell-align-center}||
  || `caBundle` | Ссылка на секрет с CA сертификатом для проверки ||
  || `tlsSecret` | Секрет с серверным сертификатом (тип kubernetes.io/tls) ||
  || `tlsClientSecret` | Секрет с клиентским сертификатом для mTLS ||
  || `tlsRequired` | Требовать обязательное шифрование (`true`/`false`) ||
  || `tlsInsecure` | Отключить проверку клиентских сертификатов (`true`/`false`) ||
  || `tlsPeerAlternativeHostName` | Имя хоста для проверки в сертификате ||
  |#

  #### Автоматическая ротация сертификатов {#k8s-cert-rotation}

  При использовании cert-manager ротация происходит автоматически:
  - Cert-manager отслеживает срок действия сертификатов.
  - При приближении истечения срока выпускается новый сертификат.
  - Секрет обновляется без перезапуска подов.
  - Компоненты используют новый сертификат для новых соединений.

  **Как работает автоматическая ротация:**
  - Когда срок действия текущего сертификата истекает, cert-manager автоматически выпускает новый сертификат.
  - Новый сертификат и ключ обновляются в Kubernetes Secret, который смонтирован в контейнер.
  - Файл сертификата, смонтированный в контейнер сервера, автоматически обновляется без перезапуска пода.
  - Каждый сервер периодически перечитывает сертификат:
    - на уровне BUS — при каждом новом соединении;
    - существующие соединения продолжают работать со старым сертификатом;
    - новые соединения используют обновлённый сертификат.

  #### Режимы работы TLS {#k8s-tls-modes}

  В зависимости от значений `tlsRequired` и `tlsInsecure` формируются разные режимы работы TLS:

  #|
  || **tlsRequired** | **tlsInsecure** | **Описание** | **server** | **client** ||
  || `false` | `true` | Шифрование отключено, компоненты общаются в открытом виде | `EO-VN` | `EO-VN` ||
  || `false` | `false` | TLS включён, но не обязателен. Клиент проверяет сертификат сервера, но сервер не требует TLS | `EO-VN` | `ER-VF` ||
  || `true` | `true` | Шифрование включено, но сервер не проверяет сертификат клиента (одностороннее шифрование) | `ER-VN` | `ER-VF` ||
  || `true` | `false` | Шифрование включено, обе стороны проверяют сертификаты друг друга (взаимная проверка) | `ER-VF` | `ER-VF` ||
  |#

  **Расшифровка:**
  - `EO` – Encryption Optional (шифрование опционально)
  - `ER` – Encryption Required (шифрование обязательно)
  - `VN` – Verification None (проверка сертификатов отключена)
  - `VF` – Verification Full (полная проверка сертификатов)

- Ручное развертывание

  #### Подготовка сертификатов {#manual-prepare-certs}

  Перед настройкой шифрования подготовьте SSL-сертификаты:

  1. CA сертификат для проверки
  2. Сертификаты и ключи для каждого компонента
  3. Разместите файлы в доступном для компонентов месте (например, `/etc/yt/certs/`)

  #### Настройка компонентов {#manual-configure-components}

  В конфигурационном файле каждого компонента добавьте параметры шифрования:

  ```yaml
  # Настройки для серверной части
  bus_server:
    encryption_mode: required
    verification_mode: none
    ca:
      file_name: /etc/yt/certs/ca.pem
    cert_chain:
      file_name: /etc/yt/certs/server.pem
    private_key:
      file_name: /etc/yt/certs/server.key

  # Настройки для клиентской части
  bus_client:
    encryption_mode: required
    verification_mode: ca
    ca:
      file_name: /etc/yt/certs/ca.pem
  ```

  #### Применение конфигурации {#manual-apply-config}

  1. Обновите конфигурационные файлы всех компонентов
  1. Перезапустите компоненты кластера
  1. Проверьте установку шифрованных соединений в логах

- Внешние кластеры

  #### Настройка защищенного соединения между кластерами {#external-clusters-mtls}

  Для организации защищенного канала между двумя кластерами {{product-name}} необходимо настроить взаимную аутентификацию (mTLS). Это обеспечивает максимальный уровень безопасности при межкластерном взаимодействии.

  #### Подготовка {#external-prepare}

  На каждом кластере подготовьте:
  - CA сертификат противоположного кластера
  - Клиентский сертификат и ключ для своего кластера
  - Разместите файлы в директории bus-сертификатов

  #### Настройка кластера A {#external-cluster-a}

  1. **Скачайте текущую конфигурацию кластеров**:
     ```bash
     yt get //sys/clusters > clusters.yaml
     ```

  1. **Добавьте конфигурацию для подключения к кластеру B**:
     ```yaml
     cluster-b:
       discovery_servers:
         - "cluster-b.example.com:2136"
       primary_master: "cluster-b.example.com:9013"
       bus_client:
         encryption_mode: required
         verification_mode: full
         ca:
           file_name: ca-cluster-b.pem
         cert_chain:
           file_name: client-cluster-a.pem
         private_key:
           file_name: client-cluster-a.key
         load_certs_from_bus_certs_directory: true
     ```

  1. **Загрузите обновленную конфигурацию**:
     ```bash
     yt set //sys/clusters < clusters.yaml
     ```

  #### Настройка кластера B {#external-cluster-b}

  Повторите аналогичные шаги на кластере B, указав параметры для подключения к кластеру A.

  {% note warning %}

  Для работы взаимной аутентификации конфигурация должна быть настроена на обоих кластерах. Односторонняя настройка приведет к ошибкам соединения.

  {% endnote %}

{% endlist %}

## Примеры конфигураций {#configuration-examples}

### Взаимная проверка сертификатов (mTLS) {#example-mtls}

Конфигурация для максимального уровня безопасности с проверкой сертификатов обеих сторон:

{% cut "Пример конфигурации mTLS" %}

```yaml
# Клиентская часть
bus_client:
  encryption_mode: required
  verification_mode: full
  ca:
    file_name: /etc/yt/certs/ca.pem
  cert_chain:
    file_name: /etc/yt/certs/client.pem
  private_key:
    file_name: /etc/yt/certs/client.key

# Серверная часть
bus_server:
  encryption_mode: required
  verification_mode: full
  ca:
    file_name: /etc/yt/certs/ca.pem
  cert_chain:
    file_name: /etc/yt/certs/server.pem
  private_key:
    file_name: /etc/yt/certs/server.key
```

{% endcut %}

### Одностороннее шифрование {#example-one-way}

Конфигурация, где только клиент проверяет сертификат сервера:

{% cut "Пример односторонней проверки" %}

```yaml
# Клиентская часть
bus_client:
  encryption_mode: required
  verification_mode: ca
  ca:
    file_name: /etc/yt/certs/ca.pem

# Серверная часть
bus_server:
  encryption_mode: required
  verification_mode: none
  cert_chain:
    file_name: /etc/yt/certs/server.pem
  private_key:
    file_name: /etc/yt/certs/server.key
```

{% endcut %}

## Производительность {#performance}

Шифрование увеличивает нагрузку на CPU и может незначительно снизить производительность. По результатам тестирования:
- нагрузка на CPU увеличивается на 5-15% в зависимости от типа операций;
- пропускная способность снижается на 3-10%;
- задержка увеличивается на 1-5 мс.


