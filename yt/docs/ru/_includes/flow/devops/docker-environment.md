# Запуск в docker-окружении

Пайплайн из выпущенных образов Flow запускается одним из двух способов:

- [Vanilla-операция](#vanilla): раннер запускает контроллер и воркеры задачами vanilla-операции, джобы которой работают в docker-образах.
- [Контроллер и воркеры в Kubernetes](#k8s): контроллер и воркеры работают постоянными контейнерами вне джоб {{product-name}} — в Kubernetes или, как в примере с docker compose, на одном хосте; раннер только отправляет им спеку.

Содержание:

- [Выпущенные образы](#images)
- [Vanilla-операция](#vanilla)
  - [Образ джоб](#job-environment)
  - [Запуск из образа](#launch)
  - [Особенности по языкам](#languages)
  - [Остановка](#stop)
  - [Разрешение имени кластера](#cluster-name)
- [Контроллер и воркеры в Kubernetes](#k8s)
  - [Конфиги узлов](#k8s-nodes)
  - [Как компоненты связываются](#k8s-network)
  - [Прямые команды контроллеру](#direct-controller-commands)
- [Доступ к кластеру снаружи](#external-access)
- [Сборка flow_server](#flow-server)
- [Типичные проблемы](#troubleshooting)

## Выпущенные образы {#images}

Каждый релиз Flow публикует три образа в версии релиза.

#|
|| **Образ** | **Содержимое** | **Точка входа** ||
|| `ghcr.io/ytsaurus/flow:<версия>` | `flow_server` | `/usr/bin/flow_server` ||
|| `ghcr.io/ytsaurus/flow-java:<версия>` | `flow_server` и среда выполнения Java 17 | `java` ||
|| `ghcr.io/ytsaurus/flow-python:<версия>` | `flow_server` и Python SDK | `python3` ||
|#

Каждый образ стартует в каталоге `/app/pipeline` и задаёт `YT_FLOW_BIN=/usr/bin/flow_server`, поэтому Java-, Python- и Go-лаунчеры, запущенные внутри него, сами находят `flow_server`. Один образ служит обеим сторонам: в нём работает раннер на вашей машине и контроллер и воркер в vanilla-джобах или в Kubernetes.

## Vanilla-операция {#vanilla}

Раннер запускает контроллер и воркеры задачами одной vanilla-операции и загружает в неё всё, что нужно пайплайну.

### Образ джоб {#job-environment}

Окружение задачи vanilla-операции задаётся полем `docker_image` в блоке задачи. Укажите в обеих задачах [выпущенный образ](#images), из которого запускается пайплайн:

```yson
"vanilla" = {
    "enable" = %true;
    "pool" = "<ваш-пул>";
    "controller" = {"count" = 1; "docker_image" = "ghcr.io/ytsaurus/flow-java:<версия>";};
    "worker" = {"count" = 1; "docker_image" = "ghcr.io/ytsaurus/flow-java:<версия>";};
};
```

`pool` — пул планировщика, в котором запускается операция; о пулах см. [Планировщик и пулы](../../../user-guide/data-processing/scheduler/scheduler-and-pools.md).

Java- и Python-компаньоны работают на среде выполнения образов `flow-java` и `flow-python`, поэтому их джобам образ нужен. Статические бинари — `flow_server`, C++- и Go-пайплайны — работают и в окружении джобы по умолчанию, без образа.

### Запуск из образа {#launch}

Раннер работает на вашей машине в контейнере выпущенного образа. Запускайте его из каталога пайплайна — того, где лежат `pipeline.yson` и всё, что пайплайн доставляет в джобы, — и монтируйте этот каталог в рабочий каталог образа `/app/pipeline`:

```bash
podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" <образ> <аргументы программы> --config pipeline.yson
```

* Контейнер видит только переданные ему переменные окружения: `-e YT_TOKEN` для токена и `-e NAME` для каждой переменной, которую пайплайн передаёт в свои джобы.
* Смонтированный каталог — единственный путь хоста, видимый контейнеру. Держите jar-файлы, бинари и `local_files`, которые доставляет пайплайн, внутри него и ссылайтесь на них по путям относительно него.
* Команда выводит лог контроллера. Пайплайн с конечным источником завершает её сам, когда переходит в `completed`; в остальных случаях Ctrl-C прекращает только вывод лога, а пайплайн продолжает работать, пока вы его [не остановите](#stop).

Аргументы программы для каждого языка приведены в следующем разделе.

### Особенности по языкам {#languages}

{% list tabs %}

- C++

  Пайплайну из стандартных классов собственный код не нужен: бинарём пайплайна служит стандартный `flow_server` — точка входа образа `flow`. Укажите `ghcr.io/ytsaurus/flow:<версия>` в `docker_image` обеих задач и запустите:

  ```bash
  podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" ghcr.io/ytsaurus/flow:<версия> --config pipeline.yson
  ```

  Пайплайн с собственными C++-компьютейшенами — один статический бинарь (раннер, контроллер и воркер сразу), собирается через `./ya make` из чекаута {{product-name}}. Ему не нужны ни образ, ни дополнительные поля в конфиге:

  ```bash
  ./pipeline --config pipeline.yson
  ```

- Java

  Воркер запускает компаньон внутри джобы, поэтому ей нужен JRE — его доставляет образ `flow-java`. Укажите `ghcr.io/ytsaurus/flow-java:<версия>` в `docker_image` обеих задач (см. [Образ джоб](#job-environment)) и класс точки входа в параметрах ресурса компаньона:

  ```yson
  "resources" = {
      "CompanionManager" = {
          "resource_class_name" = "NYT::NFlow::NCompanion::TJavaCompanionManager";
          "parameters" = {
              "main_class" = "com.example.pipeline.PipelineMain";
          };
      };
  };
  ```

  Лаунчер доставляет в джобу воркера jar-файлы из своего classpath. Поэтому передайте ему jar пайплайна и его runtime-зависимости в classpath отдельными jar-файлами — каталоги с классами лаунчер не доставляет — и держите их внутри каталога пайплайна. Например, соберите их в каталог `lib/` задачей Gradle:

  ```kotlin
  tasks.register<Sync>("installLib") {
      dependsOn(tasks.jar)
      from(tasks.jar)
      from(configurations.runtimeClasspath)
      into(layout.projectDirectory.dir("lib"))
  }
  ```

  Соберите их в официальном контейнере Gradle, затем запустите главный класс с classpath `lib/*` в образе `flow-java`:

  ```bash
  podman run --rm -v "$PWD:/src" -w /src docker.io/library/gradle:8-jdk17 gradle -q installLib
  podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" ghcr.io/ytsaurus/flow-java:<версия> \
      -cp 'lib/*' com.example.pipeline.PipelineMain --config pipeline.yson
  ```

  Путь к `java` для воркера лаунчер берёт у JVM, на которой работает сам, — это `java` образа, та же, что в джобах. Ни `jdk_bin_path` в параметрах ресурса, ни `YT_FLOW_JDK_BIN_PATH` задавать не нужно; любой из них, если задан, переопределяет этот путь. Если jar-файлы уже есть в образе джоб, задайте `classpath` в параметрах ресурса, например `/app/pipeline/lib/*`, — тогда лаунчер не загружает jar-файлы для этого ресурса.

- Python

  Воркер запускает компаньон внутри джобы, поэтому ей нужны интерпретатор и Flow SDK — их доставляет образ `flow-python`. Укажите `ghcr.io/ytsaurus/flow-python:<версия>` в `docker_image` обеих задач и запустите скрипт пайплайна, который заканчивается вызовом `app.run()`:

  ```bash
  podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" ghcr.io/ytsaurus/flow-python:<версия> \
      main.py --config pipeline.yson
  ```

  Скрипт служит и лаунчером, и компаньоном: лаунчер доставляет его в `local_files` воркера под именем `py_companion` и задаёт для каждого универсального ресурса `TCompanionManager` точку входа `./py_companion`. Воркер запускает этот файл сам, как исполняемый, — точка входа образа здесь не участвует. Поэтому оставьте в начале скрипта строку `#!/usr/bin/python3` и сделайте файл исполняемым.

  Лаунчер доставляет только этот один скрипт, поэтому Python-код пайплайна должен в нём уместиться. Для кода из нескольких модулей или с дополнительными зависимостями соберите на основе `flow-python` собственный образ, который их содержит, укажите его в `docker_image` и задайте компаньон в параметрах ресурса.

  Python-пайплайн, собранный из исходников через `ya make`, — самодостаточный бинарь с собственным интерпретатором и SDK; образ ему не нужен, а запуск описан в разделе [Сборка Python-пайплайна](../../../flow/python/getting-started.md#build).

- Go

  Go-пайплайн — статический бинарь, в котором совмещены лаунчер и компаньон; лаунчер сам доставляет этот бинарь в джобу воркера. Собирайте его с `CGO_ENABLED=0`, например в официальном контейнере Go:

  ```bash
  podman run --rm -v "$PWD:/src" -w /src -e CGO_ENABLED=0 docker.io/library/golang:1.24 \
      go build -o pipeline .
  ```

  Укажите `ghcr.io/ytsaurus/flow:<версия>` в `docker_image` обеих задач и запустите бинарь в этом образе, переопределив его точку входа:

  ```bash
  podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" --entrypoint ./pipeline \
      ghcr.io/ytsaurus/flow:<версия> --config pipeline.yson
  ```

  Чтобы запускать бинарь компаньона из образа, задайте его в параметрах ресурса, например `"entrypoint" = {"executable" = "/app/pipeline/companion"}`: заданный `executable`, отличный от `./go_companion`, лаунчер сохраняет и не загружает бинарь, если `entrypoint` задан во всех ресурсах компаньона.

- YQL

  YQL-запрос компилируется в Flow-пайплайн и запускается одной vanilla-операцией — Cypress-объекты пайплайна создаются автоматически. Понадобятся клиент `ytrun` и воркер `ytflow_worker` из репозитория {{product-name}}:

  ```bash
  ./ya make --build=release yt/yql/tools/ytrun yt/yql/tools/ytflow_worker
  ```

  Синтаксис запросов и управляющие прагмы — в разделе [YQL / Быстрый старт](../../../flow/yql/getting-started.md).

{% endlist %}

### Остановка {#stop}

Остановите пайплайн, затем отмените его vanilla-операцию. Id операции возьмите из строки лога, которую раннер выводит при запуске, — `Started vanilla operation (..., OperationId: <operation-id>)`:

```bash
yt --proxy <cluster> flow stop-pipeline //path/to/pipeline
yt --proxy <cluster> abort-op <operation-id>
```

Пайплайну, который уже в состоянии `completed`, достаточно `abort-op`. `abort-op` принимает id операции, а не alias из атрибута пайплайна `@current_vanilla_operation`: с alias'ом команда падает с ошибкой `Operation alias cannot be resolved without using runtime information`. Полное удаление пайплайна описано в разделе [Базовые операции с пайплайном](../../../flow/devops/vanilla/pipeline-operations.md#remove).

### Разрешение имени кластера {#cluster-name}

Эта настройка нужна не на всех кластерах — например, в типовой опенсорс-установке {{product-name}} в Kubernetes.

Rich-пути в спеке (`<cluster=my-cluster>//path/to/queue`) и `cluster_url` резолвятся контроллером и воркерами **изнутри** джоб. Если имя кластера не резолвится через DNS по умолчанию, задайте соответствие в блоке `vanilla`:

```yson
"vanilla" = {
    ...
    "proxy_url_aliasing_rules" = {"my-cluster" = "http://<адрес-http-прокси-изнутри-кластера>:80";};
};
```

Адрес должен быть доступен из джоб, то есть изнутри Kubernetes — это не всегда тот же адрес, с которого раннер обращается к кластеру снаружи.

Если DNS кластера отдаёт джобам только A-записи (типично для Kubernetes), отключите IPv6-резолвинг для компонент внутри джоб:

```yson
"vanilla" = {
    ...
    "node_config" = {"address_resolver" = {"enable_ipv4" = %true; "enable_ipv6" = %false;};};
};
```

## Контроллер и воркеры в Kubernetes {#k8s}

Контроллер и воркеры — долгоживущие процессы `flow_server` в контейнерах образа `flow`: в подах Kubernetes или, как в примере [`yt/yt/flow/examples/docker`](https://github.com/ytsaurus/ytsaurus/tree/main/yt/yt/flow/examples/docker), в сервисах docker compose на одном хосте. Их запускаете и перезапускаете вы, а не планировщик {{product-name}}. Раннер из того же образа только отправляет контроллеру спеку из `pipeline.yson` и завершается; блок `vanilla` в его конфиге не нужен.

Пайплайну из стандартных классов, как в примере, кроме `flow_server` ничего не нужно. Код компаньона лаунчер доставляет только в vanilla-операцию, поэтому пайплайну с компаньоном нужен образ воркера с кодом и средой выполнения компаньона, а в параметрах ресурса — пути внутри этого образа.

До запуска контроллера создайте узел пайплайна и его системные таблицы в Cypress — в примере это делает `yt_sync.py` пакетом `ytsaurus-flow-yt-sync-mini` из PyPI.

### Конфиги узлов {#k8s-nodes}

Роль процесса `flow_server` задаёт переменная окружения `YT_FLOW_MODE`: `Controller` или `Worker`; без неё `flow_server` работает раннером. Каждому процессу передайте токен в `YT_TOKEN` и конфиг узла:

```yson
{
    "cluster_url" = "<адрес-http-прокси>";
    "path" = "//path/to/pipeline";
    "rpc_port" = 9001;
    "monitoring_port" = 10001;
}
```

`cluster_url` и `path` совпадают у раннера, контроллера и всех воркеров. `rpc_port` — порт, на котором процесс принимает RPC; `monitoring_port` — HTTP-порт с orchid и метриками (`/solomon_proxy/sensors`). Процессам на одном хосте нужны разные порты.

### Как компоненты связываются {#k8s-network}

Контроллер и воркеры публикуют в Cypress адрес своего хоста и свои порты. Воркеры находят контроллер по этому адресу, а раннер по умолчанию отправляет команды через RPC-прокси кластера, которая сама подключается к RPC-порту контроллера. Так же работают команды `yt flow` и UI.

Поэтому RPC-прокси кластера должна достигать RPC-порта контроллера по опубликованному адресу. По умолчанию адреса резолвятся только по IPv6. Для сети только с IPv4 задайте в конфигах раннера, контроллера и воркеров:

```yson
"address_resolver" = {
    "enable_ipv4" = %true;
    "enable_ipv6" = %false;
};
```

В конфиге раннера можно включить оба семейства адресов, в конфиге контроллера и воркера должно быть включено ровно одно.

Пока у контроллера `require_proxy_signature = %false`, любой хост с доступом к его RPC-порту выполняет команды без аутентификации. Открывайте RPC-порты контроллера и воркеров только кластеру и доверенным хостам.

### Прямые команды контроллеру {#direct-controller-commands}

Если RPC-прокси кластера не может подключиться к контроллеру — NAT, файрвол, контроллер внутри сети Kubernetes, недоступной кластеру, — выкатка раннера падает с ошибкой `Cannot connect to pipeline controller leader`. Тогда переключитесь на прямой режим, в котором раннер отправляет команды самому контроллеру (подробнее — в разделе [Команды раннера напрямую в контроллер](../../../flow/tools/cli.md#direct-controller-commands)):

1. В конфиге раннера включите прямой режим:

   ```yson
   "direct_controller_commands" = {
       "enabled" = %true;
   };
   ```

2. В окружении контроллера задайте `YT_FLOW_SKIP_LEADER_PROXY_CONFIRMATION=1`. Без неё контроллер продолжает подтверждать лидерство через RPC-прокси, а это не удаётся.
3. В `address_resolver` конфигов контроллера и воркеров задайте `localhost_name_override` — адрес, по которому контроллер доступен раннеру и воркерам. Он должен относиться к единственному включённому семейству адресов. В примере docker compose все процессы на одном хосте, поэтому это loopback — `::1` для IPv6 или `127.0.0.1` для IPv4:

   ```yson
   "address_resolver" = {
       "localhost_name_override" = "::1";
   };
   ```

Прямой режим есть только у раннера: команды `yt flow` и UI идут через RPC-прокси и до такого контроллера не доходят.

## Доступ к кластеру снаружи {#external-access}

Эта настройка нужна, если кластер развёрнут в Kubernetes, а раннер — или, в режиме [Kubernetes](#k8s), контроллер и воркеры — работают вне его сети. Discovery RPC-прокси возвращает внутренние адреса, которые снаружи недоступны. Отключите discovery и укажите адрес прокси, доступный снаружи (по умолчанию RPC-прокси слушает порт 9013):

```yson
"clients_cache" = {
    "default_connection" = {
        "enable_proxy_discovery" = %false;
        "proxy_addresses" = ["<внешний-адрес-rpc-прокси>:9013"];
    };
};
```

Для vanilla-операции в `cluster_url` при этом задаётся адрес HTTP-прокси, доступный изнутри кластера: оттуда с ним работают контроллер и воркеры в джобах.

## Сборка flow_server {#flow-server}

Выпущенные образы содержат `flow_server`. Чтобы запускать пайплайн без них — лаунчером на хосте с `--flow-bin` или с `flow_server` со своими изменениями, — соберите его из [репозитория {{product-name}}](https://github.com/ytsaurus/ytsaurus):

```bash
./ya make --build=release yt/yt/flow/bin/flow_server
strip -o flow_server.stripped yt/yt/flow/bin/flow_server/flow_server
```

## Типичные проблемы {#troubleshooting}

#|
|| **Симптом** | **Причина и решение** ||
|| Компоненты в джобах не могут подключиться к кластеру или друг к другу | Имя кластера не резолвится изнутри джоб — задайте `proxy_url_aliasing_rules`; DNS отдаёт только A-записи — отключите IPv6 в `node_config.address_resolver` (см. [Разрешение имени кластера](#cluster-name)) ||
|| Лаунчер падает с ошибкой `flow_server is not given` | Лаунчер запущен вне выпущенных образов — запустите его в образе (см. [Запуск из образа](#launch)) или передайте `--flow-bin` ||
|| Java: джоба падает с ошибкой `JDK binary file does not exist` | В задачах указан не образ `flow-java` или раннер запущен вне него — используйте `flow-java` и для задач, и для раннера ||
|| `abort-op` падает с ошибкой `Operation alias cannot be resolved without using runtime information` | Команде передан alias операции — передайте id операции (см. [Остановка](#stop)) ||
|| Выкатка раннера падает с ошибкой `Cannot connect to pipeline controller leader` | RPC-прокси кластера не может подключиться к контроллеру — включите прямой режим (см. [Прямые команды контроллеру](#direct-controller-commands)) ||
|| Загрузка бинаря при деплое занимает минуты | Бинарь не стрипнут — используйте `strip` (см. [Сборка flow_server](#flow-server)) ||
|#

## См. также

- [Первичный деплой](../../../flow/devops/vanilla/initial-deploy.md)
- [Базовые операции с пайплайном](../../../flow/devops/vanilla/pipeline-operations.md)
- [Компаньон](../../../flow/concepts/companion.md)
- [Spec и DynamicSpec](../../../flow/concepts/spec.md)
