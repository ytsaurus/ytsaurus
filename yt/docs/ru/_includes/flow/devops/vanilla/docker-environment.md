# Запуск в docker-окружении

Общий механизм запуска пайплайна в vanilla-операции описан в [Первичном деплое](../../../../flow/devops/vanilla/initial-deploy.md); эта страница — про кластеры, где джобы операций выполняются в docker-образах (CRI job environment). Так работает типовая опенсорс-установка {{product-name}} в Kubernetes: porto-слоёв на таком кластере нет, а имя кластера не резолвится «из коробки». Всё это влияет на то, как в джобы попадают JRE и другие системные зависимости и как компоненты пайплайна находят кластер.

## Окружение джоб {#job-environment}

Окружение задачи vanilla-операции задаётся полем `docker_image` в блоке задачи:

```yson
"vanilla" = {
    "enable" = %true;
    "pool" = "<ваш-пул>";
    "worker" = {"count" = 1; "docker_image" = "docker.io/library/eclipse-temurin:17-jre";};
    "controller" = {"count" = 1; "docker_image" = "docker.io/library/eclipse-temurin:17-jre";};
};
```

`pool` — пул планировщика, в котором запускается операция; о пулах см. [Планировщик и пулы](../../../../user-guide/data-processing/scheduler/scheduler-and-pools.md).

Два правила:

* Имя образа без реестра (`eclipse-temurin:17-jre`) резолвится во внутренний docker-реестр кластера, и, если образ туда не загружен, операция не стартует. Для образов из Docker Hub указывайте полный путь с префиксом `docker.io/library/`.
* Статическим бинарям — `flow_server`, C++- и Go-пайплайнам — образ не нужен: они работают в окружении джобы по умолчанию. Python-пайплайн, собранный через `ya make`, содержит собственный интерпретатор и SDK. Используйте образ, если джобе нужны JRE для Java-компаньона или другие системные зависимости.

## Разрешение имени кластера {#cluster-name}

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

## Доступ к кластеру снаружи {#external-access}

Если кластер развёрнут в Kubernetes, discovery RPC-прокси возвращает их внутренние адреса, которые недоступны раннеру снаружи кластера. Отключите discovery и укажите адрес прокси, доступный снаружи (по умолчанию RPC-прокси слушает порт 9013):

```yson
"clients_cache" = {
    "default_connection" = {
        "enable_proxy_discovery" = %false;
        "proxy_addresses" = ["<внешний-адрес-rpc-прокси>:9013"];
    };
};
```

В `cluster_url` при этом задаётся адрес HTTP-прокси, доступный изнутри кластера: оттуда с ним работают контроллер и воркеры в джобах.

## Сборка flow_server {#flow-server}

Всем пайплайнам, кроме C++, нужен серверный бинарь `flow_server`. Он собирается из [репозитория {{product-name}}](https://github.com/ytsaurus/ytsaurus):

```bash
./ya make --build=release yt/yt/flow/bin/flow_server
strip -o flow_server.stripped yt/yt/flow/bin/flow_server/flow_server
```

Раннер загружает бинарь в файловый кеш кластера при каждом деплое, поэтому со strip (сотни мегабайт вместо гигабайт) деплой заметно быстрее.

## Особенности по языкам {#languages}

{% list tabs %}

- C++

  Пайплайн — один статический бинарь (раннер, контроллер и воркер сразу), собирается тем же `./ya make` из чекаута {{product-name}}. Никаких дополнительных полей в конфиге не требуется, `docker_image` не нужен:

  ```bash
  ./pipeline --config pipeline.yson
  ```

- Java

  Воркер запускает компаньон-JVM внутри джобы, поэтому джобе нужен JDK. В docker-окружении его доставляет образ: укажите JRE-образ в `docker_image` обеих задач (см. [Окружение джоб](#job-environment)) и путь к `java` внутри образа в параметрах ресурса компаньона:

  ```yson
  "resources" = {
      "CompanionManager" = {
          "resource_class_name" = "NYT::NFlow::NCompanion::TJavaCompanionManager";
          "parameters" = {
              "main_class" = "com.example.pipeline.PipelineMain";
              "jdk_bin_path" = "/opt/java/openjdk/bin/java";
          };
      };
  };
  ```

  `docker_image` в конфиге сам переключает раннер в docker-режим — задавать переменные окружения `YT_FLOW_JDK_*` не нужно. Воркер запускает `java` строго по указанному пути (без поиска в `PATH`), а разные образы кладут его в разные места: `/opt/java/openjdk/bin/java` — путь в образах `eclipse-temurin`. Подойдёт любой образ с JRE нужной версии.

  Запуск (jar-файлы из classpath раннер сам доставит в джобу воркера):

  ```bash
  java -cp "<каталог-с-jar>/*" \
      com.example.pipeline.PipelineMain --config pipeline.yson --flow-bin flow_server.stripped
  ```

- Python

  Python-бинарь пайплайна, собранный через `ya make`, служит и лаунчером, и компаньоном. При `vanilla.enable = %true` раннер доставляет этот бинарь в `local_files` воркера под именем `py_companion` и задаёт для каждого универсального ресурса `TCompanionManager` точку входа `./py_companion`. Раннер заменяет указанные в конфиге исполняемый файл и аргументы `entrypoint`, поэтому заданная команда `/usr/local/bin/python3 main.py` в этой схеме запуска не используется.

  Для самодостаточного лаунчера образ с Python или Flow SDK не требуется. Укажите `docker_image`, если джобе нужны другие системные зависимости; точкой входа компаньона останется `./py_companion`. [Соберите Python-пайплайн](../../../../flow/python/getting-started.md#build) и запустите его командой:

  ```bash
  ./pipeline --config pipeline.yson --flow-bin flow_server.stripped
  ```

- Go

  Go-пайплайн — статический бинарь, в котором совмещены лаунчер и компаньон; в джобу он доставляет себя сам. Рантайм в образе не нужен, `docker_image` не требуется:

  ```bash
  ./pipeline --config pipeline.yson --flow-bin flow_server.stripped
  ```

- YQL

  YQL-запрос компилируется в Flow-пайплайн и запускается одной vanilla-операцией — Cypress-объекты пайплайна создаются автоматически. Понадобятся клиент `ytrun` и воркер `ytflow_worker` из репозитория {{product-name}}:

  ```bash
  ./ya make --build=release yt/yql/tools/ytrun yt/yql/tools/ytflow_worker
  ```

  Синтаксис запросов и управляющие прагмы — в разделе [YQL / Быстрый старт](../../../../flow/yql/getting-started.md).

{% endlist %}

## Типичные проблемы {#troubleshooting}

#|
|| **Симптом** | **Причина и решение** ||
|| Операция не стартует с ошибкой резолва docker-образа | Имя образа без реестра резолвится во внутренний реестр кластера — добавьте префикс `docker.io/library/` (см. [Окружение джоб](#job-environment)) ||
|| Компоненты в джобах не могут подключиться к кластеру или друг к другу | Имя кластера не резолвится изнутри джоб — задайте `proxy_url_aliasing_rules`; DNS отдаёт только A-записи — отключите IPv6 в `node_config.address_resolver` (см. [Разрешение имени кластера](#cluster-name)) ||
|| Java: джоба падает с ошибкой `JDK binary file does not exist` | `jdk_bin_path` не совпадает с расположением `java` в выбранном образе — проверьте путь внутри образа ||
|| Загрузка бинаря при деплое занимает минуты | Бинарь не стрипнут — используйте `strip` (см. [Сборка flow_server](#flow-server)) ||
|#

## См. также

- [Первичный деплой](../../../../flow/devops/vanilla/initial-deploy.md)
- [Базовые операции с пайплайном](../../../../flow/devops/vanilla/pipeline-operations.md)
- [Компаньон](../../../../flow/concepts/companion.md)
- [Spec и DynamicSpec](../../../../flow/concepts/spec.md)
