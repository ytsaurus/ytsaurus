## Flow


Один релиз покрывает все компоненты Flow: сервер docker-образами — обычным, с Java-рантаймом и с Python-рантаймом, Java SDK в Maven Central, Python SDK в PyPI и Go SDK go-модулем, все в одной версии.




**Релизы:**

{% cut "**0.3.0**" %}

**Дата релиза:** 2026-09-28


**Страница релиза:** [0.3.0](https://github.com/ytsaurus/ytsaurus/releases/tag/flow/0.3.0)


**Docker-образ:** [ghcr.io/ytsaurus/flow:0.3.0](https://github.com/orgs/ytsaurus/packages/container/flow/1301893105?tag=0.3.0)


**Docker-образ с JRE 17:** [ghcr.io/ytsaurus/flow-java:0.3.0](https://github.com/orgs/ytsaurus/packages/container/flow-java/1301893505?tag=0.3.0)


**Docker-образ с Python 3:** [ghcr.io/ytsaurus/flow-python:0.3.0](https://github.com/orgs/ytsaurus/packages/container/flow-python/1301893891?tag=0.3.0)


**Java SDK в Maven Central:** [0.3.0](https://central.sonatype.com/artifact/tech.ytsaurus/flow-core/0.3.0)


**Python SDK в PyPI:** [0.3.0](https://pypi.org/project/ytsaurus-flow-companion/0.3.0/)


**yt_sync_mini в PyPI:** [0.3.0](https://pypi.org/project/ytsaurus-flow-yt-sync-mini/0.3.0/)


**Go SDK:** [0.3.0](https://pkg.go.dev/go.ytsaurus.tech/yt/go/flow@v0.3.0)


{{product-name}} Flow — фреймворк для потоковой кросс-ДЦ обработки событий с гарантиями exactly-once в рамках экосистемы {{product-name}} с API для C++, Java и Kotlin, Python и Go. Ближайшие внешние аналоги — Google Cloud Dataflow и Apache Flink.

Это первый релиз Flow с опубликованными артефактами. В него входят сервер, SDK и инструменты: все имеют версию 0.3.0 и собраны из одного коммита.

#### Артефакты

Docker-образы; каждый также опубликован с тегом `-relwithdebinfo`, в котором есть отладочные символы:

- `ghcr.io/ytsaurus/flow:0.3.0` — бинарный файл `flow_server`, который работает как раннер, контроллер или воркер. `YT_FLOW_BIN=/usr/bin/flow_server`, рабочая директория `/app/pipeline`, точка входа `flow_server`.
- `ghcr.io/ytsaurus/flow-java:0.3.0` — тот же образ с JRE Eclipse Temurin 17 в `/opt/java/openjdk`, точка входа `java`. Используйте его для пайплайнов с вычислениями на Java или Kotlin.
- `ghcr.io/ytsaurus/flow-python:0.3.0` — тот же образ с Python 3 и предустановленным `ytsaurus-flow-companion` 0.3.0, точка входа `python3`. Используйте его для пайплайнов с вычислениями на Python.

Java SDK в Maven Central, группа `tech.ytsaurus`, версия `0.3.0`:

- `flow-core` — API вычислений (`Computation`, `SourceComputation`, функции обработки строк и батчей, состояния, таймеры).
- `flow-runner` — `FlowApplication`, точка входа, которая запускает пайплайн.
- `flow-server` — gRPC-сервер компаньона, который выполняет ваши вычисления внутри джоба воркера.
- `flow-spring-boot-starter` — автоконфигурация Spring Boot с `@FlowComputation`.
- `flow-test-utils` — обвязка для unit-тестов вычислений без кластера.
- `flow-proto-common`, `flow-proto-companion` — классы протоколов, от которых зависят остальные модули.

Python-пакеты в PyPI, версия `0.3.0`:

- `ytsaurus-flow-companion` — Python SDK (`Pipeline`, вычисления, состояния, таймеры) и его лаунчер.
- `ytsaurus-flow-yt-sync-mini` — создаёт и обновляет объект пайплайна, его служебные таблицы и таблицы, которые использует ваш пайплайн. Работает независимо от Python SDK.

Go-модуль: `go get go.ytsaurus.tech/yt/go/flow@v0.3.0`.

#### Возможности

- Обработка exactly-once
- Корректный результат при сбоях
- Состояние по ключу
- Внешнее состояние в динамических таблицах {{product-name}}
- Водяные знаки и таймеры
- Сложные графы
- Автоматическая адаптация пайплайна под поток данных
- Коннекторы для очередей, статических и динамических таблиц {{product-name}}
- SDK для Java/Kotlin, Python и Go
- Развёртывание как vanilla-операция {{product-name}}
- Docker-образы для окружения джобов
- Управление пайплайном через CLI

#### Документация

- [Что такое Flow](https://ytsaurus.tech/docs/ru/flow/about)
- [Начало работы](https://ytsaurus.tech/docs/ru/flow/start) и [быстрый старт](https://ytsaurus.tech/docs/ru/flow/quickstart)
- Руководства по языкам: [Java](https://ytsaurus.tech/docs/ru/flow/java/getting-started), [Python](https://ytsaurus.tech/docs/ru/flow/python/getting-started), [Go](https://ytsaurus.tech/docs/ru/flow/go/getting-started)
- [Запуск в docker-окружении](https://ytsaurus.tech/docs/ru/flow/devops/docker-environment)
- [CLI Flow](https://ytsaurus.tech/docs/ru/flow/tools/cli)

{% endcut %}

