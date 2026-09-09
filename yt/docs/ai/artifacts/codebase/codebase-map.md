# Навигатор по кодовой базе YT для автора документации

Используй эту карту как начало исследования, а не как источник продуктовых
фактов. Пути помогают быстро выбрать область поиска; актуальное поведение,
дефолты, ограничения и ошибки всегда подтверждай текущим кодом и тестами.

Пути в таблицах и исследовательском отчёте указывай от корня Arcadia, например
`yt/yt/server/http_proxy/config.h`. Scoped-индекс запускается из `yt/` и принимает
пути без первого сегмента `yt/`: для того же файла используй полную команду
`../ya tool ast-index outline yt/server/http_proxy/config.h`. Scoped-форму не
указывай как самостоятельный путь и не смешивай её с путями от корня Arcadia.

## Как исследовать тему

1. Определи пользовательскую поверхность: серверный компонент, C++ API,
   Python/Java/Go SDK, CLI или отдельная интеграция.
2. Найди публичную декларацию: API, тип конфигурации, команду или протокол.
3. Проследи runtime-путь до места, где значение читается и влияет на поведение.
4. Найди тест, который фиксирует нормальный сценарий, границы и ошибки.
5. Сверь существующую документацию и примеры. Они помогают понять сценарий, но
   не заменяют код и тесты как источник истины.
6. Для Public отдельно проверь, что подтверждение относится к Open Source, а не
   только к внутренней обвязке или rollout-конфигурации.

Минимальный набор источников для нового факта: декларация или схема, runtime-
использование и тест. Если одного слоя нет, явно зафиксируй пробел.

## Основная карта

| Что документируем | С чего начать | Где искать подтверждение |
| --- | --- | --- |
| Общие C++ примитивы, RPC, YSON, YPath, логирование, concurrency | `yt/yt/core/` | Подкаталог подсистемы, соседние `unittests/`, вызывающий компонент |
| Современный C++ client API | `yt/yt/client/api/` | Публичные контракты и RPC-proxy implementation в `yt/yt/client/api/` и `yt/yt/client/api/rpc_proxy/`, типы и helpers подсистем в `yt/yt/client/<subsystem>/`, native implementation отдельно в `yt/yt/ytlib/api/native/`, тесты в `yt/yt/client/unittests/` и `yt/yt/tests/` |
| Публичный C++ MapReduce client API | `yt/cpp/mapreduce/` | Контракты в `interface/`, реализация в `client/`, тесты в `tests/`, примеры в `examples/tutorial/` |
| Команды и command descriptors | `yt/yt/client/driver/` | Соответствующий client API, `yt/yt/ytlib/driver/`, Python CLI только если команда действительно проходит через него |
| Серверный компонент или daemon | `yt/yt/server/<component>/` | `config.h`/`config.cpp`, bootstrap/service, `yt/yt/server/lib/`, интеграционные тесты |
| Внутренняя/server-side C++-логика и протоколы | `yt/yt/ytlib/` | Не считай `ytlib/<subsystem>/` продолжением одноимённого `client/<subsystem>/`: переходи сюда только по реальной зависимости или вызову; сверяй публичный контракт в `yt/yt/client/`, вызывающий server-компонент и тесты. Явное исключение — native client implementation в `yt/yt/ytlib/api/native/` |
| Переиспользуемая прикладная C++ библиотека | `yt/yt/library/<feature>/` | Вызывающий client/server-компонент и тесты библиотеки |
| Протокол и wire contract | `yt/yt_proto/`, тематические `protos/` | Исходный `.proto`, места регистрации/использования; не считай сгенерированный код первичным источником |
| Интеграционное поведение кластера | `yt/yt/tests/integration/` | Реальный server/client путь, test helpers в `yt/yt/tests/library/` |
| Готовящийся или сложный архитектурный дизайн | `yt/design-docs/` | Текущая реализация и тесты: design doc может описывать план, а не доставленное поведение |

### Конфигурация и дефолты

- Начинай с типизированной конфигурации рядом с реализацией компонента: в C++ это
  обычно `config.h` и `config.cpp`, в SDK — тематический config-модуль или схема.
- Не используй `yt/cfg/` как общий каталог типизированных конфигов компонентов:
  там находятся Freya, мониторинги и deployment-конфигурация. Обращайся к нему
  только в явно внутренней операционной задаче и не выдавай найденные overrides
  за Open Source defaults.
- Подтверди отдельно:
  - имя и тип поля;
  - дефолт или обязательность в registrar/схеме;
  - место чтения значения;
  - механизм обновления для dynamic config;
  - тест, который различает default и override.
- Не смешивай дефолт в Open Source-коде с внутренним deployment override. Ищи
  внутреннюю конфигурацию отдельно и маркируй область применимости.
- Для строкового ключа, текста ошибки или значения в YAML/JSON используй точный
  raw-text поиск после структурного поиска.

## Клиенты и SDK

### Python и CLI

| Слой | Путь |
| --- | --- |
| Пользовательский Python API и HTTP/native client | `yt/python/yt/wrapper/` |
| Публичные методы и client object | `client_api.py`, `client.py`, `client_impl.py` внутри wrapper |
| Команды CLI и разбор аргументов | `yt/python/yt/cli/`, `yt/python/yt/wrapper/cli_impl.py` |
| Конфигурация и дефолты | `yt/python/yt/wrapper/config.py`, `default_config.py`, `client_state.py` |
| Retry, transaction, streaming | Тематические `retries.py`, `transaction*.py`, `stream.py`, `response_stream.py` |
| Тесты | `yt/python/yt/wrapper/tests/`, тематические `testlib/`, `yt/yt/tests/integration/` |
| Примеры | `yt/python/examples/`, `yt/python/yandex_examples/` |

Не переноси автоматически поведение C++ client или Java/Go SDK в Python: у
клиентов могут различаться env-переменные, fallback, retry и transport.

### Java

| Слой | Путь |
| --- | --- |
| Рекомендуемый публичный клиент | `yt/java/ytsaurus-client/` |
| Общая реализация и типы | `yt/java/ytsaurus-client-core/` |
| Старый API, если на него явно указывает задача | `yt/java/ytclient/`, `yt/java/ytclient-core/` |
| Примеры | `yt/java/ytsaurus-client-examples/` |
| Тестовая инфраструктура | `yt/java/ytsaurus-testlib/` и тесты рядом с модулем |

Сначала выясни, современный или legacy-клиент описывает существующая статья. Не
обобщай найденное в одном API на другой.

### Go

| Слой | Путь |
| --- | --- |
| Публичный client interface и операции | `yt/go/yt/` |
| HTTP и RPC transports | `yt/go/yt/ythttp/`, `yt/go/yt/ytrpc/` |
| MapReduce API | `yt/go/mapreduce/` |
| Типы данных и пути | `yt/go/schema/`, `yt/go/ypath/`, `yt/go/yson/` |
| Примеры | `yt/go/examples/` и `example_test.go` рядом с API |
| Тесты | `*_test.go`, `yt/go/yt/clienttest/`, `yt/go/yt/integration/` |

Проверь interface, конкретную реализацию transport и тест: наличие метода в
interface ещё не доказывает одинаковое поведение всех transports.

## Отдельные компоненты и интеграции

| Тема | Реализация | Тесты и дополнительные источники |
| --- | --- | --- |
| CHYT | Сервер в `yt/chyt/server/`, публичные Python `ChytClient` и `start_clique` в `yt/python/yt/clickhouse/` | `yt/chyt/tests/server/`, `yt/chyt/server/unittests/`, `yt/python/yt/clickhouse/tests/`, controller в `yt/chyt/controller/`; `yt/chyt/client/` рассматривай как protocol/proxy internals, а не пользовательский client API |
| SPYT | `yt/spark/spark-over-yt/` | `yt/spark/e2e/`, примеры и jobs в `yt/spark/` |
| YQL поверх YT | `yt/yql/providers/`, `yt/yql/plugin/` | `yt/yql/tests/`, тематические testsuite/canondata рядом с provider |
| Flow | `yt/yt/flow/` | `yt/yt/flow/tests/`, `examples/`, собственные `AGENTS.md` и `README.md` |
| Kubernetes | `yt/k8s/` | CRD/API definitions, controller logic и тесты в том же дереве |
| Terraform provider | `yt/terraform-provider-ytsaurus/` | schema/resource implementation и tests/examples рядом |
| Airflow provider | `yt/airflow-provider/` | operator/hook implementation и tests в модуле |

Для компонента сначала оставайся в его дереве. Переходи в `yt/yt/client/`,
`yt/yt/ytlib/` или `yt/yt/server/` только по реальной зависимости или символу.

## Поиск по коду

Работай из корня `yt/`. Перед первым поиском в сессии обнови scoped-индекс по
правилам `yt/docs/AGENTS.md`.

Команды `ast-index` ниже принимают пути относительно `yt/`, хотя карта и
финальный отчёт используют пути от корня Arcadia. Например:

```bash
# Arcadia path: yt/yt/server/http_proxy/config.h
../ya tool ast-index outline yt/server/http_proxy/config.h
```

### Структурный поиск по умолчанию

```bash
../ya tool ast-index search '<NameOrFragment>'
../ya tool ast-index file --exact '<file-name>'
../ya tool ast-index symbol '<ExactSymbol>'
../ya tool ast-index class '<ExactClass>'
../ya tool ast-index usages '<ExactSymbol>'
../ya tool ast-index callers '<ExactFunction>'
../ya tool ast-index implementations '<InterfaceOrType>'
../ya tool ast-index outline '<path-to-large-file>'
```

Если точное имя символа неизвестно, начни с `search` по термину или фрагменту и
`file` по имени файла. После обнаружения точного символа переходи к `symbol`,
`class`, `usages` и `callers`. Для конфигурационного класса найди тип, затем
registrar/default и usages. Для API-метода проследи interface, implementation и
вызываемый backend command.

### Когда нужен raw-text поиск

Используй Fast Code Search для строковых ключей, сообщений об ошибках,
env-переменных, комментариев, Markdown, YAML/JSON и других форматов, которые
структурный индекс не покрывает:

```bash
../ya tool cs '<exact literal>' -m50
```

Если структурный запрос неожиданно пуст, сначала проверь `stats`, выполни
`update` и повтори тот же запрос. Не заменяй свежий успешный structural search
дублирующим полнотекстовым обходом.

## Как выбирать источник истины

При противоречии ориентируйся на следующий порядок:

1. Реально исполняемый тест, который проверяет нужный сценарий.
2. Текущая реализация и типизированная схема/registrar.
3. Публичный API или protocol declaration.
4. Актуальный пример, выполняемый тем же кодом.
5. Существующая пользовательская документация.
6. Design doc, комментарий или release note без подтверждения реализацией.

Тест не всегда покрывает deployment override, совместимость старых версий или
операционные ограничения. Такие пробелы фиксируй вопросом разработчику или
`TODO: уточнить`, а не правдоподобным предположением.

## Частые ловушки

- Одно имя подсистемы встречается в `client`, `ytlib` и `server`: `client`
  обычно содержит публичные контракты и типы, `ytlib` — внутреннюю/server-side
  логику и протоколы, `server` — реализацию компонента. Найди пользовательский
  entry point и проследи реальные зависимости; не считай одноимённые каталоги
  автоматической парой API и implementation.
- Рядом могут жить modern и legacy API. Проверяй imports, build dependencies и
  вызывающий код существующей статьи.
- Сгенерированный protobuf-код, vendored ClickHouse/Spark-код и `contrib/` не
  являются лучшей точкой для описания YT-контракта.
- Unit test подтверждает локальную функцию; кластерный сценарий может требовать
  integration test.
- Ошибка в client wrapper не доказывает, что server возвращает тот же текст.
- Внутренний controller или preset может переопределять Open Source default.
- `yt/opensource/` содержит экспортную обвязку; проверяй канонический источник,
  прежде чем документировать поведение продукта.
- Release note и design doc помогают найти feature name, но могут описывать
  другую ревизию или ещё не доставленное состояние.

## Чек-лист отчёта исследования

- Указаны точные пути и символы, а не только названия каталогов.
- Для каждого факта понятно, какой источник его подтверждает.
- Найдены declaration/default, runtime-use и тест либо явно указан пробел.
- Отделены SDK и transports, modern и legacy, Public и Internal.
- Для исполнимого примера найдены предусловия и способ проверить результат.
- Неподтверждённые версии, rollout-настройки и ограничения вынесены в вопросы.

## Поддержание актуальности карты

После структурных изменений в `trunk` CI проверяет существование точных путей
из карты и сравнивает верхний уровень `yt/` с зафиксированным inventory. Раз в
неделю та же проверка контролирует, что содержательная ревизия карты проводилась
не более 90 дней назад. Эти проверки обнаруживают структурный drift, но не
доказывают смысловую корректность описаний: новый каталог нужно классифицировать,
а затронутые маршруты — сверить с текущим кодом и тестами.

Проверяемый точный путь оформляй отдельным однострочным inline code span,
содержащим только этот путь. Fenced code blocks (как с backticks, так и с
tildes) служат только примерами команд и не объявляют точные маршруты. Для
объявлений не используй multiline code spans или fences внутри Markdown-
контейнеров: валидатор отклоняет такую разметку. Корень `yt/` сам по себе не
считается точным маршрутом. Placeholder и glob должны быть синтаксически
полными и не содержать пробелы: например, `<component>`, `[name]`, `{one,two}`
или `**`; повреждённый шаблон считается ошибкой карты.

`reviewed_on` в inventory — ручная аттестация содержательной ревизии, а не
результат автоматической проверки. Механическое обновление списка каталогов не
меняет эту дату. Выполняй обновление из чистой Arc-копии на проверенном commit:

```bash
./ya tool python3 yt/docs/ai/tools/check_codebase_map.py \
  --repo-root . \
  --refresh-inventory
```

После смысловой проверки маршрутов карты отдельно зафиксируй дату ревизии:

```bash
./ya tool python3 yt/docs/ai/tools/check_codebase_map.py \
  --repo-root . \
  --reviewed-on YYYY-MM-DD
```

Обе опции можно передать в одном вызове. Коммить обновлённый inventory только
после того, как новые и исчезнувшие каталоги классифицированы, а затронутые
маршруты при необходимости отражены в карте.
