# Быстрый старт в {{product-name}} Flow (YQL)

YQL over Flow позволяет описать [пайплайн](../../../flow/concepts/glossary.md#pipeline) потоковой обработки данных в виде декларативного SQL-запроса — без написания кода на [C++](../../../flow/cpp/getting-started.md), [Java](../../../flow/java/getting-started.md) или [Python](../../../flow/python/getting-started.md). Пайплайн запускается как ванилла операция на выбранном кластере {{product-name}}.

{% note warning %}

Находится в активной разработке, ещё не вся запланированная функциональность доступна.{% if audience == "internal" %} При любых сложностях обращайтесь в чат в Yandex Messenger [YT Flow Public](https://nda.ya.ru/t/MBW0Jgy-7bH78f).{% endif %}

{% endnote %}

{% if audience == "internal" %}
{% note info %}

Если нужно выполнить YQL-`SELECT` в отдельном узле обычного пайплайна, а не описывать весь пайплайн запросом, см. [YQL-вычисление в {{product-name}} Flow](../../../yandex-specific/flow/extensions/yql.md).

{% endnote %}
{% endif %}

## Полезные ссылки

- [Документация YQL](../../../yql/index.md) — полный справочник по синтаксису YQL
- [YQL провайдер YT Flow]({{source-root}}/yt/yql/providers/ytflow) — исходный код
{% if audience == "internal" %}- Вопросы и фичереквесты: чат [YT Flow Public](https://nda.ya.ru/t/hcJkQdBD7LNa9V) или очередь [YQLOVERYT](https://st.yandex-team.ru/YQLOVERYT){% endif %}

## Прагмы {#pragmas}

Запрос YQL over Flow управляется через набор прагм:

| Прагма | Описание |
|---|---|
| `PRAGMA Engine = "ytflow";` | Выбор движка Flow для выполнения запроса |
| `PRAGMA Ytflow.Cluster = "...";` | Кластер для внутренних таблиц пайплайна и выходных упорядоченных очередей |
| `PRAGMA Ytflow.RuntimeCluster = "...";` | Кластер для запуска ванилла операции.{% if audience == "internal" %} Рекомендуется Vanga как cross-DC кластер с высокой доступностью{% endif %} |
| `PRAGMA Ytflow.PipelineDirectory = "...";` | Путь к директории с пайплайнами в {{product-name}} |
| `PRAGMA Ytflow.PipelineName = "...";` | Имя пайплайна. Полный путь: `{pipeline_directory}/{pipeline_name}` |
| `PRAGMA Ytflow.WorkerCount = "...";` | Количество воркер-джобов ванилла операции |
{% if audience == "internal" %}| `PRAGMA Ytflow.LogbrokerConsumerPath = "...";` | Путь к [Logbroker](../../../yandex-specific/flow/extensions/logbroker.md) консьюмеру (только при чтении из Logbroker) |
{% endif %}

## Первый запрос {#first-query}

Пример: построчное преобразование [стрима](../../../flow/concepts/glossary.md#stream-and-computation) (мап).

```yql
-- выбрать движок Flow
PRAGMA Engine = "ytflow";

-- кластер для внутренних таблиц пайплайна
PRAGMA Ytflow.Cluster = "{{flow-data-cluster}}";
-- кластер для ванилла операции
PRAGMA Ytflow.RuntimeCluster = "{{flow-runtime-cluster}}";
-- директория с пайплайнами
PRAGMA Ytflow.PipelineDirectory = "//home/my-project/pipelines";
-- имя пайплайна
PRAGMA Ytflow.PipelineName = "my-pipeline";
-- число воркеров
PRAGMA Ytflow.WorkerCount = "1";

-- читать из входной очереди, трансформировать, писать в выходную
INSERT INTO
    {{flow-data-cluster}}.`//home/my-project/output_queues/sink_queue`
SELECT
    string_field || "_processed" AS string_field,
    int64_field,
    EndsWith(string_field, "bar") AS predicate
FROM
    {{flow-data-cluster}}.`//home/my-project/input_queues/source_queue`
WHERE int64_field > 1;
```

Запрос запускает пайплайн, который непрерывно обрабатывает сообщения из входной очереди и пишет результаты в выходную. Схемы выходных таблиц выводятся автоматически из запроса.

Описание всех поддержанных конструкций YQL см. в разделе [Поддержанные конструкции](../../../flow/yql/features.md).



## Как запустить {#how-to-run}

{% note info "Пререквизиты" %}

Нужно иметь права на чтение и запись во все упоминаемые в запросе директории, а также вычислительную квоту на кластере {{product-name}}, указанном как `Ytflow.RuntimeCluster`.

{% endnote %}

Есть два способа запустить запрос:

**Через UI {{product-name}}**: откройте вкладку **Queries** на рантайм кластере и выполните запрос.

**Через Python-клиент**:

```python
from yt.wrapper import YtClient

# любой продакшн кластер
client = YtClient('{{flow-data-cluster}}')

# запустить запрос и дождаться завершения
client.run_query(
    engine='yql',
    settings=dict(
        # рантайм кластер передаётся здесь
        cluster='{{flow-runtime-cluster}}',
    ),
    query='<YQL query>',
    sync=True,
)
```

После завершения запроса на кластере запустится пайплайн, который будет выполняться непрерывно. Если пайплайн с таким именем уже существует — он остановится с дообработкой всех внутренних потоков, после чего запустится новая версия.

## Мониторинг {#monitoring}

Для отслеживания работы запущенного пайплайна доступны:

{% if audience == "internal" %}- **Граф обработки** с характеристиками потоков и потреблением ресурсов — [вкладка **Flow** пайплайна в UI {{product-name}}](../../../yandex-specific/flow/release/ui.md).{% endif %}
- **Дашборд** — вкладка **Flow → Monitoring**.
- **Логи контроллера** (состояние воркеров, возможные проблемы):
  ```bash
  {{yt-cli}} --proxy <кластер-пайплайна> flow show-logs //home/my-project/pipelines/my-pipeline
  ```
- **Логи джобов** — через ванилла операцию, доступную по ссылке из кубика `flowPublish` в графе пайплайна.

## См. также

- [Поддержанные конструкции](../../../flow/yql/features.md)
- [Основные понятия](../../../flow/concepts/glossary.md)
- [Коннекторы](../../../flow/connectors/about.md)
