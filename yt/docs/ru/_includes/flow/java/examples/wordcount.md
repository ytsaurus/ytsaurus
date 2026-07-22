# Word Count в {{product-name}} Flow (Java)

[Пайплайн](../../../../flow/concepts/glossary.md#pipeline) читает [поток](../../../../flow/concepts/glossary.md#stream-and-computation) слов и подсчитывает количество вхождений каждого слова с использованием YSON-стейта. Пример демонстрирует конфигурацию [компаньона](../../../../flow/concepts/glossary.md#companion) через Spring Boot.

[Исходный код (Java)]({{source-root}}/yt/yt/flow/examples/java/word_count)
[Исходный код (Kotlin)]({{source-root}}/yt/yt/flow/examples/kotlin/word_count)

## Компоненты

### WordCountApplication

Точка входа компаньона на основе Spring Boot:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/word_count/wordcount/src/main/java/tech/ytsaurus/flow/examples/wordcount/WordCountApplication.java' lang='java' lines='[BEGIN word_count_application]-[END word_count_application]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/word_count/wordcount/src/main/kotlin/tech/ytsaurus/flow/examples/wordcount/WordCountApplication.kt' lang='kotlin' lines='[BEGIN word_count_application]-[END word_count_application]' keep-indents %}

{% endlist %}

gRPC-сервер поднимается автоматически через Spring Boot auto-config.

### WordCountContext

Стримы пайплайна объявляются через `ComputationProvider` (метод `getStreams()`). Компьютейшен `mapper` регистрируется аннотацией `@FlowComputation` на классе `WordCountMapper` (см. ниже):

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/word_count/wordcount/src/main/java/tech/ytsaurus/flow/examples/wordcount/WordCountContext.java' lang='java' lines='[BEGIN stream_context]-[END stream_context]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/word_count/wordcount/src/main/kotlin/tech/ytsaurus/flow/examples/wordcount/WordCountContext.kt' lang='kotlin' lines='[BEGIN stream_context]-[END stream_context]' keep-indents %}

{% endlist %}

- `reader` — SourceComputation без процессной функции. Чтение и парсинг выполняются на стороне C++ [worker](../../../../flow/concepts/glossary.md#worker)-а.
- `mapper` — Computation, реализованный классом `WordCountMapper` с аннотацией `@FlowComputation(id = "mapper")`.
- `FlowStreams.typed("words", Word.class)` — регистрирует типизированный стрим `"words"`, что позволяет получать сообщения как объекты `Word`.

### WordCountMapper

Процессная функция, реализующая подсчет слов с использованием [YsonStateAccessor](../../../../flow/java/state.md#yson-state):

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/word_count/wordcount/src/main/java/tech/ytsaurus/flow/examples/wordcount/WordCountMapper.java' lang='java' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/word_count/wordcount/src/main/kotlin/tech/ytsaurus/flow/examples/wordcount/WordCountMapper.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

{% endlist %}

Аннотация `@FlowComputation(id = "mapper")` регистрирует класс как компьютейшен и делает его Spring-бином (она мета-аннотирована `@Component`).

### RunnerMain

Точка входа для запуска C++ раннера:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/word_count/wordcount/src/main/java/tech/ytsaurus/flow/examples/wordcount/RunnerMain.java' lang='java' lines='[BEGIN main]-[END main]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/word_count/wordcount/src/main/kotlin/tech/ytsaurus/flow/examples/wordcount/RunnerMain.kt' lang='kotlin' lines='[BEGIN main]-[END main]' keep-indents %}

{% endlist %}

## Ключевые паттерны

- **Spring Boot auto-config** — не нужно вручную создавать `PipelineContext` и `GrpcServerExecution`.
- **@FlowComputation** — процессная функция одновременно становится Spring-бином и компьютейшеном; можно использовать инъекцию зависимостей.
- **ComputationProvider.getStreams()** — объявление стримов пайплайна в одном месте.
- **FlowStreams.typed** — типизированный доступ к сообщениям через Java-объекты.

## Запуск

Пайплайн запускается двумя процессами:
1. **Runner** (`RunnerMain`) — запускает C++ пайплайн.
2. **Companion** (`WordCountApplication`) — запускает Java-процесс с логикой обработки.

Оба класса находятся в одном jar-файле.

## См. также

- [Быстрый старт](../../../../flow/java/getting-started.md)
- [Computation](../../../../flow/java/computation.md)
- [Stateful processing](../../../../flow/concepts/stateful.md)
