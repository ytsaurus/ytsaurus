# Shuffle в {{product-name}} Flow (Java)

[Пайплайн](../../../../flow/concepts/glossary.md#pipeline) читает [поток](../../../../flow/concepts/glossary.md#stream-and-computation) событий, группирует их по ключу и подсчитывает количество уникальных событий с использованием внешнего [стейта](../../../../flow/concepts/glossary.md#state) (ExternalStateAccessor). Пример демонстрирует конфигурацию [компаньона](../../../../flow/concepts/glossary.md#companion) через Spring Boot.

[Исходный код (Java)]({{source-root}}/yt/yt/flow/examples/java/shuffle)

[Исходный код (Kotlin)]({{source-root}}/yt/yt/flow/examples/kotlin/shuffle)
## Компоненты компаньона

### EventMapper

Процессная функция для source-[компьютейшена](../../../../flow/concepts/glossary.md#stream-and-computation) `reader`. Выполняет парсинг и трансформацию входных данных. Ниже показана упрощённая версия — в [реальном коде]({{source-root}}/yt/yt/flow/examples/java/shuffle) дополнительно выполняется JSON-парсинг поля `data` с помощью Jackson `ObjectMapper`:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/shuffle/shuffle/src/main/java/tech/ytsaurus/flow/examples/shuffle/EventMapper.java' lang='java' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/shuffle/shuffle/src/main/kotlin/tech/ytsaurus/flow/examples/shuffle/EventMapper.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

{% endlist %}

### EventReducer

Процессная функция с использованием [ExternalStateAccessor](../../../../flow/java/state.md#external-state) для подсчета количества событий:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/shuffle/shuffle/src/main/java/tech/ytsaurus/flow/examples/shuffle/EventReducer.java' lang='java' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/shuffle/shuffle/src/main/kotlin/tech/ytsaurus/flow/examples/shuffle/EventReducer.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

{% endlist %}

Логика работы:
1. Получаем `ExternalStateAccessor` для стейта `"shuffle-state"`, привязанного к ключу текущего сообщения.
2. Извлекаем текущее значение стейта. Если стейта нет — `getOrDefault()` вернет пустой `Payload`.
3. Создаем `PayloadBuilder` из текущего стейта, увеличиваем счетчик.
4. Сохраняем обновленный стейт.

### ShuffleComputationContext

Конфигурация компаньона регистрирует компьютейшены `reader` и `reducer` через `ComputationProvider`:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/shuffle/shuffle/src/main/java/tech/ytsaurus/flow/examples/shuffle/ShuffleComputationContext.java' lang='java' lines='[BEGIN computation_context]-[END computation_context]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/shuffle/shuffle/src/main/kotlin/tech/ytsaurus/flow/examples/shuffle/ShuffleComputationContext.kt' lang='kotlin' lines='[BEGIN computation_context]-[END computation_context]' keep-indents %}

{% endlist %}

### NodeCompanionMain

Точка входа компаньона на основе Spring Boot:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/shuffle/shuffle/src/main/java/tech/ytsaurus/flow/examples/shuffle/NodeCompanionMain.java' lang='java' lines='[BEGIN main]-[END main]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/shuffle/shuffle/src/main/kotlin/tech/ytsaurus/flow/examples/shuffle/NodeCompanionMain.kt' lang='kotlin' lines='[BEGIN main]-[END main]' keep-indents %}

{% endlist %}

## Ключевые паттерны

- **Конфигурация через Spring Boot** — `ShuffleComputationContext` реализует `ComputationProvider`; `flow-spring-boot-starter` управляет жизненным циклом gRPC-сервера.
- **ExternalStateAccessor** — работа с внешним стейтом через `Payload` и `PayloadBuilder`.
- **SourceComputation с ProcessFunction** — `reader` использует `EventMapper` для трансформации входных данных на стороне компаньона.

## См. также

- [Быстрый старт (Java)](../../../../flow/java/getting-started.md)
- [Computation (Java)](../../../../flow/java/computation.md)
