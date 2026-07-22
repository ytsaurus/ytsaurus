# Async Request в {{product-name}} Flow (Java)

Данный пример [пайплайна](../../../../flow/concepts/glossary.md#pipeline) реализует событийно-ориентированный цикл запрос–ответ. Входящие события порождают запросы к обработчику, ответы возвращаются обратно в тот же компьютейшен и накапливаются во внешнем [стейте](../../../../flow/concepts/glossary.md#state). Пример демонстрирует циклическую топологию пайплайна и совместное использование внешнего стейта.

[Исходный код (Java)]({{source-root}}/yt/yt/flow/examples/java/async_request)

[Исходный код (Kotlin)]({{source-root}}/yt/yt/flow/examples/kotlin/async_request)
## Компоненты

### StateKeeperFunction

Обрабатывает два входных [стрима](../../../../flow/concepts/glossary.md#stream-and-computation) — `event` и `response`. По событию `event` генерирует запрос с уникальным `request_id` и эмитирует его в стрим `request`. По событию `response` накапливает суммарную длину ответов в поле `total_length` внешнего стейта:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/async_request/async_request/src/main/java/tech/ytsaurus/flow/examples/asyncrequest/StateKeeperFunction.java' lang='java' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/async_request/async_request/src/main/kotlin/tech/ytsaurus/flow/examples/asyncrequest/StateKeeperFunction.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

{% endlist %}

### RequestProcessorFunction

Stateless-компьютейшен: получает запрос из стрима `request`, вычисляет длину строки и немедленно отправляет ответ в стрим `response`:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/async_request/async_request/src/main/java/tech/ytsaurus/flow/examples/asyncrequest/RequestProcessorFunction.java' lang='java' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/async_request/async_request/src/main/kotlin/tech/ytsaurus/flow/examples/asyncrequest/RequestProcessorFunction.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

{% endlist %}

### AsyncRequestComputationContext

Конфигурация компаньона регистрирует компьютейшены `state` и `processor` через `ComputationProvider`:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/async_request/async_request/src/main/java/tech/ytsaurus/flow/examples/asyncrequest/AsyncRequestComputationContext.java' lang='java' lines='[BEGIN computation_context]-[END computation_context]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/async_request/async_request/src/main/kotlin/tech/ytsaurus/flow/examples/asyncrequest/AsyncRequestComputationContext.kt' lang='kotlin' lines='[BEGIN computation_context]-[END computation_context]' keep-indents %}

{% endlist %}

### NodeCompanionMain

Точка входа компаньона на основе Spring Boot:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/async_request/async_request/src/main/java/tech/ytsaurus/flow/examples/asyncrequest/NodeCompanionMain.java' lang='java' lines='[BEGIN main]-[END main]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/async_request/async_request/src/main/kotlin/tech/ytsaurus/flow/examples/asyncrequest/NodeCompanionMain.kt' lang='kotlin' lines='[BEGIN main]-[END main]' keep-indents %}

{% endlist %}

## Ключевые паттерны

- **Циклическая топология** — стрим `response` возвращается обратно в `state`-компьютейшен, замыкая цикл `event → request → response → state`. Flow поддерживает такие графы явно.
- **Маршрутизация по `streamId`** — одна функция обрабатывает несколько входных стримов, определяя тип сообщения через `message.getStreamId()`.
- **ExternalStateAccessor с PayloadBuilder** — поле `total_length` обновляется точечно: `current.toBuilder()` → изменение → `stateAccessor.set(updated.finish())`.
- **Конфигурация через Spring Boot** — `AsyncRequestComputationContext` реализует `ComputationProvider`; `flow-spring-boot-starter` управляет жизненным циклом gRPC-сервера.

## См. также

- [Быстрый старт (Java)](../../../../flow/java/getting-started.md)
- [Computation (Java)](../../../../flow/java/computation.md)
- [Stateful processing](../../../../flow/concepts/stateful.md)
- [Аналогичный пример на C++](../../../../flow/cpp/examples/async_request.md)
- [Расширенная версия с ретраями](../../../../flow/java/examples/retryable_async_request.md)
