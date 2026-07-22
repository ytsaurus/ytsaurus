# Retryable Async Request в {{product-name}} Flow (Java)

[Пайплайн](../../../../flow/concepts/glossary.md#pipeline) расширяет [AsyncRequest](../../../../flow/java/examples/async_request.md): обработчик запросов теперь поддерживает повторные попытки с задержкой. При неудаче `RequestProcessorFunction` сохраняет состояние запроса во внутреннем [стейте](../../../../flow/concepts/glossary.md#state) и устанавливает таймер на 5 секунд; успех определяется по предикату `(requestId + failedAttempts) % 3 == 0`.

[Исходный код (Java)]({{source-root}}/yt/yt/flow/examples/java/retryable_async_request)

[Исходный код (Kotlin)]({{source-root}}/yt/yt/flow/examples/kotlin/retryable_async_request)
## Компоненты

### RequestProcessorFunction

Реализует логику ретраев с помощью внутреннего YSON-стейта и таймеров. При получении запроса сохраняет его в стейт и вызывает `tryRequest`. При срабатывании таймера загружает стейт и повторяет попытку:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/retryable_async_request/retryable_async_request/src/main/java/tech/ytsaurus/flow/examples/retryableasyncrequest/RequestProcessorFunction.java' lang='java' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/retryable_async_request/retryable_async_request/src/main/kotlin/tech/ytsaurus/flow/examples/retryableasyncrequest/RequestProcessorFunction.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

{% endlist %}

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/retryable_async_request/retryable_async_request/src/main/java/tech/ytsaurus/flow/examples/retryableasyncrequest/RequestProcessorFunction.java' lang='java' lines='[BEGIN on_timer]-[END on_timer]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/retryable_async_request/retryable_async_request/src/main/kotlin/tech/ytsaurus/flow/examples/retryableasyncrequest/RequestProcessorFunction.kt' lang='kotlin' lines='[BEGIN on_timer]-[END on_timer]' keep-indents %}

{% endlist %}

Вспомогательный метод `tryRequest` проверяет предикат успеха и при неудаче инкрементирует счётчик попыток, сохраняет стейт и планирует следующую попытку.

### RequestState

YSON-сериализуемая модель стейта запроса. Хранит `requestId`, `key`, текст запроса `request` и счётчик `failedAttempts`:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/retryable_async_request/retryable_async_request/src/main/java/tech/ytsaurus/flow/examples/retryableasyncrequest/model/RequestState.java' lang='java' lines='[BEGIN request_state]-[END request_state]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/retryable_async_request/retryable_async_request/src/main/kotlin/tech/ytsaurus/flow/examples/retryableasyncrequest/model/RequestState.kt' lang='kotlin' lines='[BEGIN request_state]-[END request_state]' keep-indents %}

{% endlist %}

### StateKeeperFunction

Идентична `StateKeeperFunction` из [AsyncRequest](../../../../flow/java/examples/async_request.md): обрабатывает стримы `event` и `response`, накапливает `total_length` во внешнем стейте. Логика ретраев полностью инкапсулирована в `RequestProcessorFunction`.

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/retryable_async_request/retryable_async_request/src/main/java/tech/ytsaurus/flow/examples/retryableasyncrequest/StateKeeperFunction.java' lang='java' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/retryable_async_request/retryable_async_request/src/main/kotlin/tech/ytsaurus/flow/examples/retryableasyncrequest/StateKeeperFunction.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

{% endlist %}

### Регистрация компьютейшенов

Компьютейшены `state` и `processor` регистрируются аннотацией `@FlowComputation` на классах их process-функций:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/retryable_async_request/retryable_async_request/src/main/java/tech/ytsaurus/flow/examples/retryableasyncrequest/StateKeeperFunction.java' lang='java' lines='[BEGIN registration]-[END registration]' %}

  {% code '/yt/yt/flow/examples/java/retryable_async_request/retryable_async_request/src/main/java/tech/ytsaurus/flow/examples/retryableasyncrequest/RequestProcessorFunction.java' lang='java' lines='[BEGIN registration]-[END registration]' %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/retryable_async_request/retryable_async_request/src/main/kotlin/tech/ytsaurus/flow/examples/retryableasyncrequest/StateKeeperFunction.kt' lang='kotlin' lines='[BEGIN registration]-[END registration]' %}

  {% code '/yt/yt/flow/examples/kotlin/retryable_async_request/retryable_async_request/src/main/kotlin/tech/ytsaurus/flow/examples/retryableasyncrequest/RequestProcessorFunction.kt' lang='kotlin' lines='[BEGIN registration]-[END registration]' %}

{% endlist %}

### NodeCompanionMain

Точка входа компаньона на основе Spring Boot:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/retryable_async_request/retryable_async_request/src/main/java/tech/ytsaurus/flow/examples/retryableasyncrequest/NodeCompanionMain.java' lang='java' lines='[BEGIN main]-[END main]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/retryable_async_request/retryable_async_request/src/main/kotlin/tech/ytsaurus/flow/examples/retryableasyncrequest/NodeCompanionMain.kt' lang='kotlin' lines='[BEGIN main]-[END main]' keep-indents %}

{% endlist %}

## Ключевые паттерны

- **Ретраи через таймеры** — при неудаче `output.addTimer(now + 5, 0L)` планирует повторный вызов `onTimer`; стейт между попытками хранится в `YsonStateAccessor`.
- **Предикат успеха** — `(requestId + failedAttempts) % 3 == 0` имитирует нестабильный внешний сервис; в реальных задачах заменяется проверкой HTTP-статуса или другого признака.
- **Очистка стейта после успеха** — `accessor.clear()` вызывается только при успешном ответе, предотвращая повторную обработку.
- **Разделение ответственности** — логика состояния сессии (`StateKeeperFunction`) отделена от логики ретраев (`RequestProcessorFunction`), что упрощает тестирование каждой части независимо.

## Отличия от AsyncRequest

| Аспект | AsyncRequest | RetryableAsyncRequest |
|--------|-------------|----------------------|
| `RequestProcessorFunction` | Stateless, немедленно отвечает | С внутренним стейтом и таймерами |
| Обработка неудачи | Не предусмотрена | Ретрай с задержкой 5 с |
| Стейт запроса | Отсутствует | `RequestState` в YSON |

## См. также

- [Быстрый старт (Java)](../../../../flow/java/getting-started.md)
- [Computation (Java)](../../../../flow/java/computation.md)
- [Stateful processing](../../../../flow/concepts/stateful.md)
- [Базовая версия без ретраев](../../../../flow/java/examples/async_request.md)
- [Аналогичный пример на C++](../../../../flow/cpp/examples/retryable_async_request.md)
