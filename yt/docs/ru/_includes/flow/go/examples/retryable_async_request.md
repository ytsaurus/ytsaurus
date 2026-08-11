# Retryable Async Request в {{product-name}} Flow (Go)

Расширение примера [Async Request](../../../../flow/go/examples/async_request.md): обработчик запросов повторяет каждую неудачную попытку через [таймеры](../../../../flow/concepts/glossary.md#timer). Запрос целиком сохраняется во внутреннем [стейте](../../../../flow/concepts/glossary.md#state) и повторяется с фиксированной задержкой, пока внешний сервис на него не ответит.

[Исходный код]({{source-root}}/yt/yt/flow/examples/go/retryable_async_request)

## Структура {#structure}

Пайплайн состоит из трёх [компьютейшенов](../../../../flow/concepts/glossary.md#stream-and-computation):

- `injector` — нативный сорс (`TSwiftPassthroughOrderedSourceComputation`), объявленный прямо в [спеке](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec): он читает очередь и публикует события в [стрим](../../../../flow/concepts/glossary.md#stream-and-computation) `event`. Go-кода у него нет.
- `state` (`stateKeeper`) — transform-компьютейшен, сгруппированный по `key`: на каждое событие он открывает запрос в стриме `request`, а пришедший в стриме `response` ответ прибавляет к суммарной длине ответов ключа во внешнем стейте `/state`.
- `processor` (`requestProcessor`) — transform-компьютейшен, сгруппированный по `request_id`: он делает попытки обращения к внешнему сервису и публикует ответ в стрим `response`.

Два компьютейшена сгруппированы по разным ключам не случайно: `processor` партиционируется по идентификатору запроса, поэтому запросы одного ключа повторяются независимо друг от друга. Таймерный стрим `delay` объявлен в спеке с `allow_timer_self_dependency = %true` — компьютейшен ставит таймеры сам себе.

## `main.go` {#main-go}

Точка входа: создание пайплайна и регистрация обоих компьютейшенов компаньона.

{% code '/yt/yt/flow/examples/go/retryable_async_request/main.go' lang='go' %}

## `state_keeper.go` {#state-keeper-go}

Маршрутизация стримов и накопление результатов во внешнем стейте: ветвление по `msg.StreamID` разводит событие и ответ по двум обработчикам.

{% code '/yt/yt/flow/examples/go/retryable_async_request/state_keeper.go' lang='go' lines='[BEGIN state_keeper]-[END state_keeper]' %}

## `request_processor.go` {#request-processor-go}

Значение стейта — обычная Go-структура с YSON-тегами. В ней лежит всё, что нужно повтору, включая счётчик неудачных попыток, поэтому исходное сообщение повтору уже не требуется:

{% code '/yt/yt/flow/examples/go/retryable_async_request/request_processor.go' lang='go' lines='[BEGIN request_state]-[END request_state]' %}

Логика повторов: `OnMessage` открывает запрос и делает первую попытку, `OnTimer` читает запрос из стейта и повторяет её. Общий для обоих обработчиков метод `attempt` либо ставит таймер на следующую попытку, либо публикует ответ и очищает стейт:

{% code '/yt/yt/flow/examples/go/retryable_async_request/request_processor.go' lang='go' lines='[BEGIN request_processor]-[END request_processor]' %}

## Ключевые паттерны {#key-patterns}

- Повторы через таймеры: `out.AddTimer(flow.TimerRequest{TriggerTimestamp: ...})` откладывает следующую попытку на `retryDelay`, а `OnTimer` её выполняет. Это стандартный способ реализовать повторы в Flow без внешних очередей. Пустой `StreamID` в `flow.TimerRequest` означает единственный таймерный стрим компьютейшена.
- Счётчик попыток живёт в стейте вместе с данными запроса, поэтому переживает рестарт [воркера](../../../../flow/concepts/glossary.md#worker): после перезапуска повтор продолжается с того же места.
- Вход один раз преобразуется в `requestMessage` через `msg.ConvertTo(&input)`, а retry-логика работает с отдельной структурой `requestState`, содержащей счётчик неудачных попыток.
- Один и тот же стейт открывается и по сообщению, и по таймеру: `flow.OpenYSONState[requestState](rt, requestStateName, msg)` и `flow.OpenYSONState[requestState](rt, requestStateName, timer)` — оба входа несут ключ группировки, по которому стейт и адресуется.
- Очистка стейта при успехе: после `state.Clear()` сработавший позже таймер видит `pending == false` и ничего не делает — устаревшее срабатывание безвредно.
- Разделение ответственности: `stateKeeper` ничего не знает о повторах, вся логика инкапсулирована в `requestProcessor`, поэтому стратегию повторов можно поменять, не трогая учёт результатов.
- Детерминированная симуляция сбоев: `succeeds(request)` стоит на месте настоящего клиента внешнего сервиса — в реальном коде здесь был бы HTTP-вызов.
