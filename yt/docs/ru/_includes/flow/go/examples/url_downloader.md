# URL Downloader в {{product-name}} Flow (Go)

Пример [пайплайна](../../../../flow/concepts/glossary.md#pipeline), который группирует входящие URL по хосту и обрабатывает их пакетно по [таймеру](../../../../flow/concepts/glossary.md#timer). Показывает типичный паттерн «накопить в [стейте](../../../../flow/concepts/glossary.md#state) → обработать по таймеру → очистить стейт».

[Исходный код]({{source-root}}/yt/yt/flow/examples/go/url_downloader)

## Структура {#structure}

Пайплайн состоит из двух [компьютейшенов](../../../../flow/concepts/glossary.md#stream-and-computation):

- `url_reader` — нативный сорс (`TSwiftPassthroughOrderedSourceComputation`), объявленный прямо в [спеке](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec): он читает очередь и публикует сообщения с полями `host` и `url` в [стрим](../../../../flow/concepts/glossary.md#stream-and-computation) `urls`. Go-кода у него нет.
- `url_downloader` (`urlDownloadFunction`) — transform-компьютейшен, который обслуживает компаньон.

Компьютейшен `url_downloader` работает так:

1. Принимает сообщения из стрима `urls`.
2. Дописывает URL в батч своего хоста во внутреннем YSON-стейте `host-state`.
3. Взводит таймер на `flushDelay` (5 секунд) вперёд при каждом новом URL.
4. По срабатыванию таймера обрабатывает весь накопленный батч, публикует результаты в стрим `processed_urls` и очищает стейт.

Сообщения группируются по хосту (`group_by_schema` с `farm_hash(host)` и `host`), поэтому стейт обрабатываемого ключа — это батч ровно одного хоста.

## `main.go` {#main-go}

Точка входа: создание пайплайна, регистрация единственного компьютейшена и запуск.

{% code '/yt/yt/flow/examples/go/url_downloader/main.go' lang='go' %}

## `url_download_function.go` {#url-download-function-go}

Значение стейта — обычная Go-структура с YSON-тегами: в ней лежат имя хоста и список ещё не обработанных URL.

{% code '/yt/yt/flow/examples/go/url_downloader/url_download_function.go' lang='go' lines='[BEGIN host_state]-[END host_state]' %}

`flow.RowFunction` реализует оба обработчика: `OnMessage` накапливает URL в стейте и взводит таймер, `OnTimer` обрабатывает батч целиком.

{% code '/yt/yt/flow/examples/go/url_downloader/url_download_function.go' lang='go' lines='[BEGIN url_download_function]-[END url_download_function]' %}

## Ключевые паттерны {#key-patterns}

- Пакетная обработка по таймеру: `out.AddTimer(flow.TimerRequest{TriggerTimestamp: ...})` в `OnMessage` и вся обработка в `OnTimer` — стандартный способ собирать события во временное окно. Воркер хранит один таймер на ключ, поэтому всплеск сообщений схлопывается в одно срабатывание, а не в срабатывание на каждое сообщение.
- Внутренний YSON-стейт через `flow.OpenYSONState[hostState](rt, hostStateName, msg)`: `Value()` возвращает изменяемый батч, а `Clear()` очищает его после обработки. Имя стейта (`host-state`) совпадает с именем из `parameters.internal_states` компьютейшена в спеке.
- Ключ стейта по хосту задаётся `group_by_schema` из [спеки](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec), так что каждая партиция компьютейшена обрабатывает URL одного хоста изолированно.
- Каждый результат создаётся как `processedURLMessage` и один раз преобразуется в выходное сообщение через `flow.ConvertFrom`.
- Очистка стейта на всех путях: `OnTimer` вызывает `state.Clear()` и когда батч пуст, и когда он обработан, — сработавший таймер всегда оставляет ключ чистым, и пришедшие после этого URL образуют новый батч.
