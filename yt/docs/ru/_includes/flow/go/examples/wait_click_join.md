# Wait Click Join в {{product-name}} Flow (Go)

Пример join-[пайплайна](../../../../flow/concepts/glossary.md#pipeline), который объединяет [стримы](../../../../flow/concepts/glossary.md#stream-and-computation) показов (`hit`) и действий (`action`) по общему ключу, используя [таймеры](../../../../flow/concepts/glossary.md#timer) и внешний [стейт](../../../../flow/concepts/glossary.md#state). Go-реализация аналогичного [примера на C++](../../../../flow/cpp/examples/wait_click_join.md).

[Исходный код]({{source-root}}/yt/yt/flow/examples/go/wait_click_join)

## Структура {#structure}

Пайплайн состоит из трёх [компьютейшенов](../../../../flow/concepts/glossary.md#stream-and-computation):

- `hit_reader` и `action_reader` — нативные сорсы (`TSwiftPassthroughOrderedSourceComputation`), объявленные прямо в [спеке](../../../../flow/concepts/glossary.md#spec-and-dynamic-spec): они читают две очереди и публикуют стримы `hit` и `action`. Go-кода у них нет. Каждый сорс объявляет `watermark_strategy`, поэтому [вотермарк](../../../../flow/concepts/glossary.md#timestamps-and-watermarks) пайплайна считается по времени события.
- `join` (`joinFunction`) — transform-компьютейшен, который обслуживает компаньон.

Компьютейшен `join` работает так:

1. Получает сообщения из двух входных стримов: `hit` и `action`.
2. Накапливает окно одного показа во внешнем стейте `/join-state`.
3. Взводит таймер на момент закрытия окна ожидания.
4. По срабатыванию таймера публикует join-результат в стрим `joined_action` либо просто очищает стейт.

Оба входных стрима группируются по `hit_id` и `hit_time`, поэтому всё, что относится к одному показу, попадает в один ключ. Длину окна задаёт параметр компьютейшена `wait_for_actions` из спеки.

## `main.go` {#main-go}

Точка входа: создание пайплайна, регистрация единственного компьютейшена и запуск.

{% code '/yt/yt/flow/examples/go/wait_click_join/main.go' lang='go' %}

## `join_function.go` {#join-function-go}

Основная логика join-а: функция обрабатывает два стрима и использует таймеры для оконной агрегации.

### `OnMessage` {#on-message}

Отбрасывает запоздавшие сообщения, дописывает своё поле в окно показа и взводит таймер на закрытие окна:

{% code '/yt/yt/flow/examples/go/wait_click_join/join_function.go' lang='go' lines='[BEGIN join_function]-[END join_function]' %}

### `OnTimer` {#on-timer}

Закрывает окно: публикует join-результат, если пришли обе стороны, и в любом случае очищает стейт:

{% code '/yt/yt/flow/examples/go/wait_click_join/join_function.go' lang='go' lines='[BEGIN on_timer]-[END on_timer]' %}

## Ключевые паттерны {#key-patterns}

- Внешний стейт через `flow.OpenExternalState(rt, joinStateName, msg)`: `ConvertTo` читает окно в `joinState`, сообщение меняет нужное поле структуры, а `ConvertFrom` сохраняет окно.
- Таймер с двумя таймстемпами: `out.AddTimer(flow.TimerRequest{TriggerTimestamp: closeTime, EventTimestamp: hitTime})` — `TriggerTimestamp` задаёт, когда окно закроется, `EventTimestamp` привязывает таймер к событию. Таймер взводит каждое сообщение, а не только `hit`: действие может прийти раньше показа, к которому относится, и окно всё равно надо закрыть.
- Фильтрация запоздавших данных: `msg.EventTimestamp < rt.MinWatermark()` отбрасывает сообщения из уже закрытых окон — иначе для одного показа опубликовался бы второй join-результат. Подробнее о [вотермарках](../../../../flow/concepts/glossary.md#timestamps-and-watermarks).
- Параметры компьютейшена: `rt.Parameters().Get(waitForActionsParameter, &spelled)` читает длину окна из спеки, а не зашивает её в код.
- Обработка нескольких входных стримов: ветвление по `msg.StreamID` разводит логику для `hit` и `action`, а неизвестный стрим возвращает ошибку вместо молчаливого пропуска.
- Ключ вместо стейта: `timer.Key` несёт `hit_id` и `hit_time` из `group_by_schema`, поэтому в окне хранятся только присоединяемые данные — дублировать в нём ключ не нужно.
