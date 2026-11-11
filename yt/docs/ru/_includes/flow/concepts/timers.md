# Таймеры в {{product-name}} Flow

## Зачем нужны таймеры {#why-timers}

Многие задачи потоковой обработки требуют ожидания: «если в течение 30 минут после показа не пришёл клик — считать конверсию несостоявшейся». Стандартный обмен сообщениями между компьютейшенами для этого не подходит: сообщение либо есть, либо нет, а ждать «отсутствия» события нельзя.

Таймеры решают эту задачу. Process function в transform-режиме может создать таймер — сказать системе «разбуди меня, когда время X наступит». Когда момент наступает, функция получает вызов `ProcessTimer` / `on_timer` / `onTimer` и может принять решение на основе накопленного [стейта](../../../flow/concepts/glossary.md#state).

Типичные сценарии применения:
- **Джойн с ожиданием**: коррелировать показ рекламы с кликом, который может прийти с задержкой.
- **Таймаут**: обработать ситуацию, когда ожидаемое событие так и не пришло.
- **Агрегации с окнами**: закрыть временное окно и выдать результат, когда накопленные данные «протухают».

## Как работают таймеры {#how-timers-work}

### Жизненный цикл {#lifecycle}

1. **Регистрация.** Компьютейшен вызывает `output.add_timer` / `output.addTimer` / `output->AddTimer`, передавая `TriggerTimestamp` и `EventTimestamp`.
2. **Хранение.** Таймер надёжно сохраняется в {{product-name}} и дополнительно кешируется в памяти процесса.
3. **Срабатывание.** Когда [EventWatermark](../../../flow/concepts/watermarks.md#event-watermark) (или иной сконфигурированный вотермарк) превышает `TriggerTimestamp`, таймер передаётся в компьютейшен.
4. **Обработка и удаление.** Результат обработки и удаление таймера фиксируются в одной транзакции — это обеспечивает гарантии exactly-once (см. [Дедупликация](../../../flow/concepts/glossary.md#deduplication)).

### Поля таймера {#timer-fields}

Каждый таймер содержит два временных поля:

- `TriggerTimestamp` — момент, когда таймер должен сработать. Временна́я шкала (`event_time`, `system_time`, `real_time`) задаётся в [конфигурации](#configuration).
- `EventTimestamp` — бизнес-время исходного события, «придержанное» в таймере. Позволяет при обработке таймера знать, когда произошло инициирующее событие.

Таймер также привязан к **[ключу группировки](../../../flow/concepts/glossary.md#key)** (тому же ключу, по которому работает `Computation`), поэтому он автоматически попадает в правильную партицию и изолирован от других ключей.

## Какие компьютейшены поддерживают таймеры {#supported-computations}

| Адаптер process function | Поддержка таймеров |
|---|---|
| `TProcessFunctionComputation` | ✓ |
| `TProcessFunctionSwiftMapComputation` | ✗ |
| `TProcessFunctionSourceComputation` | ✗ |
| `TProcessFunctionTransformOrderedSourceComputation` | ✗ |

## Конфигурация {#configuration}

Чтобы компьютейшен мог использовать таймеры, в его спеке необходимо заполнить поле `timers`. Каждый элемент массива описывает один таймер-стрим со следующими параметрами:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TTimerSpec.md) %}

Пояснения:

- `time_type` определяет шкалу, по которой система сравнивает `TriggerTimestamp` с текущим вотермарком:
  - `event_time` (по умолчанию) — сравнивается с `EventWatermark` по всем `input`-потокам (или по потокам, указанным в `streams`).
  - `system_time` — сравнивается с `SystemWatermark`.
  - `real_time` — сравнивается с реальным астрономическим временем.

- `streams` / `streams_with_delays` — позволяют ограничить, какие входные потоки участвуют в расчёте вотермарка для данного таймера. `streams_with_delays` дополнительно позволяет задать индивидуальную задержку для каждого потока. Эти параметры действуют только для таймеров `event_time` и `system_time`. Таймер `current_time` срабатывает, когда текущее время достигает `TriggerTimestamp`; `streams` и `streams_with_delays` для него игнорируются.

- `deduplicate_equal_timestamps` — объединяет таймеры одного потока с одинаковым ключом и `TriggerTimestamp`, сохраняя наименьший `EventTimestamp`. Дедупликация не гарантирована: возможны дубликаты. Включено по умолчанию.

### Хранилище таймеров {#timer-store-backend}

Хранилище выбирается статическим параметром `timer_store_backend` в спеке каждого вычисления:

- `in_memory` (по умолчанию) — все активные таймеры партиции хранятся в памяти джобы и сохраняются в таблице `timers`.
- `paged` — таймеры хранятся в таблицах {{product-name}} страницами размером около `timer_page_size` таймеров. В память загружаются ближайшие к срабатыванию таймеры и метаданные страниц. Таймеры с одинаковым временем срабатывания не разделяются между страницами и загружаются в память вместе.

Динамические параметры `timer_store_count_limit` и `timer_store_byte_size_limit` ограничивают число и размер таймеров в памяти для обоих хранилищ. Хранилище `paged` не поддерживает `watermark_percentile` меньше 100 у вычислений с таймерными потоками.

Для `paged` динамический параметр `timer_store.timer_write_multiplier` ограничивает число записываемых строк таймеров за итерацию. Квота кратна числу обязательных записей новых и заменённых таймеров. По умолчанию он равен 2. Допустимы конечные значения не меньше 1. Оставшаяся квота используется для переноса таймеров после репартиционирования. Удаления и запись метаданных в квоту не входят.

Для смены `timer_store_backend` остановите пайплайн и измените спеку. При следующем запуске джобы перенесут таймеры в выбранное хранилище до начала обработки. Прерванный перенос продолжится при повторном запуске. Изменение `deduplicate_equal_timestamps` также применяется при запуске джоб. Новый `timer_page_size` применяется постепенно во время работы.

Таймеры вычислений и таймерных потоков, удалённых из спеки, автоматически не удаляются.

#### Откат на версию без поддержки paged {#timer-store-rollback}

Откат допустим, только если все вычисления используют `timer_store_backend: in_memory` и перенос таймеров из страниц завершён.

Если пайплайн использовал `paged`, остановите его, переключите соответствующие вычисления на `in_memory` и запустите пайплайн на версии с поддержкой `paged`. Дождитесь, пока все джобы завершат инициализацию и начнут обработку. Только после этого можно выполнять откат. Одного изменения спеки недостаточно: старая версия не умеет читать таймеры, оставшиеся в страницах.

## Порядок обработки {#timer-ordering}

Внутри одного таймер-потока таймеры одного ключа обрабатываются в порядке `TriggerTimestamp`.

Между разными таймер-потоками порядок обработки не гарантирован. Чтобы таймер A с `event_time` или `system_time` сработал после таймера B, добавьте поток B в поле `streams` спецификации таймера A. Тогда A будет ожидать продвижения вотермарка потока B.

## Блокировка связанных таймер-потоков {#timer-stream-blocking}

Если лимиты выходных потоков блокируют таймер-поток, выдача готовых таймеров приостанавливается для всей его группы, даже если в заблокированном потоке нет зарегистрированных таймеров. Другие группы могут продолжать обработку.

Таймер-потоки входят в одну группу, если у них одинаковый `time_type` (`event_time` или `system_time`) и пересекаются наборы потоков, по которым они ждут продвижения вотермарка. Группировка транзитивна: потоки, ожидающие `{a, b}`, `{b, c}` и `{c, d}`, образуют одну группу. Различия значений `streams_with_delays` не разделяют группу. Все таймер-потоки с `real_time` образуют отдельную группу независимо от настроенных входных потоков и задержек.

Группы включают все таймер-потоки, настроенные для компьютейшена, даже если в них нет зарегистрированных таймеров. Группировка не добавляет гарантий порядка обработки между потоками.

## Мониторинг лага таймеров {#monitoring}

Для каждого стрима, включая таймер-стримы и объединённые стримы (`__united__`, `__united_timer__` и др.), контроллер публикует метрики `yt.flow.controller.streams.*` с тегом `stream_id`:

- `time_lag`, `count_lag`, `byte_size_lag` — учитывают все зарегистрированные таймеры. Для таймера лаг отсчитывается от момента его создания, поэтому таймеры, назначенные далеко в будущее, дают постоянную «полку» лага, даже если обработка не отстаёт.
- `ready_time_lag` — наибольшее время, в течение которого необработанный таймер уже готов к срабатыванию: время с самого раннего момента готовности среди ещё не обработанных таймеров. Для таймеров с `time_type` `current_time` момент готовности — сам `TriggerTimestamp`. Для `event_time` и `system_time` контроллер восстанавливает момент, когда вотермарки стримов, которых ждёт таймер, впервые достигли `TriggerTimestamp` с учётом задержек, по сохраняемой истории этих вотермарков. История прорежена по времени, поэтому для давно готовых таймеров лаг может быть завышен не более чем на долю `1 / watermark_history_thinning_factor` (по умолчанию на 25%), но никогда не занижается.
- `ready_count_lag`, `ready_byte_size_lag` — количество и размер таймеров, которые уже сработали, но ещё не обработаны. Ещё не сработавшие таймеры не учитываются.

Для стримов без таймеров `ready_*` совпадают с `time_lag`, `count_lag` и `byte_size_lag`. Метрики `ready_*` никогда не превышают соответствующие обычные лаги.

{% note warning %}

- Если вотермарк, которого ждут таймеры, не продвигается, таймеры не становятся готовыми и `ready_time_lag` остаётся около нуля. Используйте `time_lag` и лаги входных стримов как страховку для алертов.
- Таймер, зарегистрированный с `TriggerTimestamp` в прошлом, может кратковременно (обычно на 1–2 отчёта джоба, до его обработки) завышать `ready_time_lag` вплоть до `time_lag`.
- Если момент готовности таймера с `event_time` или `system_time` предшествует началу истории вотермарков, `ready_time_lag` для его стрима и для объединённых стримов, в которые он входит, не публикуется, пока такой таймер не будет обработан. Так бывает сразу после первого выката или отката контроллера, а также для таймеров с `TriggerTimestamp` далеко в прошлом. `ready_count_lag` и `ready_byte_size_lag` при этом публикуются. В алертах на `ready_time_lag` считайте отсутствие данных нормой и используйте `time_lag` как страховку.

{% endnote %}

Метрика `ready_count_lag` — не то же самое, что метрика жизненного цикла `ready_count`. Для таймер-стримов их значения совпадают. Для остальных стримов `ready_count_lag` равна `count_lag`, а `ready_count` учитывает только сообщения, которые готовы к выдаче, но ещё не переданы в обработку.

## Структура таймера {#timer-structure}

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TTimerSerializer.md) %}

## API по языкам {#api}

### C++ {#api-cpp}

```cpp
void ProcessMessage(
    const TInputMessageConstPtr& message,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& /*context*/) override
{
    output->AddTimer(
        TSystemTimestamp(message->EventTimestamp.Underlying() + TDuration::Minutes(30).Seconds()),
        message->EventTimestamp);
}

void ProcessTimer(
    const TInputTimerConstPtr& timer,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context) override
{
    // timer->Key, timer->EventTimestamp, timer->TriggerTimestamp
    auto builder = context->MakeOutputMessageBuilder();
    // ...
    output->AddMessage(builder.Finish());
}
```

Подробнее — в разделе [Process function (C++)](../../../flow/cpp/process-functions.md).

### Java {#api-java}

```java
// Создать таймер в onMessage:
output.addTimer(message.getEventTimestamp() + 30 * 60_000_000_000L, message.getEventTimestamp());

// Обработать сработавший таймер (RowFunction):
@Override
public void onTimer(Timer timer, OutputCollector output, RuntimeContext ctx) {
    // timer.getKey(), timer.getEventTimestamp(), timer.getTriggerTimestamp()
}

// Для BatchFunction:
@Override
public void onTimers(List<Timer> timers, OutputCollector output, RuntimeContext ctx) { ... }
```

Подробнее — в разделе [Computation (Java)](../../../flow/java/computation.md).

### Python {#api-python}

```python
# Создать таймер в on_message:
output.add_timer(trigger_timestamp=message.event_timestamp + 30 * 60_000_000_000, event_timestamp=message.event_timestamp)

# Обработать сработавший таймер (RowFunction):
def on_timer(self, timer, output, ctx):
    # timer.key, timer.event_timestamp, timer.trigger_timestamp, timer.stream_id

# Для BatchFunction:
def on_timers(self, timers, output, ctx):
    for timer in timers:
        ...
```

Подробнее — в разделе [Computation (Python)](../../../flow/python/computation.md).

### Go {#api-go}

```go
// Создать таймер в OnMessage:
out.AddTimer(flow.TimerRequest{
    TriggerTimestamp: msg.EventTimestamp + 30*60_000_000_000,
    EventTimestamp:   msg.EventTimestamp,
})

// Обработать сработавший таймер (RowFunction):
func (f myFunction) OnTimer(
    ctx context.Context,
    rt flow.Runtime,
    timer flow.Timer,
    out flow.OutputCollector,
) error {
    // timer.Key, timer.EventTimestamp, timer.TriggerTimestamp, timer.StreamID
    return nil
}

// Для BatchFunction:
func (f myFunction) OnTimers(ctx context.Context, rt flow.Runtime, timers []flow.Timer, out flow.OutputCollector) error { ... }
```

Подробнее — в разделе [Computation (Go)](../../../flow/go/computation.md).

## Примеры {#examples}

Пример реализации джойна с ожиданием (показ + клик, таймаут 30 минут):

- [C++](../../../flow/cpp/examples/wait_click_join.md)
- [Java](../../../flow/java/examples/wait_click_join.md)
- [Python](../../../flow/python/examples/wait_click_join.md)
- [Go](../../../flow/go/examples/wait_click_join.md)

## Ограничения и известные проблемы {#limitations}

{% note warning %}

**Память процесса.** В хранилище `in_memory` все активные таймеры дополнительно хранятся в памяти процесса помимо {{product-name}}. При большом объёме таймеров это может приводить к ошибкам Out of Memory при старте джобов; для таких вычислений предназначено хранилище `paged` (см. [Хранилище таймеров](#timer-store-backend)).

{% endnote %}

- Swift-map режим не поддерживает таймеры. Если нужна работа с таймерами, запускайте process function под `TProcessFunctionComputation`.
- Временны́е метки передаются в наносекундах (uint64).

## См. также

- [Watermarks и Timers](../../../flow/concepts/watermarks.md)
- [Stateful processing](../../../flow/concepts/stateful.md)
- [Computation (C++)](../../../flow/cpp/computation.md)
- [Computation (Java)](../../../flow/java/computation.md)
- [Computation (Python)](../../../flow/python/computation.md)
- [Computation (Go)](../../../flow/go/computation.md)
