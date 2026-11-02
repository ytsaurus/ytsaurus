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

Где живут таймеры вычисления, задаёт статическое поле `timer_store_backend` спеки вычисления:

- `in_memory` (по умолчанию) — все активные таймеры партиции держатся в памяти джобы и дублируются в таблице `timers`. На число и объём таймеров действуют динамические лимиты `timer_store_count_limit` / `timer_store_byte_size_limit`.
- `paged` — таймеры лежат страницами по `timer_page_size` штук в таблицах `timer_pages` / `timer_page_meta` / `timer_page_journal`, упорядоченные по времени срабатывания; в памяти джобы держится только голова каждого семейства страниц (несколько ближайших к срабатыванию батчей) и метаданные страниц — около одной записи на `timer_page_size` таймеров. Лимиты `timer_store_count_limit` / `timer_store_byte_size_limit` действуют на резидентные таймеры. Исключение — таймеры с одним и тем же временем срабатывания (например, конец окна для всех ключей): они лежат в одной странице, которую нельзя разрезать, и она загружается целиком. Если в общих страницах мало таймеров партиции, объём поиска растёт геометрически. Перед срабатыванием дочитывается известное окно ближайших таймеров. Квота записи задаётся полем `timer_store.timer_write_multiplier` динамической спеки.

Динамический `timer_write_multiplier` ограничивает число записей строк таймеров за итерацию. По умолчанию он равен 2. Значение должно быть конечным и не меньше 1. Пусть `R` — обязательные записи новых или заменённых таймеров после дедупликации. Допускается не более `floor(timer_write_multiplier * R)` записей строк таймеров. Переносы после репартиционирования используют оставшуюся квоту. Без обязательных записей существующие таймеры остаются в общих или вложенных страницах. Они срабатывают из загруженной части, принадлежащей партиции. Удаления и метаданные страниц не учитываются. Неиспользованная квота не переносится.

Смена `timer_store_backend` — изменение статической спеки: пайплайн останавливается, а при следующем старте каждая джоба переносит таймеры своей партиции между таблицей `timers` и страницами до своей первой итерации, порциями в отдельных транзакциях; прерванный перенос продолжается при следующем старте. Так же при старте применяется смена `deduplicate_equal_timestamps`: страницы партиции переписываются в новую форму, дубли внутри страницы схлопываются, а дубли в разных страницах снимаются при срабатывании. Страницу, общую с соседней партицией после репартиционирования, переписывает партиция, которой принадлежит её начало ключей; другая ждёт её старта. Смена `timer_page_size` применяется постепенно: страницы режутся и сливаются за несколько итераций без переноса строк таймеров. Ограничение первого этапа: `paged` не совместим с `watermark_percentile` меньше 100 у вычисления с таймерами — такая спека отклоняется валидацией.

Хранилище `paged` должно быть включено на контроллере параметром `enable_paged_timer_store` (секция `controller_service` конфигурации контроллера). В текущем релизе оно выключено. Спека с `timer_store_backend: paged` отклоняется. Флаг проверяется при установке спеки: пайплайн, чья сохранённая спека уже выбирает `paged`, продолжает работать. Так сделано для безопасного отката. Предыдущий релиз ещё не умеет читать формат страниц. Он не должен встретить этот формат в данных пайплайна. Перед даунгрейдом на релиз без поддержки `paged` переведите такие вычисления на `in_memory` в спеке и дождитесь конвертации. Таймеры переносятся из страниц в таблицу `timers` при следующем старте джоб.

У каждой страницы таймеров есть версия формата (`format_version` в `timer_page_meta`). Джоба отказывается стартовать, если встречает страницу с версией новее поддерживаемой. Такую страницу записал более новый бинарник. Безопасно только обновиться вперёд или откатиться. Новая версия формата будет выкатываться в два релиза. Сначала выйдет бинарник, который читает обе версии и пишет старую. Затем выйдет бинарник, который пишет новую версию и при старте джобы переписывает старые страницы.

Таймеры вычисления или таймер-потока, удалённого из спеки, остаются в таблицах: автоматически они не удаляются.

## Порядок обработки {#timer-ordering}

Внутри одного таймер-потока таймеры одного ключа обрабатываются в порядке `TriggerTimestamp`.

Между разными таймер-потоками порядок обработки не гарантирован. Чтобы таймер A с `event_time` или `system_time` сработал после таймера B, добавьте поток B в поле `streams` спецификации таймера A. Тогда A будет ожидать продвижения вотермарка потока B.

## Блокировка связанных таймер-потоков {#timer-stream-blocking}

Если лимиты выходных потоков блокируют таймер-поток, выдача готовых таймеров приостанавливается для всей его группы, даже если в заблокированном потоке нет зарегистрированных таймеров. Другие группы могут продолжать обработку.

Таймер-потоки входят в одну группу, если у них одинаковый `time_type` (`event_time` или `system_time`) и пересекаются наборы потоков, по которым они ждут продвижения вотермарка. Группировка транзитивна: потоки, ожидающие `{a, b}`, `{b, c}` и `{c, d}`, образуют одну группу. Различия значений `streams_with_delays` не разделяют группу. Все таймер-потоки с `real_time` образуют отдельную группу независимо от настроенных входных потоков и задержек.

Группы включают все таймер-потоки, настроенные для компьютейшена, даже если в них нет зарегистрированных таймеров. Группировка не добавляет гарантий порядка обработки между потоками.

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
