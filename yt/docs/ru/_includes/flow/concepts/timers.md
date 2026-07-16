# Таймеры в {{product-name}} Flow

## Зачем нужны таймеры {#why-timers}

Многие задачи потоковой обработки требуют ожидания: «если в течение 30 минут после показа не пришёл клик — считать конверсию несостоявшейся». Стандартный обмен сообщениями между компьютейшенами для этого не подходит: сообщение либо есть, либо нет, а ждать «отсутствия» события нельзя.

Таймеры решают эту задачу. `TransformComputation` может создать таймер — сказать системе «разбуди меня, когда время X наступит». Когда момент наступает, компьютейшен получает вызов `on_timer` / `onTimer` / `DoProcessTimer` и может принять решение на основе накопленного [стейта](../../../flow/concepts/glossary.md#state).

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

| Computation | Поддержка таймеров |
|---|---|
| `TTransformComputation` | ✓ |
| `TSwiftMapComputation` | ✗ |
| `TSwiftOrderedSourceComputation` | ✗ |

## Конфигурация {#configuration}

Чтобы компьютейшен мог использовать таймеры, в его спеке необходимо заполнить поле `timers`. Каждый элемент массива описывает один таймер-стрим со следующими параметрами:

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TTimerSpec.md) %}

Пояснения:

- **`time_type` определяет шкалу, по которой система сравнивает `TriggerTimestamp` с текущим вотермарком:
  - `event_time` (по умолчанию) — сравнивается с `EventWatermark` по всем `input`-потокам (или по потокам, указанным в `streams`).
  - `system_time` — сравнивается с `SystemWatermark`.
  - `real_time` — сравнивается с реальным астрономическим временем.

- `streams` / `streams_with_delays` — позволяют ограничить, какие входные потоки участвуют в расчёте вотермарка для данного таймера. `streams_with_delays` дополнительно позволяет задать индивидуальную задержку для каждого потока.

- `deduplicate_equal_timestamps` — при создании нескольких таймеров с одинаковым ключом и `TriggerTimestamp` сохраняется только один (с наименьшим `EventTimestamp`). Включено по умолчанию.

## Структура таймера {#timer-structure}

{% include notitle [_](../../../flow/generated_docs/NYT_NFlow_TTimerSerializer.md) %}

## API по языкам {#api}

### C++ {#api-cpp}

```cpp
// Создать таймер в DoProcessMessage:
output->AddTimer(TSystemTimestamp(message.EventTimestamp.Underlying() + TDuration::Minutes(30).Seconds()));

// Обработать сработавший таймер:
void DoProcessTimer(const TTimer& timer, IOutputCollectorPtr output) override {
    // timer.Key, timer.EventTimestamp, timer.TriggerTimestamp
    auto builder = MakeMessageBuilder();
    // ...
    output->AddMessage(builder.Finish());
}
```

Подробнее — в разделе [Computation (C++)](../../../flow/cpp/computation.md).

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

## Примеры {#examples}

Пример реализации джойна с ожиданием (показ + клик, таймаут 30 минут):

- [C++](../../../flow/cpp/examples/wait_click_join.md)
- [Java](../../../flow/java/examples/wait_click_join.md)
- [Python](../../../flow/python/examples/wait_click_join.md)

## Ограничения и известные проблемы {#limitations}

{% note warning %}

**Память процесса.** В текущей реализации все активные таймеры дополнительно хранятся в памяти процесса помимо {{product-name}}. При большом объёме таймеров это может приводить к ошибкам Out of Memory при старте джобов.

{% endnote %}

- **`TSwiftMapComputation`** не поддерживает таймеры. Если нужна работа с таймерами — используйте `TTransformComputation`.
- Временны́е метки передаются в наносекундах (uint64).

## См. также

- [Watermarks и Timers](../../../flow/concepts/watermarks.md)
- [Stateful processing](../../../flow/concepts/stateful.md)
- [Computation (C++)](../../../flow/cpp/computation.md)
- [Computation (Java)](../../../flow/java/computation.md)
- [Computation (Python)](../../../flow/python/computation.md)
