# Computation в {{product-name}} Flow

Computation — основной строительный блок [пайплайна](../../../flow/concepts/glossary.md#pipeline). Каждый Computation получает [сообщения](../../../flow/concepts/glossary.md#message) из входных [потоков](../../../flow/concepts/glossary.md#stream-and-computation), обрабатывает их и отправляет результаты в выходные потоки.

## Виды Computation {#computation-types}

Во Flow реализовано четыре базовых вида Computation, каждый из которых описан в следующих разделах.

Классы с `Swift` в названии реализуют принцип [Swift](../../../flow/concepts/swift.md) — подход к обработке данных без полной материализации, но с сохранением [exactly-once](../../../flow/concepts/glossary.md#exactly-once) гарантий и требованием к детерминированности преобразований.

В C++ эти классы задают режим исполнения и используются встроенными адаптерами. Новую пользовательскую логику реализуйте только как [process function](../../../flow/cpp/process-functions.md), а в спеке выбирайте подходящий `TProcessFunction*Computation`. Не создавайте пользовательские классы-наследники от базовых классов Computation.

### TTransformComputation {#ttransformcomputation}
Режим для произвольных преобразований входных данных. Результат обработки сохраняется в {{product-name}}, поэтому нет требований к детерминированности. Поддерживает [таймеры](../../../flow/concepts/glossary.md#timer), [стейты](../../../flow/concepts/glossary.md#state) и [Sink](../../../flow/concepts/glossary.md#sink). Пользовательскую process function запускает в этом режиме `TProcessFunctionComputation`; для passthrough-варианта без бизнес-логики используют [TPassthroughComputation](#passthrough).

### TSwiftMapComputation {#tswiftmapcomputation}
Режим детерминированного Map без материализации результатов в {{product-name}}. Не поддерживает таймеры, [Source](../../../flow/concepts/glossary.md#source) и Sink. Process function должна быть строго детерминированной — при необходимости результат будет вычислен повторно. Для пользовательской логики используют `TProcessFunctionSwiftMapComputation`, для passthrough-варианта — [TSwiftPassthroughComputation](#passthrough).

### TSwiftOrderedSourceComputation {#tswiftorderedsourcecomputation}
Source-режим для чтения данных из внешних источников. Требует, чтобы поток данных из каждого инстанса был упорядочен. Поддерживает `WatermarkStrategy` для оценки [вотермарков](../../../flow/concepts/glossary.md#timestamps-and-watermarks). Для пользовательской логики используют `TProcessFunctionSourceComputation`, для passthrough-варианта — [TSwiftPassthroughOrderedSourceComputation](#passthrough).

### TTransformOrderedSourceComputation {#ttransformorderedsourcecomputation}

Режим для обработки данных `Source` произвольной пользовательской логикой: парсинга, фильтрации или разворачивания одного сообщения в несколько. Пользовательскую process function запускает в этом режиме `TProcessFunctionTransformOrderedSourceComputation`, заменяя связку `TSwiftPassthroughOrderedSourceComputation` → `TProcessFunctionComputation`.

Результат обработки материализуется в {{product-name}}, как у `TTransformComputation`, поэтому требований к детерминированности нет: после рестарта Flow доставляет уже материализованные сообщения с ранее назначенными им `MessageId`, а не вычисляет их заново. Смещение источника, материализованные выходные сообщения и [стейты](../../../flow/concepts/stateful.md) коммитятся в одной транзакции {{product-name}} — обработка каждого сообщения источника применяется ровно один раз, включая обновления стейта.

Собственный стейт process function хранит в поле `TMutableStateKeyClient<T>`, инициализирует через `initContext->InitClient(...)` в `Init(const IRuntimeInitContextPtr&)` и читает через `GetState(message->Key)` при обработке. Пример — в разделе [Process function (C++)](../../../flow/cpp/process-functions.md).

Поддерживаются `source_streams` (ровно один упорядоченный `Source`), несколько выходных стримов, `watermark_strategy` (`watermark_generator` оценивает вотермарки источника, `watermark_alignment` выравнивает чтение, `event_timestamp_assigner` назначает `event_timestamp`), `skip_if_expression` и сообщения с `distribute = false`. Непустой `group_by_schema`, `input`-стримы, [таймеры](../../../flow/concepts/glossary.md#timer) и [key-visitor-стримы](../../../flow/concepts/key_visitor.md) приводят к ошибке валидации спеки.

## Passthrough Computation {#passthrough}

[Passthrough-компьютейшен](../../../flow/concepts/glossary.md#passthrough) не содержит пользовательской бизнес-логики: входящие сообщения конвертируются в схему выходного [стрима](../../../flow/concepts/glossary.md#stream) и передаются дальше без изменений. Используется для простого приведения схем между стримами, например при чтении очереди и перекладывании данных в другой стрим без какой-либо обработки.

В Flow реализованы три C++-класса:

| Класс | Базовый класс | Назначение |
|-------|--------------|------------|
| `TPassthroughComputation` | `TTransformComputation` | Конвертирует `input`-сообщения в схему `output`-стрима |
| `TSwiftPassthroughComputation` | `TSwiftMapComputation` | Аналогично, без материализации ([Swift](../../../flow/concepts/swift.md)) |
| `TSwiftPassthroughOrderedSourceComputation` | `TSwiftOrderedSourceComputation` | Конвертирует `source`-сообщения в `output`-стрим |

Passthrough реализуется в Flow нативно на C++ и не требует Java- или Python-компаньона. Чтобы включить его, в статической спеке компьютейшена укажите соответствующий C++-класс в поле `computation_class_name`:

```yson
"passthrough" = {
    "computation_class_name" = "NYT::NFlow::TPassthroughComputation";
    "group_by_schema" = [...];
    "input_stream_ids" = [...];
    "output_stream_ids" = [...];
};
```

Подробнее — [Computation (C++)](../../../flow/cpp/computation.md#tpassthroughcomputation).

## Общие свойства {#common-properties}
- Всё выполнение в рамках одной [партиции](../../../flow/concepts/glossary.md#partition) строго однопоточно. Многопоточность достигается за счёт увеличения числа партиций.
- Все Computation берут на себя заполнение метаполей message и timer.
- Объект `OutputCollector` предназначен для сбора выходных сообщений и таймеров.
- Метод `SetParents` позволяет управлять [lineage](../../../flow/concepts/glossary.md#lineage) сообщений для корректного расчёта метаполей.

## Реализация на разных языках

Каждый язык предоставляет свой набор интерфейсов для реализации Computation:

- **C++**: реализация process function (`IProcessFunction`, `IBatchProcessFunction` или `IKeyedBatchProcessFunction`) и выбор встроенного адаптера режима в спеке. [Подробнее →](../../../flow/cpp/process-functions.md)
- **Java**: реализация интерфейсов `RowFunction` или `BatchFunction` с методами `onMessage`/`onTimer`. [Подробнее →](../../../flow/java/computation.md)
- **Python**: наследование от `RowFunction` или `BatchFunction` с методами `on_message`/`on_timer`. [Подробнее →](../../../flow/python/computation.md)
- **Go**: реализация интерфейсов `flow.RowFunction` (`OnMessage`) или `flow.BatchFunction` (`OnMessages`); таймеры — отдельными интерфейсами `flow.RowTimerFunction`/`flow.BatchTimerFunction`. [Подробнее →](../../../flow/go/computation.md)
- **YQL**: компьютейшны генерируются автоматически по декларативному описанию. [Подробнее →](../../../flow/yql/getting-started.md)

## См. также

- [Stateful processing](../../../flow/concepts/stateful.md)
- [Вотермарки](../../../flow/concepts/watermarks.md)
- [Таймеры](../../../flow/concepts/timers.md)
- [Спеки](../../../flow/concepts/spec.md)
- [Process function (C++)](../../../flow/cpp/process-functions.md)
- [Режимы Computation (C++)](../../../flow/cpp/computation.md)
- [Computation (Java)](../../../flow/java/computation.md)
- [Computation (Python)](../../../flow/python/computation.md)
- [Computation (Go)](../../../flow/go/computation.md)
- [Computation (YQL)](../../../flow/yql/features.md)

## Фильтрация входа {#input-filter}

`skip_if_expression` фильтрует входные сообщения до дедупликации и пользовательской обработки. Пропущенные сообщения не записываются в хранилище дедупликации. Отфильтрованные сообщения подтверждаются без вызова обработчика, в том числе если отфильтрован весь батч. Статистика входа, включая распределение ключей и heavy hitters, описывает исходный поток до фильтрации.
