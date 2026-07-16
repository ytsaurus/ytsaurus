# Computation в {{product-name}} Flow

Computation — основной строительный блок [пайплайна](../../../flow/concepts/glossary.md#pipeline). Каждый Computation получает [сообщения](../../../flow/concepts/glossary.md#message) из входных [потоков](../../../flow/concepts/glossary.md#stream-and-computation), обрабатывает их и отправляет результаты в выходные потоки.

## Виды Computation {#computation-types}

Во Flow реализовано три базовых вида Computation, каждый из которых описан в следующих разделах.

Классы с `Swift` в названии реализуют принцип [Swift](../../../flow/concepts/swift.md) — подход к обработке данных без полной материализации, но с сохранением [exactly-once](../../../flow/concepts/glossary.md#exactly-once) гарантий и требованием к детерминированности преобразований.

### TTransformComputation {#ttransformcomputation}
Предназначен для произвольных преобразований входных данных. Результат обработки сохраняется в {{product-name}}, поэтому нет требований к детерминированности. Поддерживает [таймеры](../../../flow/concepts/glossary.md#timer), [стейты](../../../flow/concepts/glossary.md#state) и [Sink](../../../flow/concepts/glossary.md#sink). Для passthrough-варианта без бизнес-логики используют [TPassthroughComputation](#passthrough).

### TSwiftMapComputation {#tswiftmapcomputation}
Реализует детерминированный Map без материализации результатов в {{product-name}}. Не поддерживает таймеры, [Source](../../../flow/concepts/glossary.md#source) и Sink. Функция преобразования должна быть строго детерминированной — при необходимости результат будет вычислен повторно. Для passthrough-варианта используют [TSwiftPassthroughComputation](#passthrough).

### TSwiftOrderedSourceComputation {#tswiftorderedsourcecomputation}
Основной класс для чтения данных из внешних источников. Требует, чтобы поток данных из каждого инстанса был упорядочен. Поддерживает `WatermarkStrategy` для оценки [вотермарков](../../../flow/concepts/glossary.md#timestamps-and-watermarks). Для passthrough-варианта используют [TSwiftPassthroughOrderedSourceComputation](#passthrough).

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

- **C++**: наследование от базовых классов (`TTransformComputation`, `TSwiftMapComputation` и т.д.) с переопределением методов `DoProcessMessage`/`DoProcessTimer`. [Подробнее →](../../../flow/cpp/computation.md)
- **Java**: реализация интерфейсов `RowFunction` или `BatchFunction` с методами `onMessage`/`onTimer`. [Подробнее →](../../../flow/java/computation.md)
- **Python**: наследование от `RowFunction` или `BatchFunction` с методами `on_message`/`on_timer`. [Подробнее →](../../../flow/python/computation.md)
- **YQL**: компьютейшны генерируются автоматически по декларативному описанию. [Подробнее →](../../../flow/yql/getting-started.md)

## См. также

- [Stateful processing](../../../flow/concepts/stateful.md)
- [Вотермарки](../../../flow/concepts/watermarks.md)
- [Таймеры](../../../flow/concepts/timers.md)
- [Спеки](../../../flow/concepts/spec.md)
- [Computation (C++)](../../../flow/cpp/computation.md)
- [Computation (Java)](../../../flow/java/computation.md)
- [Computation (Python)](../../../flow/python/computation.md)
- [Computation (YQL)](../../../flow/yql/features.md)
