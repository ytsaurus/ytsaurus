# Watermarks в {{product-name}} Flow

## Зачем нужно время в потоковой обработке {#why-time-matters}

Представьте, что вы сортируете письма по дате написания, но они приходят к вам не по порядку. Одно письмо от 1 марта пришло сегодня, другое от 5 марта — вчера. Как понять, что все письма за март уже получены и можно подводить итог? Вотермарк (watermark) — это именно такой сигнал: «все события старше момента X уже получены и обработаны».

В реальных системах ситуация ещё интереснее. Допустим, рекламный показ зафиксирован в системе в 10:05, но пользователь увидел рекламу в 10:02. Какое время считать «настоящим»? Время фиксации в системе (10:05) или время самого события (10:02)? Ответ зависит от задачи, и для каждого из этих случаев нужна своя шкала.

Flow поддерживает обе шкалы времени — `SystemTimestamp` (время фиксации) и `EventTimestamp` (время события) — а также механизм Watermarks для отслеживания прогресса обработки. Ниже разберём каждый из этих элементов.

В данный момент во Flow отсутствует понятие `Window`, которое есть в [The Dataflow Model](https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/43864.pdf) и [Apache Beam](https://beam.apache.org/). Однако присутствуют базовые кубики, позволяющие определять `EventTimestamp`, вычислять `EventWatermark` и заводить `Timers`.

## Две шкалы времени: SystemTimestamp и EventTimestamp {#two-timescales}

### SystemTimestamp

Все сообщения в Flow в обязательном порядке имеют поле `SystemTimestamp`, хранящее в себе время создания события. Для `source` потоков - это время создания события во внешней системе. Для всех остальных - строго время `YT`.

### EventTimestamp

У всех сообщений во `Flow` должно быть заполнено поле `EventTimestamp`. Это время самого события в терминах бизнес-логики: например, момент клика пользователя или время записи в лог на стороне клиента. `EventTimestamp` задаётся с помощью `WatermarkStrategy` в `SourceComputation`.

## SystemWatermark

Все публичные сообщения (типа `input` и `output`) имеют `SystemTimestamp`. `SystemWatermark` принимает такое значение, что любое возможное сообщение с `SystemTimestamp < SystemWatermark` можно считать обработанным. Исключение - `source` потоки. Так как это внешняя система, то `SystemWatermark` для них может быть неточным.

`SystemWatermark` используется для оценки временного лага всех потоков.

Так как `Flow` сам назначает все `SystemTimestamp` (и знает текущее время {{product-name}}), а также поддерживает информацию о текущем `inflight`, то можно надёжно поддерживать значение `SystemWatermark` достаточно актуальным. В данный момент можно рассчитывать на задержку до единиц минут.

Можно утверждать, что `SystemWatermark` для `input` и `output` сообщений абсолютно точен. Поэтому таблицу дедупликации входных сообщений (`input_messages` или `compact_input_messages`) с уже обработанными `message_id` можно очищать с его помощью.

## EventWatermark {#event-watermark}

С помощью настроенного `WatermarkStrategy` в `SourceComputation` происходит оценка `EventWatermark` в разных точках системы.

`WatermarkStrategy` содержит три основных модуля:
- `EventTimestampAssigner` &mdash; для автоматического заполнения EventTimestamp на основе определенной колонки `output` потоков.
- `WatermarkGenerator` &mdash; настройки генератора `EventWatermark`. Позволяет настроить `out_of_orderness_bound`, а также обработку `idle` и `unavailable` партициями.
- `WatermarkAlignment` &mdash; специальный модуль для выравнивания чтения.

Детальнее в [спеке](../../../flow/concepts/spec.md#watermark-strategy).

## Late Data

Late Data &mdash; понятие для сообщений, которые пришли в нарушение `EventWatermark`, оно введено в статье про [The Dataflow Model](https://static.googleusercontent.com/media/research.google.com/ru//pubs/archive/43864.pdf).

О способах обработки данных событий подробно можно прочитать в статье.

## WatermarkGenerator

Данный модуль для source [компьютейшенов](../../../flow/concepts/glossary.md#stream-and-computation) пытается дать оценку снизу для `EventTimestamp` будущих событий по всем `output` потокам . Алгоритм устроен так:

1. Оценивается `EventWatermark` для каждой [партиции](../../../flow/concepts/glossary.md#partition) входной очереди независимо.
2. Берётся минимум по всем партициям.
3. Для оценки `EventWatermark` конкретной партиции берётся `max(MaxTimestamp – OutOfOrdernessBound, min(MinAheadTimestamp, MaxAheadTimestamp - OutOfOrdernessBound))`, где:

   * `MaxTimestamp` &mdash; максимальное встретившееся время в данной партиции;
   * `OutOfOrdernessBound` &mdash; определённый в спеке параметр, характеризующий задержки записи данных в очередь.
   * `MinAheadTimestamp` &mdash; минимальное время в ещё необработанном следующем батче данных.
   * `MaxAheadTimestamp` &mdash; максимальное время в ещё необработанном следующем батче данных.
4. Недоступные партиции, а также партиции, в которые не идёт записи, могут ограниченно игнорироваться. Настройки этих эвристик можно найти в [описании спеки](../../../flow/concepts/spec.md#watermark-generator)

Данный подход неоптимален. В будущем планируется:
- Считать `MaxTimestamp` в нескольких разрезах{% if audience == "internal" %} (тикет [YTFLOW-64](https://nda.ya.ru/t/cHIENl-i7gKZHC)){% endif %} &mdash; для защиты от кейсов вида «показы происходят, а клики — нет».
- Поддерживать значение `MinTimestamp` на последних N сообщениях из партиции{% if audience == "internal" %} (тикет [YTFLOW-65](https://nda.ya.ru/t/DuDC3wCs7gKZHJ)){% endif %} &mdash; для защиты от кейсов, когда в партицию пишут несколько поставщиков, и один из них отстал значительно сильнее, чем на `OutOfOrdernessBound`.

{% note info %}

Также планируется создать протокол для передачи между разными [пайплайнами](../../../flow/concepts/glossary.md#pipeline) информации про `Watermark`.

{% endnote %}

## WatermarkState

Flow поддерживает на каждом потоке и каждой `alignment` группе значения `EventWatermark` и `SystemWatermark`.

## Timers {#timers}

Таймер позволяет `TransformComputation` сказать: «разбуди меня, когда `EventWatermark` достигнет времени X». Это основной инструмент для отложенной обработки, джойнов с ожиданием и агрегаций с окнами.

Таймеры тесно связаны с вотермарками: именно `EventWatermark` (по умолчанию) определяет момент срабатывания таймера. Подробнее о том, как устроены таймеры, их конфигурации и API — в разделе [Таймеры](../../../flow/concepts/timers.md).

## Конфигурация {#configuration}

Настройка `WatermarkStrategy` (`EventTimestampAssigner`, `WatermarkGenerator`, `WatermarkAlignment`) описана в разделе [Spec](../../../flow/concepts/spec.md#watermark-strategy).

## См. также

- [Таймеры](../../../flow/concepts/timers.md)
- [Порядок обработки сообщений](../../../flow/concepts/ordering.md)
- [Spec и DynamicSpec](../../../flow/concepts/spec.md)
