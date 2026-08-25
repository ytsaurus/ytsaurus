# Lineage в {{product-name}} Flow

Lineage *(с англ. родословная)* — это информация о том, из каких входных [сообщений](../../../flow/concepts/glossary.md#message) и [таймеров](../../../flow/concepts/glossary.md#timer) был получен конкретный выходной результат [компьютейшена](../../../flow/concepts/glossary.md#computation). Эта информация используется фреймворком в момент обработки для вычисления метаданных и обеспечения гарантий порядка, но не сохраняется в самом выходном сообщении.

## Зачем нужен lineage {#why-lineage}

Lineage используется фреймворком в двух целях:

1. **Вычисление метаполей.** На основе родительских сообщений Flow автоматически заполняет метаполя выходных сообщений: `EventTimestamp`, `AlignmentTimestamp` и другие. Для Swift-компьютейшенов и [passthrough](../../../flow/concepts/glossary.md#passthrough) `AlignmentTimestamp` наследуется от родителей без изменений — это гарантирует корректную [приоритизацию](../../../flow/concepts/ordering.md) сообщений в downstream-компьютейшенах.

2. **Гарантии порядка производных сообщений.** Если у двух сообщений совпадают [ключи группировки](../../../flow/concepts/glossary.md#key) по всей цепочке lineage от источника до текущего компьютейшена, то их относительный порядок обработки сохраняется. Подробнее — в разделе [Порядок обработки сообщений](../../../flow/concepts/ordering.md#ordering-guarantees).

## Поведение по умолчанию {#default-behavior}

В большинстве случаев явно управлять lineage не нужно — фреймворк устанавливает родителей автоматически:

| Тип функции | Родитель выходного сообщения |
|---|---|
| `RowFunction` / `DoProcessMessage` | текущее входное сообщение |
| `BatchFunction` / `DoProcess` | все сообщения текущего батча |
| Обработчик таймера | текущий таймер |

## Когда задавать lineage явно {#explicit-lineage}

Родителями указываются входные сообщения **текущего вызова** batch-функции, из которых фактически получен данный вывод (в SDK компаньонов — их `message_id`); сообщения из других батчей родителями быть не могут.

Обязательность зависит от типа компьютейшена:

- **Swift**: у каждого выходного сообщения должен быть **ровно один родитель**, поэтому в batch-функции с батчем из более чем одного сообщения задавать lineage **обязательно** — сужайте родителей до одного входного сообщения. С дефолтом «весь батч» обработка завершится ошибкой `Message should have exactly one parent message`. Несколько родителей у выходного сообщения допустимы только при включённом параметре [`allow_batching_with_relaxed_guarantees`](../../../flow/concepts/guarantees.md#swift-allow-batching-with-relaxed-guarantees).
- **Transform**: задавать lineage необязательно, но с дефолтом «весь батч» `EventTimestamp` каждого выходного сообщения (если он не задан явно при построении) равен минимальному по всему батчу, что искажает event time и [watermark](../../../flow/concepts/watermarks.md). Сужайте родителей, если пайплайн использует event time.

В row-функциях (`RowFunction` / `DoProcessMessage`) явный lineage не нужен: фреймворк сам назначает родителем текущее входное сообщение.

## API {#api}

Lineage устанавливается через метод `SetParents` / `set_parent_ids` / `setParentIds` / `WithParentIDs` на объекте `OutputCollector`. Метод возвращает **новый** коллектор с привязанным контекстом lineage — все вызовы `AddMessage` / `add_message` / `addMessage` на нём будут нести этот lineage.

Подробнее об использовании в каждом языке:
- [C++](../../../flow/cpp/computation.md#output-collector)
- [Java](../../../flow/java/computation.md#output-collector)
- [Python](../../../flow/python/computation.md#output-collector)
- [Go](../../../flow/go/computation.md#output-collector)

## См. также

- [Порядок обработки сообщений](../../../flow/concepts/ordering.md)
- [Computation](../../../flow/concepts/computation.md)
- [Основные понятия (глоссарий)](../../../flow/concepts/glossary.md)
- [Computation (Go)](../../../flow/go/computation.md)
