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

По умолчанию родителем выходного сообщения считается весь текущий батч. Явный lineage позволяет сузить это множество до конкретного подмножества входных объектов, что делает вычисление `EventTimestamp` и `AlignmentTimestamp` более точным.

## API {#api}

Lineage устанавливается через метод `SetParents` / `set_parent_ids` / `setParentIds` на объекте `OutputCollector`. Метод возвращает **новый** коллектор с привязанным контекстом lineage — все вызовы `AddMessage` / `add_message` / `addMessage` на нём будут нести этот lineage.

Подробнее об использовании в каждом языке:
- [C++](../../../flow/cpp/computation.md#output-collector)
- [Java](../../../flow/java/computation.md#output-collector)
- [Python](../../../flow/python/computation.md#output-collector)

## См. также

- [Порядок обработки сообщений](../../../flow/concepts/ordering.md)
- [Computation](../../../flow/concepts/computation.md)
- [Основные понятия (глоссарий)](../../../flow/concepts/glossary.md)
