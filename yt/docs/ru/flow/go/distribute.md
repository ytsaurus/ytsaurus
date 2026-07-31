# Флаг distribute в {{product-name}} Flow (Go)

Флаг `distribute` — это per-message-флаг, задаваемый при добавлении выходного [сообщения](../../flow/concepts/glossary.md#message) в [source-компьютейшене](getting-started.md#computation-and-source). Он управляет тем, будет ли сообщение опубликовано дальше по графу обработки.

Флаг `distribute` обеспечивает:

- Корректную оценку [вотермарка](../../flow/concepts/watermarks.md): сообщения с `distribute=false` всё равно учитываются генератором вотермарка (в отличие от простого пропуска сообщения в обработчике, который ломает оценку вотермарка).
- Присвоение детерминированных идентификаторов сообщениям.

{% note warning %}

Чтобы отфильтровать сообщение в сорсе, не пропускайте его в `OnMessage` — вместо этого добавьте его с `distribute=false`. Так сообщение не будет опубликовано дальше, но останется учтённым при оценке вотермарка.

{% endnote %}

## Когда использовать distribute=false {#when-to-use}

Флаг `distribute=false` следует использовать, когда:

- Необходимо отфильтровать часть выходных сообщений на этапе source-компьютейшена.
- Важна корректная оценка вотермарка.

Метод `out.AddMessage(msg)` публикует сообщение дальше. `out.AddUndistributedMessage(msg)` оставляет его учтённым, но не публикует.

## Использование {#usage}

Логика фильтрации переносится в функцию обработки: обычное сообщение добавляется через `AddMessage`, отфильтрованное — через `AddUndistributedMessage`.

```go
type hitMessage struct {
    flow.YSONMessage
    HitID      uint64 `yson:"hit_id"`
    HitPayload string `yson:"hit_payload"`
}

// hitParsingFunction разбирает входную строку и отбрасывает дубликаты.
type hitParsingFunction struct{}

var _ flow.RowFunction = (*hitParsingFunction)(nil)

func (*hitParsingFunction) OnMessage(
    ctx context.Context,
    rt flow.Runtime,
    msg flow.ExtendedMessage,
    out flow.OutputCollector,
) error {
    var input hitMessage
    if err := msg.ConvertTo(&input); err != nil {
        return err
    }

    hit := flow.NewYSONMessage[hitMessage]("hit")
    hit.HitID = input.HitID
    hit.HitPayload = input.HitPayload
    encoded, err := flow.ConvertFrom(rt, hit)
    if err != nil {
        return err
    }

    // Дубликаты добавляются, но не публикуются дальше.
    isDuplicate := input.HitPayload == "duplicate_payload"
    if isDuplicate {
        out.AddUndistributedMessage(encoded)
    } else {
        out.AddMessage(encoded)
    }
    return nil
}
```

Флаг задаётся отдельно для каждого сообщения, поэтому в одном вызове обработчика часть сообщений может публиковаться, а часть — нет. Порядок сообщений при этом сохраняется: отброшенное сообщение остаётся в выходной группе и участвует в [линиидже](../../flow/concepts/lineage.md), просто не уходит дальше по графу.

## Флаг читается только на source-пути {#source-path-only}

[Воркер](../../flow/concepts/glossary.md#worker) читает флаг `distribute` только на пути сорса. Поэтому Go SDK отвергает `AddUndistributedMessage` в трансформе ошибкой `flow.ErrDistributeOnTransform`.

{% note info %}

Трансформ фильтрует сообщение тем, что просто не собирает его: не вызывайте для него `AddMessage`. Вотермарк при этом не нарушается — его двигают входные сообщения сорса, а не выход трансформа.

{% endnote %}

## Регистрация source-компьютейшена {#registration}

Source-компьютейшен создаётся конструктором `flow.NewRowSourceComputation` (или `flow.NewBatchSourceComputation`) и регистрируется в пайплайне через `pipeline.Add`. Отдельный параметр фильтрации не требуется — решение о публикации принимается в функции обработки:

{% code '/yt/yt/flow/examples/go/shuffle/main.go' lang='go' %}

Тип компьютейшена — это то, чем он был создан: сорс отличается от трансформа только тем, каким он объявляется воркеру. Поэтому флаг `distribute` учитывается ровно у тех компьютейшенов, которые созданы source-конструкторами.

## Проверка в тестах {#testing}

Значения флага видны в офлайн-тестах: метод `Distribute()` результата прогона возвращает флаги в том же порядке, что `Messages()` и `Rows()`.

```go
r := h.Process(
    h.Message("hits", flowtest.Row{"hit_id": uint64(1), "hit_payload": "payload"}),
    h.Message("hits", flowtest.Row{"hit_id": uint64(2), "hit_payload": "duplicate_payload"}),
)

require.Equal(t, []bool{true, false}, r.Distribute())
```

Подробнее — в разделе [Тестирование (Go)](testing.md).

## См. также

- [Computation (Go)](computation.md)
- [Тестирование (Go)](testing.md)
- [Примеры: Shuffle (Go)](examples/shuffle.md)
- [Watermarks](../../flow/concepts/watermarks.md)
- [Флаг distribute (Python)](../../flow/python/distribute.md)
