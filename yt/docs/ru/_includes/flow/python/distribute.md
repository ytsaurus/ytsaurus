# Флаг distribute в {{product-name}} Flow (Python)

Флаг `distribute` — это per-message-флаг, задаваемый при добавлении выходного [сообщения](../../../flow/concepts/glossary.md#message) в [SourceComputation](../../../flow/python/computation.md#sourcecomputation). Он управляет тем, будет ли сообщение опубликовано дальше по графу обработки.

Флаг `distribute` обеспечивает:

- Корректную оценку [watermark](../../../flow/concepts/watermarks.md): сообщения с `distribute=False` всё равно учитываются генератором watermark (в отличие от фильтрации в `on_message`, которая может нарушить watermark).
- Присвоение детерминированных идентификаторов сообщениям.

{% note warning %}

Чтобы отфильтровать сообщение в `SourceComputation`, не пропускайте его в `on_message` — вместо этого эмитьте его с `distribute=False`. Так сообщение не будет опубликовано дальше, но останется учтённым при оценке watermark.

{% endnote %}

## Когда использовать distribute=False

Флаг `distribute=False` следует использовать, когда:

- Необходимо отфильтровать часть выходных сообщений на этапе source-компьютейшена.
- Важна корректная оценка watermark.

Если флаг не задан, он по умолчанию равен `True`, и сообщение публикуется дальше.

## Использование {#usage}

Логика фильтрации переносится в функцию обработки: вместо отдельного шага фильтрации сообщение эмитится с нужным флагом.

```python
from yt.yt.flow.library.python.companion.computation import RowFunction


class HitParsingFunction(RowFunction):
    def on_message(self, message, output, ctx):
        builder = ctx.message_builder("hit")
        builder.set("hit_id", message.payload["hit_id"])
        builder.set("hit_payload", message.payload["hit_payload"])
        # Дубликаты эмитятся, но не публикуются дальше.
        is_duplicate = message.payload["hit_payload"] == "duplicate_payload"
        output.add_message(builder.finish(), distribute=not is_duplicate)
```

## Регистрация source-компьютейшена {#registration}

Source-компьютейшен регистрируется через `Pipeline.add()` с `source=True`. Отдельный параметр фильтрации больше не требуется — решение о публикации принимается в функции обработки.

```python
from yt.yt.flow.library.python.companion import Pipeline

pipeline = Pipeline()
pipeline.add("hit_reader", HitParsingFunction(), source=True)
```

## См. также

- [Computation (Python)](../../../flow/python/computation.md)
- [Watermarks](../../../flow/concepts/watermarks.md)
- [Флаг distribute (Java)](../../../flow/java/distribute.md)
{% if audience == "internal" %}- [Пример lb_wait_click_join](../../../yandex-specific/flow/python/examples/lb_wait_click_join.md){% endif %}
