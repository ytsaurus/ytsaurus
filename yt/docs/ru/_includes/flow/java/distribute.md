# Флаг distribute в {{product-name}} Flow (Java)

Флаг `distribute` — это per-message-флаг, задаваемый при добавлении выходного сообщения в [SourceComputation](../../../flow/java/computation.md#sourcecomputation). Он управляет тем, будет ли сообщение опубликовано дальше по графу обработки.

Флаг `distribute` обеспечивает:

- Корректную оценку [watermark](../../../flow/concepts/watermarks.md): сообщения с `distribute=false` всё равно учитываются генератором watermark (в отличие от фильтрации в `onMessage`, которая может нарушить watermark).
- Присвоение детерминированных идентификаторов сообщениям.

{% note warning %}

Чтобы отфильтровать сообщение в `SourceComputation`, не пропускайте его в `onMessage` — вместо этого эмитьте его с `distribute=false`. Так сообщение не будет опубликовано дальше, но останется учтённым при оценке watermark.

{% endnote %}

## Когда использовать distribute=false

Флаг `distribute=false` следует использовать, когда:

- Необходимо отфильтровать часть выходных сообщений на этапе source-компьютейшена.
- Важна корректная оценка watermark.

Если флаг не задан, он по умолчанию равен `true`, и сообщение публикуется дальше.

## Использование {#usage}

Логика фильтрации переносится в функцию обработки: вместо отдельного шага фильтрации сообщение эмитится с нужным флагом через перегрузку `OutputCollector.addMessage(Message, boolean)`.

{% list tabs %}

- Java

  ```java
  public class HitParsingFunction implements RowFunction {
      @Override
      public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
          var hit = ProtoUtils.parseBytes(message.get("data", byte[].class), THit.class);
          // Дубликаты эмитятся, но не публикуются дальше.
          var distribute = !hit.getHitPayload().equals("duplicate_payload");
          output.addMessage(
                  ctx.createMessageBuilder("hit")
                          .set("hit_id", hit.getHitId())
                          .set("hit_payload", hit.getHitPayload())
                          .finish(),
                  distribute
          );
      }
  }
  ```

- Kotlin

  ```kotlin
  class HitParsingFunction : RowFunction {
      override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
          val hit = ProtoUtils.parseBytes(message.get("data", ByteArray::class.java), THit::class.java)
          // Дубликаты эмитятся, но не публикуются дальше.
          val distribute = hit.hitPayload != "duplicate_payload"
          output.addMessage(
              ctx.createMessageBuilder("hit")
                  .set("hit_id", hit.hitId)
                  .set("hit_payload", hit.hitPayload)
                  .finish(),
              distribute
          )
      }
  }
  ```

{% endlist %}

## Регистрация source-компьютейшена {#registration}

Source-компьютейшен создаётся через `SourceComputation.builder()`. Отдельный параметр фильтрации больше не требуется — решение о публикации принимается в функции обработки.

```java
var hitReader = SourceComputation.builder()
        .setComputationId("hit_reader")
        .setProcessFunction(new HitParsingFunction())
        .build();
```

## См. также

- [Computation (Java)](../../../flow/java/computation.md)
- [Watermarks](../../../flow/concepts/watermarks.md)
- [Флаг distribute (Python)](../../../flow/python/distribute.md)
{% if audience == "internal" %}- [Пример lb_wait_click_join](../../../yandex-specific/flow/java/examples/lb_wait_click_join.md){% endif %}
