# The distribute flag in {{product-name}} Flow (Java)

The `distribute` flag is a per-message flag that you set when you add an output message to [SourceComputation](../../../flow/java/computation.md#sourcecomputation). It controls whether the message is published further along the processing graph.

The `distribute` flag ensures:

- Correct watermark evaluation: messages with `distribute=false` are still accounted for by the watermark generator (unlike filtering in `onMessage`, which can break the watermark).
- Assignment of deterministic identifiers to messages.

{% note warning %}

To filter a message in `SourceComputation`, don’t skip it in `onMessage`. Instead, emit it with `distribute=false`. This way, the message isn’t published further, but it remains accounted for in watermark evaluation.

{% endnote %}

## When to use distribute=false

Use the `distribute=false` flag when:

- You need to filter some output messages at the source computation stage.
- Correct watermark evaluation is important.

If you don’t set the flag, it defaults to `true`, and the message is published further.

## Usage {#usage}

Move the filtering logic to the processing function: instead of a separate filtering step, you emit the message with the required flag via the `OutputCollector.addMessage(Message, boolean)` overload.

{% list tabs %}

- Java

  ```java
  public class HitParsingFunction implements RowFunction {
      @Override
      public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
          var hit = ProtoUtils.parseBytes(message.get("data", byte[].class), THit.class);
          // Duplicates are emitted but not published further.
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
          // Duplicates are emitted but not published further.
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

## Registering a source computation {#registration}

You create a source computation using `SourceComputation.builder()`. You no longer need a separate filtering parameter — the decision about publishing is made in the processing function.

```java
var hitReader = SourceComputation.builder()
        .setComputationId("hit_reader")
        .setProcessFunction(new HitParsingFunction())
        .build();
```

## See also

- [Computation (Java)](../../../flow/java/computation.md)
- [Watermarks](../../../flow/concepts/watermarks.md)
- [The distribute flag (Python)](../../../flow/python/distribute.md)
{% if audience == "internal" %}- [Example lb_wait_click_join](../../../yandex-specific/flow/java/examples/lb_wait_click_join.md){% endif %}