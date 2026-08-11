# Typed Streams in {{product-name}} Flow (Java)

Use the Java SDK Flow (Java and Kotlin) to work with typed streams via `FlowStreams.typed`. This lets you automatically serialize and deserialize messages into POJO objects, which simplifies working with data in [ProcessFunction](../../../flow/java/computation.md#rowfunction).

The binary format for untyped (`FlowStreams.raw`) and typed (`FlowStreams.typed`) streams is fully identical. It matches the binary format of `UnversionedRow` in varint encoding, which Flow uses to transfer data between cluster nodes.

## Entity

You describe POJO classes for streams using the JPA annotations `@Entity` and `@Column`. Make sure the class has a default constructor.

{% list tabs group=lang %}

- Java

  ```java
  @Entity
  public class Hit {
      @Column(name = "hit_id")
      private String hitId;

      @Column(name = "hit_time", columnDefinition = "uint64")
      private Long hitTime;

      @Column(name = "hit_payload")
      private String hitPayload;

      // constructors, getters, setters...
  }
  ```

- Kotlin

  ```kotlin
  @Entity
  class Hit {
      @Column(name = "hit_id")
      var hitId: String? = null

      @Column(name = "hit_time", columnDefinition = "uint64")
      var hitTime: Long? = null

      @Column(name = "hit_payload")
      var hitPayload: String? = null

      // constructors, getters, setters...
  }
  ```

{% endlist %}

{% include notitle [_](_field_order_warning.md) %}

If you register streams via `FlowStreams.raw`, the messages are available in an untyped form. To get field values from an untyped message, use `message.get("field_name", Type.class)`.

For more details on working with typed and untyped messages, see [Process Function](../../../flow/java/computation.md#rowfunction).

## Column

The `@Column` annotation is optional. If a class field doesn’t have this annotation, the field name is used as the column name.

With `@Column`, you can set the column name via the `name` attribute and specify the column type via the `columnDefinition` attribute.

The `columnDefinition` attribute accepts a string with a Type V3 name. See the [full list of types](../../../flow/user-guide/storage/data-types#schema).

### Registering streams

You must register all typed streams in `PipelineContext`.

Create typed streams using the factory method `FlowStreams.typed`, which takes two arguments: `streamId` and the message class.

{% list tabs group=lang %}

- Java

  ```java
  context.registerStream(FlowStreams.typed("hit", Hit.class));
  context.registerStream(FlowStreams.typed("action", Action.class));
  context.registerStream(FlowStreams.typed("joined_action", JoinedAction.class));
  ```

- Kotlin

  ```kotlin
  context.registerStream(FlowStreams.typed("hit", Hit::class.java))
  context.registerStream(FlowStreams.typed("action", Action::class.java))
  context.registerStream(FlowStreams.typed("joined_action", JoinedAction::class.java))
  ```

{% endlist %}