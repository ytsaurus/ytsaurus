# Typed Streams in {{product-name}} Flow (Java)

Use the Java SDK Flow (Java and Kotlin) to work with typed streams: declare them with the `@FlowMessage` annotation (the recommended way) or register them imperatively via `FlowStreams.typed`. Either way, messages are serialized and deserialized into POJO objects automatically, which simplifies working with data in [ProcessFunction](../../../flow/java/computation.md#rowfunction). Both registration modes are described in [Registering streams](#registering-streams).

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

The `columnDefinition` attribute accepts a string with a Type V3 name. See the [full list of types](../../../user-guide/storage/data-types.md#schema).

### Registering streams

You must register all typed streams before the companion server starts.

#### Via the `@FlowMessage` annotation (recommended)

Mark the message POJO class with the `@FlowMessage` annotation listing the stream identifiers (`streamIds`) it serves. The annotation is used together with `@Entity`, which the schema is derived from, and doesn’t replace it. A single POJO can serve several streams with the same schema; in that case, list several identifiers in `streamIds`.

{% list tabs group=lang %}

- Java

  ```java
  @Entity
  @FlowMessage(streamIds = {"hit"})
  public class Hit {
      // fields...
  }
  ```

- Kotlin

  ```kotlin
  @Entity
  @FlowMessage(streamIds = ["hit"])
  class Hit {
      // fields...
  }
  ```

{% endlist %}

In Spring Boot applications, such classes are found by scanning the application packages and are registered automatically. By default, the Spring Boot autoconfiguration packages are scanned: the package of the class annotated with `@SpringBootApplication` and its nested packages. You can specify additional packages with the `flow.entity-scan-packages` property.

Without Spring Boot, pass the classes directly to `PipelineContext.registerTypedStreams`:

{% list tabs group=lang %}

- Java

  ```java
  context.registerTypedStreams(Hit.class, Action.class, JoinedAction.class);
  ```

- Kotlin

  ```kotlin
  context.registerTypedStreams(Hit::class.java, Action::class.java, JoinedAction::class.java)
  ```

{% endlist %}

#### Via `FlowStreams.typed` (imperative)

You can also create and register typed streams manually via the `FlowStreams.typed` factory method, which takes two arguments: `streamId` and the message class. In Spring Boot applications, you can declare streams via `ComputationProvider` (the `getStreams()` method) or as separate `FlowStream<?>` beans.

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