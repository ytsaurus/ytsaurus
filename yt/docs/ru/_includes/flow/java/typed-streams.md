# Typed Streams в {{product-name}} Flow (Java)

Java SDK Flow (Java и Kotlin) поддерживает типизированные стримы через `FlowStreams.typed`. Это позволяет автоматически сериализовать и десериализовать сообщения в POJO-объекты, что упрощает работу с данными в [ProcessFunction](../../../flow/java/computation.md#rowfunction).

Бинарный формат нетипизированных (`FlowStreams.raw`) и типизированных (`FlowStreams.typed`) стримов полностью идентичен и соответствует бинарному формату `UnversionedRow` в varint энкодинге, используемому во Flow для передачи данных между узлами кластера.

## Entity

POJO-классы для стримов описываются с помощью JPA-аннотаций `@Entity` и `@Column`. При этом класс должен иметь конструктор по умолчанию.

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

      // конструкторы, геттеры, сеттеры...
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

      // конструкторы, геттеры, сеттеры...
  }
  ```

{% endlist %}

{% include notitle [_](_field_order_warning.md) %}

Если стримы зарегистрированы через `FlowStreams.raw`, сообщения будут доступны в нетипизированном виде. Получить значение полей из нетипизированного сообщения можно через `message.get("field_name", Type.class)`.

Подробнее о работе с типизированными и нетипизированными сообщениями см. [Process Function](../../../flow/java/computation.md#rowfunction).

## Column

Аннотация `@Column` является необязательной. Если на поле класса нет аннотации, то в качестве имени колонки будет использоваться имя поля.

`@Column` позволяет задать имя колонки через атрибут `name`, а также указать тип колонки через атрибут `columnDefinition`.

Атрибут `columnDefinition` принимает строку с именем Type V3. [Полный список типов](../../../flow/user-guide/storage/data-types#schema).

### Регистрация стримов

Все типизированные стримы необходимо зарегистрировать до старта companion-сервера.

#### Через аннотацию `@FlowMessage` (рекомендуется)

POJO-класс сообщения помечается аннотацией `@FlowMessage` со списком идентификаторов стримов (`streamIds`), которые он обслуживает. Аннотация используется вместе с `@Entity` (из которой выводится схема) и не заменяет её. Один POJO может обслуживать несколько стримов с одинаковой схемой — в этом случае в `streamIds` указывается несколько идентификаторов.

{% list tabs group=lang %}

- Java

  ```java
  @Entity
  @FlowMessage(streamIds = {"hit"})
  public class Hit {
      // поля...
  }
  ```

- Kotlin

  ```kotlin
  @Entity
  @FlowMessage(streamIds = ["hit"])
  class Hit {
      // поля...
  }
  ```

{% endlist %}

В Spring Boot-приложениях такие классы находятся сканированием пакетов приложения и регистрируются автоматически. По умолчанию сканируются пакеты автоконфигурации Spring Boot (пакет класса с `@SpringBootApplication` и вложенные пакеты). Дополнительные пакеты можно указать свойством `flow.entity-scan-packages`.

Без Spring Boot классы передаются напрямую в `PipelineContext.registerTypedStreams`:

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

#### Через `FlowStreams.typed` (императивно)

Типизированные стримы также можно создать и зарегистрировать вручную через фабричный метод `FlowStreams.typed`, который принимает два аргумента: `streamId` и класс сообщения. В Spring Boot-приложениях стримы можно объявить через `ComputationProvider` (метод `getStreams()`) либо как отдельные бины `FlowStream<?>`.

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
