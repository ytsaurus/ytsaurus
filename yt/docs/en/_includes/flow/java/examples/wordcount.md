# Word Count in {{product-name}} Flow (Java)

The [pipeline](../../../../flow/concepts/glossary.md#pipeline) reads a [stream](../../../../flow/concepts/glossary.md#stream-and-computation) of words and counts how many times each word appears, using a YSON state. This example shows how to configure a [companion](../../../../flow/concepts/glossary.md#companion) with Spring Boot.

[Source code (Java)]({{source-root}}/yt/yt/flow/examples/java/word_count)
[Source code (Kotlin)]({{source-root}}/yt/yt/flow/examples/kotlin/word_count)

## Components

### WordCountApplication

This is the Spring Boot application entry point:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/word_count/wordcount/src/main/java/tech/ytsaurus/flow/examples/wordcount/WordCountApplication.java' lang='java' lines='[BEGIN word_count_application]-[END word_count_application]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/word_count/wordcount/src/main/kotlin/tech/ytsaurus/flow/examples/wordcount/WordCountApplication.kt' lang='kotlin' lines='[BEGIN word_count_application]-[END word_count_application]' keep-indents %}

{% endlist %}

### Stream registration

Typed streams are registered declaratively. `Word` is the message model for the `words` stream: `@FlowMessage` associates it with that stream, and `@Entity` marks it as a JPA entity. Spring Boot scans and registers these classes. The `mapper` computation is registered with `@FlowComputation` on `WordCountMapper`:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/word_count/wordcount/src/main/java/tech/ytsaurus/flow/examples/wordcount/model/Word.java' lang='java' lines='[BEGIN stream_context]-[END stream_context]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/word_count/wordcount/src/main/kotlin/tech/ytsaurus/flow/examples/wordcount/model/Word.kt' lang='kotlin' lines='[BEGIN stream_context]-[END stream_context]' keep-indents %}

{% endlist %}

- `reader` is a source computation without a process function; the C++ worker reads and parses its input.
- `mapper` is implemented by `WordCountMapper` with `@FlowComputation(id = "mapper")`.
- `@FlowMessage(streamIds = {"words"})` associates the Java class with the stream; the Kotlin form uses `streamIds = ["words"]`.

### WordCountMapper

This is the processing function that counts words using the [YsonStateAccessor](../../../../flow/java/state.md#yson-state):

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/word_count/wordcount/src/main/java/tech/ytsaurus/flow/examples/wordcount/WordCountMapper.java' lang='java' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/word_count/wordcount/src/main/kotlin/tech/ytsaurus/flow/examples/wordcount/WordCountMapper.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

{% endlist %}

The `@FlowComputation(id = "mapper")` annotation registers the class as a computation and makes it a Spring bean (it’s meta-annotated with `@Component`).

## Key patterns

- **Spring Boot auto-config**: you don’t need to manually create `PipelineContext` and `GrpcServerExecution`.
- **@FlowComputation**: the processing function becomes both a Spring bean and a computation; you can use dependency injection.
- **@FlowMessage**: associates a message model with a named stream.

## Running

The `WordCountApplication` class runs in two modes. With `YT_FLOW_MODE` unset, it starts the pipeline through the runner:

```bash
./run.sh tech.ytsaurus.flow.examples.wordcount.WordCountApplication --config pipeline.yson --flow-bin flow_server
```

When a worker starts the class configured as `main_class` with `YT_FLOW_MODE=Worker`, it runs as the companion process and starts the gRPC server.
