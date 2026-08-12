# Word Count in {{product-name}} Flow (Java)

The [pipeline](../../../../flow/concepts/glossary.md#pipeline) reads a [stream](../../../../flow/concepts/glossary.md#stream-and-computation) of words and counts how many times each word appears, using a YSON state. This example shows how to configure a [companion](../../../../flow/concepts/glossary.md#companion) with Spring Boot.

[Source code (Java)]({{source-root}}/yt/yt/flow/examples/java/word_count)
[Source code (Kotlin)]({{source-root}}/yt/yt/flow/examples/kotlin/word_count)

## Components

### WordCountApplication

This is the entry point for the Spring Boot–based companion:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/word_count/wordcount/src/main/java/tech/ytsaurus/flow/examples/wordcount/WordCountApplication.java' lang='java' lines='[BEGIN word_count_application]-[END word_count_application]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/word_count/wordcount/src/main/kotlin/tech/ytsaurus/flow/examples/wordcount/WordCountApplication.kt' lang='kotlin' lines='[BEGIN word_count_application]-[END word_count_application]' keep-indents %}

{% endlist %}

The gRPC server starts automatically through Spring Boot auto-config.

### WordCountContext

You declare the pipeline’s streams via `ComputationProvider` (the `getStreams()` method). The `mapper` computation is registered with the `@FlowComputation` annotation on the `WordCountMapper` class (see below):

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/word_count/wordcount/src/main/java/tech/ytsaurus/flow/examples/wordcount/WordCountContext.java' lang='java' lines='[BEGIN stream_context]-[END stream_context]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/word_count/wordcount/src/main/kotlin/tech/ytsaurus/flow/examples/wordcount/WordCountContext.kt' lang='kotlin' lines='[BEGIN stream_context]-[END stream_context]' keep-indents %}

{% endlist %}

- `reader` is a SourceComputation without a processing function. Reading and parsing happen on the C++ [worker](../../../../flow/concepts/glossary.md#worker) side.
- `mapper` is a Computation implemented by the `WordCountMapper` class with the `@FlowComputation(id = "mapper")` annotation.
- `FlowStreams.typed("words", Word.class)` registers a typed stream named `"words"`, which lets you receive messages as `Word` objects.

### WordCountMapper

This is the processing function that counts words using the [YsonStateAccessor](../../../../flow/java/state.md#yson-state):

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/word_count/wordcount/src/main/java/tech/ytsaurus/flow/examples/wordcount/WordCountMapper.java' lang='java' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/word_count/wordcount/src/main/kotlin/tech/ytsaurus/flow/examples/wordcount/WordCountMapper.kt' lang='kotlin' lines='[BEGIN on_message]-[END on_message]' keep-indents %}

{% endlist %}

The `@FlowComputation(id = "mapper")` annotation registers the class as a computation and makes it a Spring bean (it’s meta-annotated with `@Component`).

### RunnerMain

This is the entry point to start the C++ runner:

{% list tabs group=lang %}

- Java

  {% code '/yt/yt/flow/examples/java/word_count/wordcount/src/main/java/tech/ytsaurus/flow/examples/wordcount/RunnerMain.java' lang='java' lines='[BEGIN main]-[END main]' keep-indents %}

- Kotlin

  {% code '/yt/yt/flow/examples/kotlin/word_count/wordcount/src/main/kotlin/tech/ytsaurus/flow/examples/wordcount/RunnerMain.kt' lang='kotlin' lines='[BEGIN main]-[END main]' keep-indents %}

{% endlist %}

## Key patterns

- **Spring Boot auto-config**: you don’t need to manually create `PipelineContext` and `GrpcServerExecution`.
- **@FlowComputation**: the processing function becomes both a Spring bean and a computation; you can use dependency injection.
- **ComputationProvider.getStreams()**: declare the pipeline’s streams in one place.
- **FlowStreams.typed**: get typed access to messages via Java objects.

## Running

You run the pipeline with two processes:
1. **Runner** (`RunnerMain`) starts the C++ pipeline.
2. **Companion** (`WordCountApplication`) starts the Java process with the processing logic.

Both classes are in the same JAR file.

