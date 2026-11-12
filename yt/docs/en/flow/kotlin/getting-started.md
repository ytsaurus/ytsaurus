# Getting started with {{product-name}} Flow (Kotlin)

Kotlin code in Flow runs on the same [Java SDK](../../flow/java/getting-started.md) as Java code: there is no separate Kotlin SDK. Computations, states, typed streams, the Spring Boot integration, and the test harness are documented in the Java section. This page covers Kotlin-specific build, entry-point, JVM-artifact, and nullability details.

[Java SDK source code for Flow]({{source-root}}/yt/java/flow)

[Kotlin examples]({{source-root}}/yt/yt/flow/examples/kotlin)

For process-shared objects, use the [Java companion resource API](../../flow/java/resources.md), including `FlowResource`, factory registration and `RuntimeContext.getResource`. Kotlin uses the same lifecycle and spec bindings.

## How Kotlin runs in Flow {#architecture}

Kotlin computations run through the [companion](../../flow/concepts/glossary.md#companion) mechanism: the [worker](../../flow/concepts/glossary.md#worker) spawns a JVM process with your code and exchanges messages with it over gRPC. The pipeline spec uses the same Java companion as Java, and `main_class` is always required.

The JVM runner resolves the job environment and ships companion jars only for an enabled Vanilla launch. In a docker job, `jdk_bin_path` takes priority over `YT_FLOW_JDK_BIN_PATH`, followed by the path of the JVM that runs the launcher; whichever path is selected must exist in the job image and provide a compatible Java runtime. In porto mode with the supplied JDK layer, `YT_FLOW_JDK_BIN_PATH` can override the layer path, while a different `jdk_bin_path` in the resource is ignored. If the worker declares its own layers, the supplied JDK layer is not used, so set a compatible `jdk_bin_path` or `YT_FLOW_JDK_BIN_PATH`. Leave `classpath` out for jars that the Vanilla launcher should ship; a declared classpath is preserved, and a mixed spec still ships jars for resources without one. A direct custom deployment{% if audience == "internal" %}, such as Deploy / YP,{% endif %} does not receive this Vanilla enrichment: provide the JDK, jars, and all companion resource parameters in the worker environment. See [Vanilla](../devops/vanilla/initial-deploy.md) and [docker](../devops/docker-environment.md). The Runner and Worker roles are described in [Application architecture (Java)](../../flow/java/getting-started.md#architecture).

## Building with ya make {#build}

A Kotlin pipeline is built by the `JAVA_PROGRAM` module with the Kotlin compiler enabled. The [Word Count]({{source-root}}/yt/yt/flow/examples/kotlin/word_count) example uses `WITH_KOTLIN()`, `JAVA_SRCS(SRCDIR src/main/kotlin **/*.kt)`, and `WITH_KOTLINC_ALLOPEN(preset=spring)` for Spring classes that Kotlin makes `final` by default.

The complete module is kept in the example source:

{% code '/yt/yt/flow/examples/kotlin/word_count/wordcount/ya.make' lang='text' %}

```bash
cd yt/yt/flow
ya make examples/kotlin/word_count/wordcount bin/flow_server
```

For this `JAVA_PROGRAM(wordcount_kotlin_bin)` module, the build produces `run.sh`, the program jar `wordcount_kotlin_bin.jar`, dependency jars in the `wordcount_kotlin_bin/` directory, and `jdk.tar`. The module targets JDK 21, and `WITH_JDK()` packs that JDK into `jdk.tar`; it does not automatically unpack or deploy a `jdk/` directory. On the launch host, make a Java 21 runtime available through `JAVA_HOME`, or pass its `java` binary with a leading `run.sh -j <path/to/java>` option. The `-j` option takes priority over `JAVA_HOME`. The separate `flow-java` docker example uses Java 17, so do not assume that image can run this JDK 21 module; the selected job path or image must provide a compatible runtime.

Keep `run.sh`, `wordcount_kotlin_bin.jar`, and `wordcount_kotlin_bin/` in this relative layout and run the script from the directory that contains them: its classpath and JDK probes are relative to the current directory, and the script does not change to its own directory. The generated `run.sh` has no embedded main class, so the first argument after an optional leading `-j` must be the same fully qualified entry-point class as the companion resource's `main_class`.

For an enabled Vanilla launch, leave `classpath` out of companion resource parameters when the runner should ship these jars. A declared `classpath` is preserved and tells the runner that the resource's jars are already in the job environment. If every Java companion resource declares `classpath`, the runner ships no companion jars; in a mixed spec, it still ships jars for resources without `classpath`.

## Entry point {#entry-point}

The entry point is a class with a JVM-static `main` method. In Kotlin, declare it with `@JvmStatic` in an `object` or a `companion object`:

{% code '/yt/yt/flow/examples/kotlin/word_count/wordcount/src/main/kotlin/tech/ytsaurus/flow/examples/wordcount/WordCountApplication.kt' lang='kotlin' lines='[BEGIN word_count_application]-[END word_count_application]' keep-indents %}

The companion `main_class` is the fully qualified name of that class. For Kotlin Word Count it is `tech.ytsaurus.flow.examples.wordcount.WordCountApplication`. A top-level `fun main` compiles to a class with a `Kt` suffix, so an `object` or `companion object` with `@JvmStatic` keeps the class name explicit.

## Starting a pipeline {#launch}

Run the command from the directory that contains `run.sh`, the program jar, and the dependency directory. The first argument after an optional leading `-j <path/to/java>` is the fully qualified entry-point class, consumed by `run.sh`. The remaining arguments go to the runner. It requires `--config`; `--flow-bin` is optional when `YT_FLOW_BIN` contains the path to `flow_server`:

```bash
./run.sh tech.ytsaurus.flow.examples.wordcount.WordCountApplication --config pipeline.yson --flow-bin <path/to/flow_server>
```

For a Vanilla operation, see [Initial deployment](../devops/vanilla/initial-deploy.md). For docker execution, see [Running in a docker environment](../devops/docker-environment.md).

## Nullability {#nullability}

The Java SDK uses [JSpecify](https://jspecify.dev/) annotations. Kotlin 2.1+ enforces them strictly: a `@Nullable` result maps to `T?`, and a nullable value cannot be passed to a non-null parameter. Kotlin examples check the annotations in CI.

## Testing {#testing}

Kotlin process-function unit tests use the same `flow-test-utils` harness as Java tests; see [Testing (Java)](../../flow/java/testing.md). The test module uses `JUNIT6()` and `WITH_KOTLIN()`; see [`src/test/ya.make`]({{source-root}}/yt/yt/flow/examples/kotlin/word_count/wordcount/src/test/ya.make). Integration tests use the same JVM entry-point class as the companion resource.

## Examples {#examples}

Kotlin source examples include [Word Count]({{source-root}}/yt/yt/flow/examples/kotlin/word_count), [Shuffle]({{source-root}}/yt/yt/flow/examples/kotlin/shuffle), [Async Request]({{source-root}}/yt/yt/flow/examples/kotlin/async_request), [Retryable Async Request]({{source-root}}/yt/yt/flow/examples/kotlin/retryable_async_request), [Wait Click Join]({{source-root}}/yt/yt/flow/examples/kotlin/wait_click_join), [URL Downloader]({{source-root}}/yt/yt/flow/examples/kotlin/url_downloader), [Static Table Join]({{source-root}}/yt/yt/flow/examples/kotlin/static_table_join), and [External State Join]({{source-root}}/yt/yt/flow/examples/kotlin/external_state_join). The Java section explains the shared API.

## See also

- [Getting started (Java)](../../flow/java/getting-started.md)
- [Computation (Java)](../../flow/java/computation.md)
- [Spring Boot integration](../../flow/java/spring.md)
- [The companion](../../flow/concepts/companion.md)
