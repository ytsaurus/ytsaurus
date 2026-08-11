# Testing with TestComputationHarness in {{product-name}} Flow (Java)

The `flow-test-utils` module provides utilities for unit testing Flow pipeline components in Java and Kotlin without running a real gRPC server or C++ workers. The central class is [`TestComputationHarness`]({{source-root}}/yt/java/flow/flow-test-utils/src/main/java/tech/ytsaurus/flow/harness/TestComputationHarness.java), which emulates `doProcess` calls at the companion level.

{% note info %}

The `TestComputationHarness` class is designed for **unit testing** individual `ProcessFunction` instances without launching a real pipeline. For **integration testing** of the full pipeline (with real C++ workers, queues, and streams), use `FlowTestJavaBase` — see the [section below](#integration-testing).

{% endnote %}

## General testing architecture {#architecture}

In production, the C++ worker sends gRPC requests to the companion, passing messages, timers, state, and watermarks. The `Companion` calls `Computation.doProcess()`, which delegates processing to the `ProcessFunction`.

In tests, `TestComputationHarness` replaces the gRPC layer: it accepts [`TestDoProcessRequest`]({{source-root}}/yt/java/flow/flow-test-utils/src/main/java/tech/ytsaurus/flow/request/TestDoProcessRequest.java), converts it to protobuf format, calls `CompanionRequestProcessor.processBatch()`, and returns [`TestDoProcessResponse`]({{source-root}}/yt/java/flow/flow-test-utils/src/main/java/tech/ytsaurus/flow/request/TestDoProcessResponse.java) with deserialized results.

## Dependencies {#dependencies}

To use the test utilities, you need to add a dependency on `flow-test-utils`:

```
PEERDIR(
    yt/java/flow/flow-test-utils
)
```

All examples below use JUnit version 5 or later.

## Setting up TestComputationHarness {#setup}

To create a `TestComputationHarness`, you need:

1. **`PipelineContext`** — the pipeline context with registered `Computation` objects and streams.
2. **Pipeline spec** — the static pipeline specification in YSON format (the `pipeline.yson` file).
3. **External state schemas** (optional) — schemas for external states if the computation uses `ExternalStateAccessor`.

### Builder API

{% list tabs group=lang %}

- Java

  ```java
  TestComputationHarness harness = TestComputationHarness.builder()
          .setPipelineContext(pipelineContext)       // required
          .setPipelineSpec(txtSpec)                  // required: String, YTreeNode, or InputStream
          .addExternalStateSchema("state-name", schema) // optional
          .setJobContext(jobContext)                 // optional, default timeout=10 minutes
          .build();
  ```

- Kotlin

  ```kotlin
  val harness = TestComputationHarness.builder()
          .setPipelineContext(pipelineContext)       // required
          .setPipelineSpec(txtSpec)                  // required: String, YTreeNode, or InputStream
          .addExternalStateSchema("state-name", schema) // optional
          .setJobContext(jobContext)                 // optional, default timeout=10 minutes
          .build()
  ```

{% endlist %}

When you call `build()`, the harness automatically:

- Extracts stream information from the pipeline spec.
- Registers missing streams as untyped (via `FlowStreams.raw`) in the `PipelineContext`.
- Creates an internal `CompanionRequestProcessor`.

## Building test requests {#test-requests}

### TestDoProcessRequest {#test-do-process-request}

[`TestDoProcessRequest`]({{source-root}}/yt/java/flow/flow-test-utils/src/main/java/tech/ytsaurus/flow/request/TestDoProcessRequest.java) is a request to process a batch of messages and/or timers. You create it using a builder:

{% list tabs group=lang %}

- Java

  ```java
  var request = TestDoProcessRequest.builder("computation-id")
          .setMessages(messages)           // List<ExtendedMessage>
          .setTimers(timers)               // List<Timer>
          .setWatermarks(watermarks)       // Map<String, Long>
          .setExternalState("name", stateMap)  // Map<Payload, ExternalState>
          .setInternalState("name", stateMap)  // Map<Payload, InternalState>
          .build();
  ```

- Kotlin

  ```kotlin
  val request = TestDoProcessRequest.builder("computation-id")
          .setMessages(messages)           // List<ExtendedMessage>
          .setTimers(timers)               // List<Timer>
          .setWatermarks(watermarks)       // Map<String, Long>
          .setExternalState("name", stateMap)  // Map<Payload, ExternalState>
          .setInternalState("name", stateMap)  // Map<Payload, InternalState>
          .build()
  ```

{% endlist %}

Parameters:
- **`computationId`** — the ID of the computation that the request targets.
- **`messages`** — a list of input messages of type `ExtendedMessage`.
- **`timers`** — a list of triggered timers of type `Timer`.
- **`watermarks`** — a map of watermarks by stream (`streamId → timestamp`).
- **`externalStates`** — pre-filled external state.
- **`internalStates`** — pre-filled internal state.

### Building messages {#building-messages}

#### With typed streams

If you register streams as typed (via `FlowStreams.typed(...)`), you can pass the payload as a POJO object:

{% list tabs group=lang %}

- Java

  ```java
  ExtendedMessage msg = ExtendedMessage.builder()
          .setStreamId("hit")
          .setKey(key)
          .setPayload(new Hit("hit-1", 1000L, "payload-data"))
          .setEventTimestamp(1000L)
          .build();
  ```

- Kotlin

  ```kotlin
  val msg = ExtendedMessage.builder()
          .setStreamId("hit")
          .setKey(key)
          .setPayload(Hit("hit-1", 1000L, "payload-data"))
          .setEventTimestamp(1000L)
          .build()
  ```

{% endlist %}

#### With untyped streams (PayloadBuilder)

If streams aren’t typed, you build the payload using `PayloadBuilder`:

{% list tabs group=lang %}

- Java

  ```java
  var hitSchema = testHarness.getStream("hit").getSchema();
  var payload = new PayloadBuilder(hitSchema)
          .set("hit_id", "hit-1")
          .set("hit_time", 1000L)
          .set("hit_payload", "payload-data")
          .finish();

  ExtendedMessage msg = ExtendedMessage.builder()
          .setStreamId("hit")
          .setKey(key)
          .setPayload(payload)
          .setEventTimestamp(1000L)
          .build();
  ```

- Kotlin

  ```kotlin
  val hitSchema = testHarness.getStream("hit").getSchema()
  val payload = PayloadBuilder(hitSchema)
          .set("hit_id", "hit-1")
          .set("hit_time", 1000L)
          .set("hit_payload", "payload-data")
          .finish()

  val msg = ExtendedMessage.builder()
          .setStreamId("hit")
          .setKey(key)
          .setPayload(payload)
          .setEventTimestamp(1000L)
          .build()
  ```

{% endlist %}

{% note info %}

The `testHarness.getStream(streamId)` method returns a `FlowStream<?>` with the schema extracted from the pipeline spec. This is convenient for getting the stream schema without defining it manually.

{% endnote %}

### Building keys {#building-keys}

You build the key (`Payload`) according to the `group_by_schema` of the `Computation` you’re testing, from the pipeline spec.

{% note info %}

The `testHarness.getGroupBySchema(computationId)` method returns a `TableSchema` based on the `group_by_schema` extracted from the pipeline spec for the `Computation` you’re testing.

{% endnote %}

{% list tabs group=lang %}

- Java

  ```java
  TableSchema keySchema = testHarness.getGroupBySchema("join");

  Payload key = new PayloadBuilder(keySchema)
          .set("hash", 0L)
          .set("hit_id", "hit-1")
          .set("hit_time", 1000L)
          .finish();
  ```

- Kotlin

  ```kotlin
  val keySchema = testHarness.getGroupBySchema("join")

  val key = PayloadBuilder(keySchema)
          .set("hash", 0L)
          .set("hit_id", "hit-1")
          .set("hit_time", 1000L)
          .finish()
  ```

{% endlist %}

{% note tip %}

The `hash` field (farm_hash) is calculated on the C++ worker side. In tests, you can set it to `0L`.

{% endnote %}

### Building timers {#building-timers}

You create a timer using a builder:

{% list tabs group=lang %}

- Java

  ```java
  Timer timer = Timer.builder()
          .setStreamId("timer")
          .setKey(key)
          .setTriggerTimestamp(triggerTimestamp)
          .build();
  ```

- Kotlin

  ```kotlin
  val timer = Timer.builder()
          .setStreamId("timer")
          .setKey(key)
          .setTriggerTimestamp(triggerTimestamp)
          .build()
  ```

{% endlist %}

### Configuring watermarks {#watermarks}

Watermarks define which messages are considered late. A message with `eventTimestamp < watermark` will be discarded if the `ProcessFunction` implements the corresponding check via `ctx.getEpochInputEventWatermark()`. See an example of this check in [Wait Click Join in Java](../../../flow/java/examples/wait_click_join.md#late-data-check).

{% list tabs group=lang %}

- Java

  ```java
  Map<String, Long> watermarks = Map.of(
          "hit", 0L,
          "action", 0L
  );
  ```

- Kotlin

  ```kotlin
  val watermarks = mapOf(
          "hit" to 0L,
          "action" to 0L
  )
  ```

{% endlist %}

Setting the watermark to `0L` means that all messages with `eventTimestamp >= 0` won’t be considered late.

### Pre-populating state {#pre-populating-state}

#### External State

{% list tabs group=lang %}

- Java

  ```java
  TableSchema stateSchema = TableSchema.builder()
          .addValue("hit_payload", ColumnValueType.STRING)
          .addValue("show_time", ColumnValueType.UINT64)
          .addValue("click_time", ColumnValueType.UINT64)
          .build();

  ExternalState preState = new ExternalState(
          new PayloadBuilder(stateSchema)
                  .set("hit_payload", "some-payload")
                  .set("show_time", showTime)
                  .set("click_time", clickTime)
                  .finish()
  );

  var request = TestDoProcessRequest.builder("join")
          .setTimers(List.of(timer))
          .setExternalState("join-state", Map.of(key, preState))
          .build();
  ```

- Kotlin

  ```kotlin
  val stateSchema = TableSchema.builder()
          .addValue("hit_payload", ColumnValueType.STRING)
          .addValue("show_time", ColumnValueType.UINT64)
          .addValue("click_time", ColumnValueType.UINT64)
          .build()

  val preState = ExternalState(
          PayloadBuilder(stateSchema)
                  .set("hit_payload", "some-payload")
                  .set("show_time", showTime)
                  .set("click_time", clickTime)
                  .finish()
  )

  val request = TestDoProcessRequest.builder("join")
          .setTimers(listOf(timer))
          .setExternalState("join-state", mapOf(key to preState))
          .build()
  ```

{% endlist %}

#### Internal State (Proto)

For internal state based on protobuf:

{% list tabs group=lang %}

- Java

  ```java
  TJoinState protoState = TJoinState.newBuilder()
          .setHitPayload("some-payload")
          .setShowTime(showTime)
          .setClickTime(clickTime)
          .build();

  InternalState internalState = new InternalState(protoState.toByteArray());

  var request = TestDoProcessRequest.builder("join")
          .setTimers(List.of(timer))
          .setInternalState("join-state", Map.of(key, internalState))
          .build();
  ```

- Kotlin

  ```kotlin
  val protoState = TJoinState.newBuilder()
          .setHitPayload("some-payload")
          .setShowTime(showTime)
          .setClickTime(clickTime)
          .build()

  val internalState = InternalState(protoState.toByteArray())

  val request = TestDoProcessRequest.builder("join")
          .setTimers(listOf(timer))
          .setInternalState("join-state", mapOf(key to internalState))
          .build()
  ```

{% endlist %}

## Analyzing the results {#analyzing-response}

### TestDoProcessResponse {#test-do-process-response}

[`TestDoProcessResponse`]({{source-root}}/yt/java/flow/flow-test-utils/src/main/java/tech/ytsaurus/flow/request/TestDoProcessResponse.java) provides methods to verify processing results.

| Method | Description |
|-------|----------|
| `getOutputMessagesFlatten()` | All output messages from all transform groups |
| `getOutputTimersFlatten()` | All set timers from all transform groups |
| `getTransformResults()` | A list of `TransformResult` grouped by parent IDs |

{% note info %}

`TestDoProcessResponse` provides two sets of methods to access the state:

- **Full view** (`getExternalState(s)`, `getInternalState(s)`) — all states after processing: the keys passed in the request, with computation changes applied on top. Use this when you need to check the final state regardless of whether computation changed it.
- **Only modified** (`getModifiedExternalState(s)`, `getModifiedInternalState(s)`) — only what computation actually changed. These are the states sent back to the worker and saved. Use this to verify that the state for a key hasn’t changed, or when you need to check only the state changes.

All state access methods are null-safe: an unknown state name or key returns an empty map or `null`, not an exception.

{% endnote %}

Full view of the state (loaded states with computation changes applied):

| Method | Description |
|-------|----------|
| `getExternalStateNames()` / `getInternalStateNames()` | Names of all states (loaded and/or modified) |
| `getExternalStates()` / `getInternalStates()` | All states grouped by name |
| `getExternalStates(name)` / `getInternalStates(name)` | All state records by name (empty map if none) |
| `getExternalStateSize(name)` / `getInternalStateSize(name)` | Number of records in the state (0 if none) |
| `getExternalState(name, key)` / `getInternalState(name, key)` | Specific state record (`null` if none) |

Only states modified by computation:

| Method | Description |
|-------|----------|
| `getModifiedExternalStateNames()` / `getModifiedInternalStateNames()` | Names of modified states |
| `getModifiedExternalStates()` / `getModifiedInternalStates()` | Modified states grouped by name |
| `getModifiedExternalStates(name)` / `getModifiedInternalStates(name)` | Modified state records by name (empty map if none) |
| `getModifiedExternalStateSize(name)` / `getModifiedInternalStateSize(name)` | Number of modified records (0 if none) |
| `getModifiedExternalState(name, key)` / `getModifiedInternalState(name, key)` | Specific modified record (`null` if not modified) |

### Checking output messages

{% list tabs group=lang %}

- Java

  ```java
  var response = testHarness.doProcess(request);

  // Check the number of output messages
  assertEquals(1, response.getOutputMessagesFlatten().size());

  // Get the message and check the stream
  var msg = response.getOutputMessagesFlatten().get(0);
  assertEquals("joined_action", msg.getStreamId());

  // For typed streams — cast payload to POJO
  JoinedAction result = (JoinedAction) msg.getPayload();
  assertEquals("hit-1", result.getHitId());

  // For untyped streams — read fields via get()
  byte[] data = msg.get("data", byte[].class);
  ```

- Kotlin

  ```kotlin
  val response = testHarness.doProcess(request)

  // Check the number of output messages
  assertEquals(1, response.getOutputMessagesFlatten().size)

  // Get the message and check the stream
  val msg = response.getOutputMessagesFlatten()[0]
  assertEquals("joined_action", msg.getStreamId())

  // For typed streams — cast payload to POJO
  val result = msg.getPayload() as JoinedAction
  assertEquals("hit-1", result.getHitId())

  // For untyped streams — read fields via get()
  val data = msg.get("data", ByteArray::class.java)
  ```

{% endlist %}

### Checking timers

{% list tabs group=lang %}

- Java

  ```java
  assertEquals(1, response.getOutputTimersFlatten().size());
  var timer = response.getOutputTimersFlatten().get(0);
  assertEquals(expectedTriggerTimestamp, timer.getTriggerTimestamp());
  ```

- Kotlin

  ```kotlin
  assertEquals(1, response.getOutputTimersFlatten().size)
  val timer = response.getOutputTimersFlatten()[0]
  assertEquals(expectedTriggerTimestamp, timer.getTriggerTimestamp())
  ```

{% endlist %}

### Checking the state

{% list tabs group=lang %}

- Java

  ```java
  // External state
  assertEquals(1, response.getExternalStateSize("join-state"));
  var state = response.getExternalState("join-state", key);
  assertFalse(state.isReset());
  assertEquals("payload-data", state.getValue().get("hit_payload", String.class));

  // Check state reset
  var stateAfterTimer = response.getExternalState("join-state", key);
  assertTrue(stateAfterTimer.isReset());
  ```

- Kotlin

  ```kotlin
  // External state
  assertEquals(1, response.getExternalStateSize("join-state"))
  val state = response.getExternalState("join-state", key)!!
  assertFalse(state.isReset)
  assertEquals("payload-data", state.value.get("hit_payload", String::class.java))

  // Check state reset
  val stateAfterTimer = response.getExternalState("join-state", key)!!
  assertTrue(stateAfterTimer.isReset)
  ```

{% endlist %}

{% list tabs group=lang %}

- Java

  ```java
  // Internal state (Proto)
  var internalState = response.getInternalState("join-state", key);
  assertFalse(internalState.isReset());
  TJoinState joinState = ProtoUtils.parseBytes(internalState.getValue(), TJoinState.class);
  assertEquals("payload-data", joinState.getHitPayload());
  ```

- Kotlin

  ```kotlin
  // Internal state (Proto)
  val internalState = response.getInternalState("join-state", key)!!
  assertFalse(internalState.isReset)
  val joinState = ProtoUtils.parseBytes(internalState.value, TJoinState::class.java)
  assertEquals("payload-data", joinState.getHitPayload())
  ```

{% endlist %}

#### Full view vs. modified states {#all-vs-modified-state}

The `getExternalState` / `getInternalState` methods return the **full view**: the pre-filled state is available even if computation didn’t change it. To check what computation actually changed, use the `getModified*` methods.

{% list tabs group=lang %}

- Java

  ```java
  // Full view: the loaded state is visible even if it wasn’t changed
  assertEquals(1, response.getExternalStateSize("join-state"));
  assertNotNull(response.getExternalState("join-state", key));

  // Only modified states: computation wrote nothing for this key
  assertEquals(0, response.getModifiedExternalStateSize("join-state"));
  assertNull(response.getModifiedExternalState("join-state", key));

  // Unknown name/key doesn’t throw an exception
  assertNull(response.getExternalState("unknown", key));
  assertTrue(response.getExternalStates("unknown").isEmpty());
  ```

- Kotlin

  ```kotlin
  // Full view: the loaded state is visible even if it wasn’t changed
  assertEquals(1, response.getExternalStateSize("join-state"))
  assertNotNull(response.getExternalState("join-state", key))

  // Only modified states: computation wrote nothing for this key
  assertEquals(0, response.getModifiedExternalStateSize("join-state"))
  assertNull(response.getModifiedExternalState("join-state", key))

  // Unknown name/key doesn’t throw an exception
  assertNull(response.getExternalState("unknown", key))
  assertTrue(response.getExternalStates("unknown").isEmpty())
  ```

{% endlist %}

## Example: a test without Spring {#example-without-spring}

This is a complete unit test example for `JoinProcessFunction` from the [wait_click_join](../../../flow/java/examples/wait_click_join.md) project. In this variant, you create `PipelineContext` manually, without a Spring container.

{% list tabs group=lang %}

- Java

  ```java
  public class JoinProcessFunctionTest {

      private static final long WAIT_SECONDS = 10L;
      private static final long BASE_HIT_TIME = 1000L;
      private static final long WATERMARK = 0L;

      private TestComputationHarness testHarness;
      private TableSchema keySchema;
      private TableSchema joinStateSchema;

      @BeforeEach
      void init() throws IOException {
          // 1. You create PipelineContext manually
          var pipelineContext = new PipelineContext();

          // 2. Register the computation with the process function
          Computation join = Computation.builder()
                  .setComputationId("join")
                  .setProcessFunction(new JoinProcessFunction())
                  .build();
          pipelineContext.registerComputation(join);

          // 3. Register typed streams
          pipelineContext.registerStream(FlowStreams.typed("hit", Hit.class));
          pipelineContext.registerStream(FlowStreams.typed("action", Action.class));
          pipelineContext.registerStream(FlowStreams.typed("joined_action", JoinedAction.class));

          // 4. Define the external state schema
          this.joinStateSchema = TableSchema.builder()
                  .addValue("hit_payload", ColumnValueType.STRING)
                  .addValue("show_time", ColumnValueType.UINT64)
                  .addValue("click_time", ColumnValueType.UINT64)
                  .build();

          // 5. Read the pipeline spec and create the harness
          var specPath = Paths.getSourcePath(
                  "yt/yt/flow/examples/java/wait_click_join/test/pipeline.yson");
          var txtSpec = Files.readString(Path.of(specPath));

          this.testHarness = TestComputationHarness.builder()
                  .setPipelineContext(pipelineContext)
                  .setPipelineSpec(txtSpec)
                  .addExternalStateSchema("join-state", joinStateSchema)
                  .build();

          // 6. Get the key schema for the join computation (group_by_schema from pipeline.yson)
          this.keySchema = testHarness.getGroupBySchema("join");
      }

      // — Helper methods —

      private Payload buildKey(String hitId, long hitTime) {
          return new PayloadBuilder(keySchema)
                  .set("hash", 0L)
                  .set("hit_id", hitId)
                  .set("hit_time", hitTime)
                  .finish();
      }

      private ExtendedMessage buildHitMessage(String hitId, long hitTime, String hitPayload) {
          return ExtendedMessage.builder()
                  .setStreamId("hit")
                  .setKey(buildKey(hitId, hitTime))
                  .setPayload(new Hit(hitId, hitTime, hitPayload))
                  .setEventTimestamp(hitTime)
                  .build();
      }

      private Map<String, Long> defaultWatermarks() {
          return Map.of("hit", WATERMARK, "action", WATERMARK);
      }

      // — Tests —

      @Test
      void testHitMessageStoresHitPayloadInState() {
          var messages = List.of(buildHitMessage("hit-1", BASE_HIT_TIME, "payload-data"));

          var request = TestDoProcessRequest.builder("join")
                  .setMessages(messages)
                  .setWatermarks(defaultWatermarks())
                  .build();

          var response = testHarness.doProcess(request);

          // There are no output messages — the result will appear when the timer fires
          assertTrue(response.getOutputMessagesFlatten().isEmpty());

          // The timer must be set
          assertEquals(1, response.getOutputTimersFlatten().size());
          assertEquals(BASE_HIT_TIME + WAIT_SECONDS,
                  response.getOutputTimersFlatten().get(0).getTriggerTimestamp());

          // The state must contain hit_payload
          Payload key = buildKey("hit-1", BASE_HIT_TIME);
          var state = response.getExternalState("join-state", key);
          assertFalse(state.isReset());
          assertEquals("payload-data", state.getValue().get("hit_payload", String.class));
      }

      @Test
      void testTimerEmitsJoinedAction() {
          Payload key = buildKey("hit-10", BASE_HIT_TIME);

          // Pre-populate the state
          ExternalState preState = new ExternalState(
                  new PayloadBuilder(joinStateSchema)
                          .set("hit_payload", "some-payload")
                          .set("show_time", BASE_HIT_TIME + 3L)
                          .set("click_time", BASE_HIT_TIME + 7L)
                          .finish()
          );

          Timer timer = Timer.builder()
                  .setStreamId("timer")
                  .setKey(key)
                  .setTriggerTimestamp(BASE_HIT_TIME + WAIT_SECONDS)
                  .build();

          var request = TestDoProcessRequest.builder("join")
                  .setTimers(List.of(timer))
                  .setExternalState("join-state", Map.of(key, preState))
                  .build();

          var response = testHarness.doProcess(request);

          // There must be one output message
          assertEquals(1, response.getOutputMessagesFlatten().size());
          var msg = response.getOutputMessagesFlatten().get(0);
          assertEquals("joined_action", msg.getStreamId());

          JoinedAction result = (JoinedAction) msg.getPayload();
          assertEquals("hit-10", result.getHitId());
          assertTrue(result.getClick());

          // The state must be reset
          assertTrue(response.getExternalState("join-state", key).isReset());
      }

      @Test
      void testLateMessageIsDropped() {
          long watermark = BASE_HIT_TIME + 5L;
          long eventTimestamp = BASE_HIT_TIME + 3L; // < watermark → late

          var messages = List.of(buildActionMessage("hit-5", BASE_HIT_TIME,
                  BASE_HIT_TIME + 2L, false, eventTimestamp));

          var request = TestDoProcessRequest.builder("join")
                  .setMessages(messages)
                  .setWatermarks(Map.of("hit", watermark, "action", watermark))
                  .build();

          var response = testHarness.doProcess(request);

          // The late message is dropped
          assertTrue(response.getOutputMessagesFlatten().isEmpty());
          assertTrue(response.getOutputTimersFlatten().isEmpty());
          assertEquals(0, response.getExternalStateSize("join-state"));
      }
  }
  ```

- Kotlin

  ```kotlin
  class JoinProcessFunctionTest {

      companion object {
          private const val WAIT_SECONDS = 10L
          private const val BASE_HIT_TIME = 1000L
          private const val WATERMARK = 0L
      }

      private lateinit var testHarness: TestComputationHarness
      private lateinit var keySchema: TableSchema
      private lateinit var joinStateSchema: TableSchema

      @BeforeEach
      fun init() {
          // 1. You create PipelineContext manually
          val pipelineContext = PipelineContext()

          // 2. Register the computation with the process function
          val join = Computation.builder()
                  .setComputationId("join")
                  .setProcessFunction(JoinProcessFunction())
                  .build()
          pipelineContext.registerComputation(join)

          // 3. Register typed streams
          pipelineContext.registerStream(FlowStreams.typed("hit", Hit::class.java))
          pipelineContext.registerStream(FlowStreams.typed("action", Action::class.java))
          pipelineContext.registerStream(FlowStreams.typed("joined_action", JoinedAction::class.java))

          // 4. Define the external state schema
          joinStateSchema = TableSchema.builder()
                  .addValue("hit_payload", ColumnValueType.STRING)
                  .addValue("show_time", ColumnValueType.UINT64)
                  .addValue("click_time", ColumnValueType.UINT64)
                  .build()

          // 5. Read the pipeline spec and create the harness
          val specPath = Paths.getSourcePath(
                  "yt/yt/flow/examples/java/wait_click_join/test/pipeline.yson")
          val txtSpec = Files.readString(Path.of(specPath))

          testHarness = TestComputationHarness.builder()
                  .setPipelineContext(pipelineContext)
                  .setPipelineSpec(txtSpec)
                  .addExternalStateSchema("join-state", joinStateSchema)
                  .build()

          // 6. Get the key schema for the join computation (group_by_schema from pipeline.yson)
          keySchema = testHarness.getGroupBySchema("join")
      }

      // — Helper methods —

      private fun buildKey(hitId: String, hitTime: Long): Payload =
          PayloadBuilder(keySchema)
                  .set("hash", 0L)
                  .set("hit_id", hitId)
                  .set("hit_time", hitTime)
                  .finish()

      private fun buildHitMessage(hitId: String, hitTime: Long, hitPayload: String): ExtendedMessage =
          ExtendedMessage.builder()
                  .setStreamId("hit")
                  .setKey(buildKey(hitId, hitTime))
                  .setPayload(Hit(hitId, hitTime, hitPayload))
                  .setEventTimestamp(hitTime)
                  .build()

      private fun defaultWatermarks() = mapOf("hit" to WATERMARK, "action" to WATERMARK)

      // — Tests —

      @Test
      fun testHitMessageStoresHitPayloadInState() {
          val messages = listOf(buildHitMessage("hit-1", BASE_HIT_TIME, "payload-data"))

          val request = TestDoProcessRequest.builder("join")
                  .setMessages(messages)
                  .setWatermarks(defaultWatermarks())
                  .build()

          val response = testHarness.doProcess(request)

          // There are no output messages — the result will appear when the timer fires
          assertTrue(response.getOutputMessagesFlatten().isEmpty())

          // The timer must be set
          assertEquals(1, response.getOutputTimersFlatten().size)
          assertEquals(BASE_HIT_TIME + WAIT_SECONDS,
                  response.getOutputTimersFlatten()[0].getTriggerTimestamp())

          // The state must contain hit_payload
          val key = buildKey("hit-1", BASE_HIT_TIME)
          val state = response.getExternalState("join-state", key)!!
          assertFalse(state.isReset)
          assertEquals("payload-data", state.value.get("hit_payload", String::class.java))
      }

      @Test
      fun testTimerEmitsJoinedAction() {
          val key = buildKey("hit-10", BASE_HIT_TIME)

          // Pre-populate the state
          val preState = ExternalState(
                  PayloadBuilder(joinStateSchema)
                          .set("hit_payload", "some-payload")
                          .set("show_time", BASE_HIT_TIME + 3L)
                          .set("click_time", BASE_HIT_TIME + 7L)
                          .finish()
          )

          val timer = Timer.builder()
                  .setStreamId("timer")
                  .setKey(key)
                  .setTriggerTimestamp(BASE_HIT_TIME + WAIT_SECONDS)
                  .build()

          val request = TestDoProcessRequest.builder("join")
                  .setTimers(listOf(timer))
                  .setExternalState("join-state", mapOf(key to preState))
                  .build()

          val response = testHarness.doProcess(request)

          // There must be one output message
          assertEquals(1, response.getOutputMessagesFlatten().size)
          val msg = response.getOutputMessagesFlatten()[0]
          assertEquals("joined_action", msg.getStreamId())

          val result = msg.getPayload() as JoinedAction
          assertEquals("hit-10", result.getHitId())
          assertTrue(result.getClick())

          // The state must be reset
          assertTrue(response.getExternalState("join-state", key)!!.isReset)
      }

      @Test
      fun testLateMessageIsDropped() {
          val watermark = BASE_HIT_TIME + 5L
          val eventTimestamp = BASE_HIT_TIME + 3L // < watermark → late

          val messages = listOf(buildActionMessage("hit-5", BASE_HIT_TIME,
                  BASE_HIT_TIME + 2L, false, eventTimestamp))

          val request = TestDoProcessRequest.builder("join")
                  .setMessages(messages)
                  .setWatermarks(mapOf("hit" to watermark, "action" to watermark))
                  .build()

          val response = testHarness.doProcess(request)

          // The late message is dropped
          assertTrue(response.getOutputMessagesFlatten().isEmpty())
          assertTrue(response.getOutputTimersFlatten().isEmpty())
          assertEquals(0, response.getExternalStateSize("join-state"))
      }
  }
  ```

{% endlist %}

### Multi-step test {#multi-step-test}

To test the full processing flow (hit → show → click → timer), you manually pass the state between steps:

{% list tabs group=lang %}

- Java

  ```java
  @Test
  void testFullJoinFlow() {
      String hitId = "hit-20";
      long hitTime = BASE_HIT_TIME;

      // Step 1: process the hit message
      var hitRequest = TestDoProcessRequest.builder("join")
              .setMessages(List.of(buildHitMessage(hitId, hitTime, "full-payload")))
              .setWatermarks(defaultWatermarks())
              .build();
      var hitResponse = testHarness.doProcess(hitRequest);

      Payload key = buildKey(hitId, hitTime);
      var stateAfterHit = hitResponse.getExternalState("join-state", key);

      // Step 2: process the show message, pass the state from step 1
      var showRequest = TestDoProcessRequest.builder("join")
              .setMessages(List.of(buildActionMessage(hitId, hitTime, hitTime + 3L, false, hitTime + 1L)))
              .setExternalState("join-state", Map.of(key, stateAfterHit))
              .setWatermarks(defaultWatermarks())
              .build();
      var showResponse = testHarness.doProcess(showRequest);

      var stateAfterShow = showResponse.getExternalState("join-state", key);

      // Step 3: process the click message
      var clickRequest = TestDoProcessRequest.builder("join")
              .setMessages(List.of(buildActionMessage(hitId, hitTime, hitTime + 7L, true, hitTime + 2L)))
              .setExternalState("join-state", Map.of(key, stateAfterShow))
              .setWatermarks(defaultWatermarks())
              .build();
      var clickResponse = testHarness.doProcess(clickRequest);

      var stateAfterClick = clickResponse.getExternalState("join-state", key);

      // Step 4: trigger the timer
      Timer timer = Timer.builder()
              .setStreamId("timer")
              .setKey(key)
              .setTriggerTimestamp(hitTime + WAIT_SECONDS)
              .build();
      var timerRequest = TestDoProcessRequest.builder("join")
              .setTimers(List.of(timer))
              .setExternalState("join-state", Map.of(key, stateAfterClick))
              .build();
      var timerResponse = testHarness.doProcess(timerRequest);

      // Check the final result
      assertEquals(1, timerResponse.getOutputMessagesFlatten().size());
      JoinedAction result = timerResponse.getOutputMessagesFlatten().get(0).getPayload();
      assertEquals(hitId, result.getHitId());
      assertTrue(result.getClick());

      assertTrue(timerResponse.getExternalState("join-state", key).isReset());
  }
  ```

- Kotlin

  ```kotlin
  @Test
  fun testFullJoinFlow() {
      val hitId = "hit-20"
      val hitTime = BASE_HIT_TIME

      // Step 1: process the hit message
      val hitRequest = TestDoProcessRequest.builder("join")
              .setMessages(listOf(buildHitMessage(hitId, hitTime, "full-payload")))
              .setWatermarks(defaultWatermarks())
              .build()
      val hitResponse = testHarness.doProcess(hitRequest)

      val key = buildKey(hitId, hitTime)
      val stateAfterHit = hitResponse.getExternalState("join-state", key)!!

      // Step 2: process the show message, pass the state from step 1
      val showRequest = TestDoProcessRequest.builder("join")
              .setMessages(listOf(buildActionMessage(hitId, hitTime, hitTime + 3L, false, hitTime + 1L)))
              .setExternalState("join-state", mapOf(key to stateAfterHit))
              .setWatermarks(defaultWatermarks())
              .build()
      val showResponse = testHarness.doProcess(showRequest)

      val stateAfterShow = showResponse.getExternalState("join-state", key)!!

      // Step 3: process the click message
      val clickRequest = TestDoProcessRequest.builder("join")
              .setMessages(listOf(buildActionMessage(hitId, hitTime, hitTime + 7L, true, hitTime + 2L)))
              .setExternalState("join-state", mapOf(key to stateAfterShow))
              .setWatermarks(defaultWatermarks())
              .build()
      val clickResponse = testHarness.doProcess(clickRequest)

      val stateAfterClick = clickResponse.getExternalState("join-state", key)!!

      // Step 4: trigger the timer
      val timer = Timer.builder()
              .setStreamId("timer")
              .setKey(key)
              .setTriggerTimestamp(hitTime + WAIT_SECONDS)
              .build()
      val timerRequest = TestDoProcessRequest.builder("join")
              .setTimers(listOf(timer))
              .setExternalState("join-state", mapOf(key to stateAfterClick))
              .build()
      val timerResponse = testHarness.doProcess(timerRequest)

      // Check the final result
      assertEquals(1, timerResponse.getOutputMessagesFlatten().size)
      val result = timerResponse.getOutputMessagesFlatten()[0].getPayload() as JoinedAction
      assertEquals(hitId, result.getHitId())
      assertTrue(result.getClick())

      assertTrue(timerResponse.getExternalState("join-state", key)!!.isReset)
  }
  ```

{% endlist %}

{% note warning %}

`TestComputationHarness` doesn't store state between `doProcess()` calls. Each call is an independent batch. To emulate multi-step processing, you must manually pass the state from the previous step's response to the next step's request.

{% endnote %}

## Example: a test with Spring {#example-with-spring}

When you use Spring Boot Starter, testing becomes simpler: the `PipelineContext` is created automatically via `FlowAutoConfiguration` based on beans annotated with `@FlowComputation` or `@FlowSourceComputation`.

### Test configuration {#spring-test-config}

For tests, you need to replace `GrpcServerExecution` with `NoServerTestExecution` so you don’t start a real gRPC server:

{% list tabs group=lang %}

- Java

  ```java
  @TestConfiguration
  public class MyTestConfiguration {

      @Bean
      public CompanionExecutionConfig companionExecutionConfig() {
          return new CompanionExecutionConfig(0, new MockEnvironmentReader().worker());
      }

      @Bean
      public GrpcServerExecution grpcServerExecution(
              PipelineContext pipelineContext,
              CompanionExecutionConfig companionExecutionConfig
      ) {
          return new NoServerTestExecution(pipelineContext, companionExecutionConfig);
      }
  }
  ```

- Kotlin

  ```kotlin
  @TestConfiguration
  class MyTestConfiguration {

      @Bean
      fun companionExecutionConfig(): CompanionExecutionConfig =
          CompanionExecutionConfig(0, MockEnvironmentReader().worker())

      @Bean
      fun grpcServerExecution(
          pipelineContext: PipelineContext,
          companionExecutionConfig: CompanionExecutionConfig
      ): GrpcServerExecution = NoServerTestExecution(pipelineContext, companionExecutionConfig)
  }
  ```

{% endlist %}

Key points:
- [`MockEnvironmentReader`]({{source-root}}/yt/java/flow/flow-test-utils/src/main/java/tech/ytsaurus/flow/config/MockEnvironmentReader.java) replaces environment variable reading. The `.worker()` method sets `YT_FLOW_MODE=Worker`.
- [`NoServerTestExecution`]({{source-root}}/yt/java/flow/flow-test-utils/src/main/java/tech/ytsaurus/flow/execution/NoServerTestExecution.java) is a stub for `GrpcServerExecution` that doesn’t start a real gRPC server.
- `CompanionExecutionConfig(0, ...)` — a port value of `0` means no real port is allocated.

### Test class {#spring-test-class}

{% list tabs group=lang %}

- Java

  ```java
  @SpringBootTest(classes = MyTestConfiguration.class)
  class JoinFunctionTest {

      @Autowired
      private PipelineContext pipelineContext;

      private TestComputationHarness testHarness;
      private TableSchema keySchema;

      @BeforeEach
      void init() throws IOException {
          this.keySchema = TableSchema.builder()
                  .addValue("hash", ColumnValueType.UINT64)
                  .addValue("hit_id", ColumnValueType.STRING)
                  .addValue("hit_time", ColumnValueType.UINT64)
                  .build();

          var specPath = Paths.getSourcePath(
                  "yt/yt/flow/examples/java/lb_wait_click_join/test/pipeline.yson");
          var txtSpec = Files.readString(Path.of(specPath));

          this.testHarness = TestComputationHarness.builder()
                  .setPipelineContext(pipelineContext)  // injected via Spring
                  .setPipelineSpec(txtSpec)
                  .build();
      }

      @Test
      void testHitMessageStoresState() {
          var messages = List.of(buildHitMessage("hit-1", 1000L, "payload-data"));

          var request = TestDoProcessRequest.builder("join")
                  .setMessages(messages)
                  .setWatermarks(Map.of("hit", 0L, "action", 0L))
                  .build();

          var response = testHarness.doProcess(request);

          assertTrue(response.getOutputMessagesFlatten().isEmpty());
          assertEquals(1, response.getOutputTimersFlatten().size());
          assertEquals(1, response.getInternalStateSize("join-state"));
      }

      // ... helper methods and other tests
  }
  ```

- Kotlin

  ```kotlin
  @SpringBootTest(classes = [MyTestConfiguration::class])
  class JoinFunctionTest {

      @Autowired
      private lateinit var pipelineContext: PipelineContext

      private lateinit var testHarness: TestComputationHarness
      private lateinit var keySchema: TableSchema

      @BeforeEach
      fun init() {
          keySchema = TableSchema.builder()
                  .addValue("hash", ColumnValueType.UINT64)
                  .addValue("hit_id", ColumnValueType.STRING)
                  .addValue("hit_time", ColumnValueType.UINT64)
                  .build()

          val specPath = Paths.getSourcePath(
                  "yt/yt/flow/examples/java/lb_wait_click_join/test/pipeline.yson")
          val txtSpec = Files.readString(Path.of(specPath))

          testHarness = TestComputationHarness.builder()
                  .setPipelineContext(pipelineContext)  // injected via Spring
                  .setPipelineSpec(txtSpec)
                  .build()
      }

      @Test
      fun testHitMessageStoresState() {
          val messages = listOf(buildHitMessage("hit-1", 1000L, "payload-data"))

          val request = TestDoProcessRequest.builder("join")
                  .setMessages(messages)
                  .setWatermarks(mapOf("hit" to 0L, "action" to 0L))
                  .build()

          val response = testHarness.doProcess(request)

          assertTrue(response.getOutputMessagesFlatten().isEmpty())
          assertEquals(1, response.getOutputTimersFlatten().size)
          assertEquals(1, response.getInternalStateSize("join-state"))
      }

      // ... helper methods and other tests
  }
  ```

{% endlist %}

### Differences from a test without Spring {#spring-vs-no-spring}

| Aspect | Without Spring | With Spring |
|--------|----------------|-------------|
| Creating `PipelineContext` | Manually: `new PipelineContext()` | Automatically via `FlowAutoConfiguration` |
| Registering Computation | `pipelineContext.registerComputation(...)` | Via the `@FlowComputation` or `@FlowSourceComputation` annotation |
| Registering streams | `pipelineContext.registerStream(...)` | Via a `FlowStream<?>` bean or `ComputationProvider.getStreams()` |
| Injecting dependencies into ProcessFunction | Manually via the constructor | Automatically via `@Autowired` |
| Replacing the gRPC server | Not needed (no server) | `NoServerTestExecution` + `MockEnvironmentReader` |

## Integration testing with FlowTestJavaBase {#integration-testing}

To run full integration testing of the pipeline (with real C++ workers, queues, and streams), use the `FlowTestJavaBase` base class.

### Dependencies {#integration-dependencies}

```
PEERDIR(
    yt/yt/flow/library/python/integration_test_base
)
```

### Setup {#java-test-setup}

Your test inherits from `FlowTestJavaBase` and defines two required attributes:

```python
from yt.yt.flow.library.python.integration_test_base.yt_flow_java_base import FlowTestJavaBase
import yatest.common

class TestWordCount(FlowTestJavaBase):
    JAVA_RUNNER_BINARY_DIR = yatest.common.binary_path(
        "yt/yt/flow/examples/java/word_count/wordcount/"
    )
    JAVA_RUNNER_MAIN_CLASS = "tech.ytsaurus.flow.examples.wordcount.RunnerMain"
```

| Attribute | Description |
|-----------|-------------|
| `JAVA_RUNNER_BINARY_DIR` | Path to the directory with the Java runner binary (contains `run.sh`) |
| `JAVA_RUNNER_MAIN_CLASS` | Full name of the Java runner’s main class |

[Integration test examples (Java)]({{source-root}}/yt/yt/flow/examples/java)

{% note warning %}

Integration tests require a deployed {{product-name}} cluster and are run via `ya make -ttt`. For fast iteration, use unit tests with `TestComputationHarness` as described above.

{% endnote %}

{% include notitle [_](../testing-integration-body.md) %}

{% include notitle [_](../testing-test-param-body.md) %}

## See also

- [Computation (Java)](../../../flow/java/computation.md)
- [Working with states (Java)](../../../flow/java/state.md)
- [Testing (Python)](../../../flow/python/testing.md)
