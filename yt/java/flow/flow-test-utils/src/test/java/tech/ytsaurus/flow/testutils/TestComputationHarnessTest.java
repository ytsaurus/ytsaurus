package tech.ytsaurus.flow.testutils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.computation.SourceComputation;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.job.JobContext;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Message;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.state.StateDescriptors;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.ytsaurus.flow.testutils.ComputationTestUtils.passthroughComputation;
import static tech.ytsaurus.flow.testutils.ComputationTestUtils.passthroughSourceComputation;

/**
 * Tests for {@link TestComputationHarness}.
 */
@DisplayName("TestComputationHarness Tests")
class TestComputationHarnessTest {

    private static final String COMPUTATION_ID = "test-computation";
    private static final String SOURCE_COMPUTATION_ID = "test-source-computation";
    private static final SchemaGenerator SCHEMAS = SchemaGenerator.builder().build();
    private static final TableSchema PAYLOAD_SCHEMA = SCHEMAS.createMixedSchema(4);
    private static final TableSchema KEY_SCHEMA = SCHEMAS.createStringSchema(1);
    // External state names must be path-like (leading slash) to be accepted by
    // StateDescriptors.external(...).
    private static final String EXT_STATE_A = "/state-a";
    private static final String EXT_STATE_B = "/state-b";

    private static String spec;
    private static String jinjaSpec;

    private TestComputationHarnessTest() {
    }

    @BeforeAll
    public static void initTest() throws IOException {
        spec = loadResource("pipeline.yson");
        jinjaSpec = loadResource("pipeline.yson.j2");
    }

    private static String loadResource(String name) throws IOException {
        try (var is = TestComputationHarnessTest.class.getClassLoader().getResourceAsStream(name)) {
            return new String(
                    Objects.requireNonNull(is, "Resource " + name + " not found").readAllBytes(),
                    StandardCharsets.UTF_8
            );
        }
    }

    private static PipelineContext ctx(String computationId, boolean isSource) {
        var context = new PipelineContext();
        if (isSource) {
            context.registerComputation(passthroughSourceComputation(computationId));
        } else {
            context.registerComputation(passthroughComputation(computationId));
        }
        return context;
    }

    private static List<ExtendedMessage> extMessages(int count) {
        var messages = new ArrayList<ExtendedMessage>(count);
        for (int i = 0; i < count; i++) {
            var payload = new Payload(
                    TestDataUtils.createUnversionedRow(PAYLOAD_SCHEMA, 10),
                    PAYLOAD_SCHEMA
            );
            var key = new Payload(
                    TestDataUtils.createUnversionedRow(KEY_SCHEMA, 10),
                    KEY_SCHEMA
            );
            messages.add(ExtendedMessage.builder()
                    .setStreamId("inputStream-0")
                    .setStreamSpecId(0L)
                    .setMessageId("<id>" + i)
                    .setSystemTimestamp(1712182928L)
                    .setEventTimestamp(1712182927L)
                    .setPayload(payload)
                    .setKey(key)
                    .build());
        }
        return messages;
    }

    /**
     * Computation that writes each incoming message's payload into the given external state(s),
     * keyed by the message key, then forwards the message. Used to exercise the response path:
     * only states the computation actually modifies are sent back.
     */
    private static Computation externalStateWriter(String computationId, List<String> stateNames) {
        return Computation.builder()
                .setComputationId(computationId)
                .setProcessFunction(new RowFunction() {
                    @Override
                    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
                        for (var stateName : stateNames) {
                            ctx.getState(StateDescriptors.external(stateName), message).set(message.getPayload());
                        }
                        output.addMessage(Message.builder()
                                .setStreamId(message.getStreamId())
                                .setPayload(message.getPayload())
                                .build());
                    }
                })
                .build();
    }

    /**
     * Pipeline context registering {@link #externalStateWriter} as {@link #COMPUTATION_ID}.
     */
    private static PipelineContext ctxWritingExternalStates(List<String> stateNames) {
        var context = new PipelineContext();
        context.registerComputation(externalStateWriter(COMPUTATION_ID, stateNames));
        return context;
    }

    /**
     * A copy of the base spec with the given external state managers declared on
     * {@link #COMPUTATION_ID}, so the computation is allowed to access those external states.
     */
    private static YTreeNode specDeclaringExternalStates(List<String> stateNames) {
        var node = YTreeTextSerializer.deserialize(spec);
        var managers = node.asMap().get("spec").asMap()
                .get("computations").asMap()
                .get(COMPUTATION_ID).asMap()
                .get("external_state_managers").asMap();
        for (var name : stateNames) {
            managers.put(name, YTree.mapBuilder()
                    .key("parameters").beginMap().key("path").value("//placeholder").endMap()
                    .endMap().build());
        }
        return node;
    }

    /**
     * Set of message keys (used to assert which external state entries a computation produced).
     */
    private static Set<UnversionedRow> keysOf(List<ExtendedMessage> messages) {
        return messages.stream().map(m -> m.getKey().getRow()).collect(Collectors.toSet());
    }

    /**
     * Builds {@code count} random {@link Payload} key/value entries for seeding external state.
     */
    private static Map<Payload, Payload> loadedExternalEntries(int count) {
        var entries = new LinkedHashMap<Payload, Payload>();
        for (int i = 0; i < count; i++) {
            var key = new Payload(TestDataUtils.createUnversionedRow(KEY_SCHEMA, 10), KEY_SCHEMA);
            var value = new Payload(TestDataUtils.createUnversionedRow(PAYLOAD_SCHEMA, 10), PAYLOAD_SCHEMA);
            entries.put(key, value);
        }
        return entries;
    }

    @Nested
    @DisplayName("Builder Validation Tests")
    class BuilderValidationTests {

        @Test
        @DisplayName("Build without PipelineContext throws IllegalStateException")
        void testBuildWithoutPipelineContext() {
            // Given: A builder with spec but no PipelineContext.
            var builder = TestComputationHarness.builder()
                    .setPipelineSpec(spec);

            // When & Then: Build throws IllegalStateException.
            var ex = assertThrows(IllegalStateException.class, builder::build);
            assertTrue(ex.getMessage().contains("PipelineContext"));
        }

        @Test
        @DisplayName("Build without PipelineSpec throws IllegalStateException")
        void testBuildWithoutPipelineSpec() {
            // Given: A builder with PipelineContext but no spec.
            var builder = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false));

            // When & Then: Build throws IllegalStateException.
            var ex = assertThrows(IllegalStateException.class, builder::build);
            assertTrue(ex.getMessage().contains("PipelineSpec"));
        }

        @Test
        @DisplayName("Build with required fields succeeds")
        void testBuildWithRequiredFields() {
            // Given: A builder with both required fields.
            var pipelineContext = ctx(COMPUTATION_ID, false);

            // When: Building the harness.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(pipelineContext)
                    .setPipelineSpec(spec)
                    .build();

            // Then: Harness is created and exposes an immutable snapshot of the context.
            assertNotNull(harness);
            assertNotNull(harness.getPipelineContextSnapshot());
        }

        @Test
        @DisplayName("Build with custom JobContext succeeds")
        void testBuildWithCustomJobContext() {
            // Given: A builder with a custom JobContext.
            // When: Building the harness.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(spec)
                    .setJobContext(new JobContext(Duration.ofMinutes(30)))
                    .build();

            // Then: Harness is created successfully.
            assertNotNull(harness);
        }

        @Test
        @DisplayName("builder() returns a new Builder instance")
        void testBuilderFactory() {
            assertNotNull(TestComputationHarness.builder());
        }
    }

    @Nested
    @DisplayName("Pipeline Spec Parsing Tests")
    class PipelineSpecParsingTests {

        @Test
        @DisplayName("setPipelineSpec(String): parses YSON text")
        void testFromString() {
            // Given: A harness built from a YSON string spec.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(spec)
                    .build();

            // Then: Streams from the spec are registered.
            assertAll(
                    () -> assertNotNull(harness.getStream("inputStream-0")),
                    () -> assertNotNull(harness.getStream("outputStream-0"))
            );
        }

        @Test
        @DisplayName("setPipelineSpec(YTreeNode): uses pre-parsed node")
        void testFromYTreeNode() {
            // Given: A pre-parsed YTreeNode spec.
            var node = YTreeTextSerializer.deserialize(spec);

            // When: Building harness with the node.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(node)
                    .build();

            // Then: Streams from the spec are registered.
            assertAll(
                    () -> assertNotNull(harness.getStream("inputStream-0")),
                    () -> assertNotNull(harness.getStream("outputStream-0"))
            );
        }

        @Test
        @DisplayName("setPipelineSpec(InputStream): reads from input stream")
        void testFromInputStream() {
            // Given: A spec provided as an InputStream.
            var is = new ByteArrayInputStream(spec.getBytes(StandardCharsets.UTF_8));

            // When: Building harness from the stream.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(is)
                    .build();

            // Then: Streams are registered.
            assertNotNull(harness.getStream("inputStream-0"));
        }

        @Test
        @DisplayName("setPipelineSpec(Reader): reads from reader")
        void testFromReader() {
            // Given: A spec provided as a Reader.
            // When: Building harness from the reader.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(new StringReader(spec))
                    .build();

            // Then: Streams are registered.
            assertNotNull(harness.getStream("inputStream-0"));
        }

        @Test
        @DisplayName("Last setPipelineSpec call wins")
        void testLastSetPipelineSpecWins() {
            // Given: A valid YTreeNode and an invalid string spec.
            var node = YTreeTextSerializer.deserialize(spec);

            // When: Setting invalid string first, then valid node — last call wins.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec("invalid yson")
                    .setPipelineSpec(node)
                    .build();

            // Then: The node spec (last set) is used.
            assertNotNull(harness.getStream("inputStream-0"));
        }

        @Test
        @DisplayName("Build registers streams from pipeline spec into context")
        void testBuildRegistersStreams() {
            // Given: A fresh PipelineContext.
            var pipelineContext = ctx(COMPUTATION_ID, false);

            // When: Building harness with the context.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(pipelineContext)
                    .setPipelineSpec(spec)
                    .build();

            // Then: Streams are present in the snapshot owned by the harness.
            var snapshotStreams = harness.getPipelineContextSnapshot().getStreamContext();
            assertAll(
                    () -> assertNotNull(snapshotStreams.getStream("inputStream-0")),
                    () -> assertNotNull(snapshotStreams.getStream("outputStream-0"))
            );
        }
    }

    @Nested
    @DisplayName("Jinja Template Tests")
    class JinjaTemplateTests {

        @Test
        @DisplayName("setJinjaContext(Map): renders template variables")
        void testFromMap() {
            // Given: A Jinja context map with template variables.
            var jinjaCtx = Map.<String, Object>of(
                    "computation_id", COMPUTATION_ID,
                    "wait_time", "10s"
            );

            // When: Building harness with Jinja spec and context.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(jinjaSpec)
                    .setJinjaContext(jinjaCtx)
                    .build();

            // Then: Template is rendered and streams/schemas are available.
            assertAll(
                    () -> assertNotNull(harness.getStream("inputStream-0")),
                    () -> assertNotNull(harness.getGroupBySchema(COMPUTATION_ID))
            );
        }

        @Test
        @DisplayName("setJinjaContext(Reader): loads properties from reader")
        void testFromReader() {
            // Given: Properties as a string reader.
            var props = "computation_id=" + COMPUTATION_ID + "\nwait_time=10s\n";

            // When: Building harness with Jinja context from reader.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(jinjaSpec)
                    .setJinjaContext(new StringReader(props))
                    .build();

            // Then: Template is rendered.
            assertNotNull(harness.getStream("inputStream-0"));
        }

        @Test
        @DisplayName("setJinjaContext(InputStream): loads properties from input stream")
        void testFromInputStream() {
            // Given: Properties as an input stream.
            var props = "computation_id=" + COMPUTATION_ID + "\nwait_time=10s\n";
            var is = new ByteArrayInputStream(props.getBytes(StandardCharsets.UTF_8));

            // When: Building harness with Jinja context from input stream.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(jinjaSpec)
                    .setJinjaContext(is)
                    .build();

            // Then: Template is rendered.
            assertNotNull(harness.getStream("inputStream-0"));
        }

        @Test
        @DisplayName("setJinjaContext(Properties): converts properties to context")
        void testFromProperties() {
            // Given: A Properties object with template variables.
            var props = new Properties();
            props.setProperty("computation_id", COMPUTATION_ID);
            props.setProperty("wait_time", "10s");

            // When: Building harness with Jinja context from Properties.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(jinjaSpec)
                    .setJinjaContext(props)
                    .build();

            // Then: Template is rendered.
            assertNotNull(harness.getStream("inputStream-0"));
        }

        @Test
        @DisplayName("setJinjaContext(Properties): loads from '=' delimited resource file")
        void testFromEqualsDelimitedPropertiesFile() throws IOException {
            // Given: Properties loaded from a '=' delimited resource file.
            var props = new Properties();
            try (var is = TestComputationHarnessTest.class.getClassLoader()
                    .getResourceAsStream("jinja-context-equals.properties")) {
                props.load(Objects.requireNonNull(is));
            }

            // When: Building harness with loaded properties.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(jinjaSpec)
                    .setJinjaContext(props)
                    .build();

            // Then: Template is rendered and streams/schemas are available.
            assertAll(
                    () -> assertNotNull(harness.getStream("inputStream-0")),
                    () -> assertNotNull(harness.getGroupBySchema(COMPUTATION_ID))
            );
        }

        @Test
        @DisplayName("setJinjaContext(Properties): loads from ':' delimited resource file")
        void testFromColonDelimitedPropertiesFile() throws IOException {
            // Given: Properties loaded from a ':' delimited resource file.
            var props = new Properties();
            try (var is = TestComputationHarnessTest.class.getClassLoader()
                    .getResourceAsStream("jinja-context-colon.properties")) {
                props.load(Objects.requireNonNull(is));
            }

            // When: Building harness with loaded properties.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(jinjaSpec)
                    .setJinjaContext(props)
                    .build();

            // Then: Template is rendered and streams/schemas are available.
            assertAll(
                    () -> assertNotNull(harness.getStream("inputStream-0")),
                    () -> assertNotNull(harness.getGroupBySchema(COMPUTATION_ID))
            );
        }
    }

    @Nested
    @DisplayName("External State Schema Tests")
    class ExternalStateSchemaTests {

        @Test
        @DisplayName("setExternalStateSchemas: replaces all schemas")
        void testSetAll() {
            // Given: A map of external state schemas.
            // When: Building harness with external state schemas.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(spec)
                    .setExternalStateSchemas(Map.of("s1", SCHEMAS.createMixedSchema(4)))
                    .build();

            // Then: Harness is created successfully.
            assertNotNull(harness);
        }

        @Test
        @DisplayName("addExternalStateSchema: adds individual schemas")
        void testAddIndividual() {
            // Given: Individual external state schemas.
            var schema = SCHEMAS.createMixedSchema(4);

            // When: Adding schemas one by one.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(spec)
                    .addExternalStateSchema("s1", schema)
                    .addExternalStateSchema("s2", schema)
                    .build();

            // Then: Harness is created successfully.
            assertNotNull(harness);
        }
    }

    @Nested
    @DisplayName("Accessor Method Tests")
    class AccessorMethodTests {

        @Test
        @DisplayName("getPipelineContextSnapshot: returns the snapshot built from the configured context")
        void testGetPipelineContextSnapshot() {
            // Given: A specific PipelineContext with a known computation.
            var pipelineContext = ctx(COMPUTATION_ID, false);

            // When: Building harness with the context.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(pipelineContext)
                    .setPipelineSpec(spec)
                    .build();

            // Then: The snapshot exposes the computation that was registered in the original context.
            var snapshot = harness.getPipelineContextSnapshot();
            assertNotNull(snapshot);
            assertNotNull(snapshot.getComputation(COMPUTATION_ID));
        }

        @Test
        @DisplayName("getStream: returns registered stream by id")
        void testGetStream() {
            // Given: A harness with registered streams.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(spec)
                    .build();

            // When: Retrieving streams by id.
            FlowStream<?> in = harness.getStream("inputStream-0");
            FlowStream<?> out = harness.getStream("outputStream-0");

            // Then: Streams are found with correct ids.
            assertAll(
                    () -> assertNotNull(in),
                    () -> assertEquals("inputStream-0", in.getStreamId()),
                    () -> assertNotNull(out),
                    () -> assertEquals("outputStream-0", out.getStreamId())
            );
        }

        @Test
        @DisplayName("getStream: returns null for non-existent stream")
        void testGetStreamNotFound() {
            // Given: A harness with registered streams.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(spec)
                    .build();

            // When & Then: Non-existent stream returns null.
            assertNull(harness.getStream("non-existent-stream"));
        }

        @Test
        @DisplayName("getGroupBySchema: returns schema for computation with group_by_schema")
        void testGetGroupBySchema() {
            // Given: A harness with a computation that has group_by_schema.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(spec)
                    .build();

            // When: Retrieving the group-by schema.
            TableSchema gbs = harness.getGroupBySchema(COMPUTATION_ID);

            // Then: Schema is present and non-empty.
            assertNotNull(gbs);
            assertTrue(gbs.getColumnsCount() > 0);
        }

        @Test
        @DisplayName("getGroupBySchema: returns null for non-existent computation")
        void testGetGroupBySchemaNotFound() {
            // Given: A harness with registered computations.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(spec)
                    .build();

            // When & Then: Non-existent computation returns null.
            assertNull(harness.getGroupBySchema("non-existent"));
        }
    }

    @Nested
    @DisplayName("DoProcess Tests")
    class DoProcessTests {

        @Test
        @DisplayName("doProcess: passthrough computation returns output messages")
        void testPassthrough() {
            // Given: A harness and a request with 5 messages.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(spec)
                    .build();
            var request = TestDoProcessRequest.builder(COMPUTATION_ID)
                    .setMessages(extMessages(5))
                    .build();

            // When: Processing the request.
            TestDoProcessResponse response = harness.doProcess(request);

            // Then: All 5 messages are passed through.
            assertAll(
                    () -> assertNotNull(response),
                    () -> assertEquals(5, response.getOutputMessagesFlatten().size())
            );
        }

        @Test
        @DisplayName("doProcess: empty messages returns empty output")
        void testEmptyMessages() {
            // Given: A request with no messages.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(spec)
                    .build();
            var request = TestDoProcessRequest.builder(COMPUTATION_ID)
                    .setMessages(Collections.emptyList())
                    .build();

            // When: Processing the empty request.
            TestDoProcessResponse response = harness.doProcess(request);

            // Then: Output is empty.
            assertAll(
                    () -> assertNotNull(response),
                    () -> assertTrue(response.getOutputMessagesFlatten().isEmpty())
            );
        }

        @Test
        @DisplayName("doProcess: non-existent computation throws RuntimeException")
        void testComputationNotFound() {
            // Given: A request for a non-existent computation.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(spec)
                    .build();
            var request = TestDoProcessRequest.builder("non-existent")
                    .setMessages(extMessages(1))
                    .build();

            // When & Then: Exception is thrown.
            assertThrows(RuntimeException.class, () -> harness.doProcess(request));
        }

        @Test
        @DisplayName("doProcess: multiple messages processed correctly")
        void testMultipleMessages() {
            // Given: A request with 50 messages.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(spec)
                    .build();
            var request = TestDoProcessRequest.builder(COMPUTATION_ID)
                    .setMessages(extMessages(50))
                    .build();

            // When: Processing the request.
            TestDoProcessResponse response = harness.doProcess(request);

            // Then: All 50 messages are passed through.
            assertEquals(50, response.getOutputMessagesFlatten().size());
        }

        @Test
        @DisplayName("doProcess: consecutive calls produce consistent results")
        void testIdempotency() {
            // Given: A harness and a request with 3 messages.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(spec)
                    .build();
            var msgs = extMessages(3);
            var request = TestDoProcessRequest.builder(COMPUTATION_ID)
                    .setMessages(msgs)
                    .build();

            // When: Processing the same request twice.
            TestDoProcessResponse r1 = harness.doProcess(request);
            TestDoProcessResponse r2 = harness.doProcess(request);

            // Then: Both responses have the same size.
            assertAll(
                    () -> assertEquals(
                            r1.getOutputMessagesFlatten().size(),
                            r2.getOutputMessagesFlatten().size()
                    ),
                    () -> assertEquals(3, r1.getOutputMessagesFlatten().size())
            );
        }
    }

    @Nested
    @DisplayName("Distribute Tests")
    class DistributeTests {

        @Test
        @DisplayName("doProcess: source computation propagates the distribute flag")
        void testDistributeFlag() {
            // Given: A source computation that emits each message twice (distributed + not).
            var context = new PipelineContext();
            context.registerComputation(SourceComputation.builder()
                    .setComputationId(SOURCE_COMPUTATION_ID)
                    .setProcessFunction((RowFunction) (message, output, ctx) -> {
                        output.addMessage(Message.builder()
                                .setStreamId(message.getStreamId())
                                .setPayload(message.getPayload())
                                .build(), true);
                        output.addMessage(Message.builder()
                                .setStreamId(message.getStreamId())
                                .setPayload(message.getPayload())
                                .build(), false);
                    })
                    .build());

            var harness = TestComputationHarness.builder()
                    .setPipelineContext(context)
                    .setPipelineSpec(spec)
                    .build();
            var request = TestDoProcessRequest.builder(SOURCE_COMPUTATION_ID)
                    .setMessages(extMessages(1))
                    .build();

            // When: Processing the request.
            TestDoProcessResponse response = harness.doProcess(request);

            // Then: The distribute flags are carried through, in order.
            var distribute = response.getTransformResults().stream()
                    .flatMap(tr -> tr.getDistribute().stream())
                    .toList();
            assertEquals(List.of(true, false), distribute);
        }
    }

    @Nested
    @DisplayName("External State Response Tests")
    class ExternalStateResponseTests {

        @Test
        @DisplayName("external state names: empty when no external state in request")
        void testGetAllExternalStatesEmpty() {
            // Given: A request without any external state.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(spec)
                    .build();
            var request = TestDoProcessRequest.builder(COMPUTATION_ID)
                    .setMessages(extMessages(1))
                    .build();

            // When: Processing the request.
            TestDoProcessResponse response = harness.doProcess(request);

            // Then: There are no external state names.
            assertTrue(response.allStates().externalNames().isEmpty());
        }

        @Test
        @DisplayName("getExternalStateNames/Keys: reflect the states the computation modified")
        void testGetAllExternalStates() {
            // Given: A computation that writes two external states for every message, and a request
            // with three messages. Only modified states are sent back, so both states show up
            // keyed by the (distinct) message keys.
            var stateNames = List.of(EXT_STATE_A, EXT_STATE_B);
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctxWritingExternalStates(stateNames))
                    .setPipelineSpec(specDeclaringExternalStates(stateNames))
                    .addExternalStateSchema(EXT_STATE_A, PAYLOAD_SCHEMA)
                    .addExternalStateSchema(EXT_STATE_B, PAYLOAD_SCHEMA)
                    .build();
            var messages = extMessages(3);
            var request = TestDoProcessRequest.builder(COMPUTATION_ID)
                    .setMessages(messages)
                    .build();

            // When: Processing the request.
            TestDoProcessResponse response = harness.doProcess(request);

            // Then: Both modified external states are present, keyed by the message keys.
            var expectedKeys = keysOf(messages);
            assertAll(
                    () -> assertEquals(Set.of(EXT_STATE_A, EXT_STATE_B), response.allStates().externalNames()),
                    () -> assertEquals(expectedKeys, response.allStates().externalKeys(EXT_STATE_A)),
                    () -> assertEquals(expectedKeys, response.allStates().externalKeys(EXT_STATE_B)),
                    () -> assertEquals(expectedKeys.size(), response.allStates().externalSize(EXT_STATE_A)),
                    () -> assertEquals(expectedKeys.size(), response.allStates().externalSize(EXT_STATE_B))
            );
        }

        @Test
        @DisplayName("getExternalStateNames/Keys: returned sets are unmodifiable")
        void testGetExternalStateMetadataUnmodifiable() {
            // Given: A response with one external state modified by the computation.
            var stateNames = List.of(EXT_STATE_A);
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctxWritingExternalStates(stateNames))
                    .setPipelineSpec(specDeclaringExternalStates(stateNames))
                    .addExternalStateSchema(EXT_STATE_A, PAYLOAD_SCHEMA)
                    .build();
            var request = TestDoProcessRequest.builder(COMPUTATION_ID)
                    .setMessages(extMessages(1))
                    .build();
            var response = harness.doProcess(request);

            // When & Then: Mutating the returned sets throws.
            var names = response.allStates().externalNames();
            var keys = response.allStates().externalKeys(EXT_STATE_A);
            assertAll(
                    () -> assertThrows(UnsupportedOperationException.class, names::clear),
                    () -> assertThrows(UnsupportedOperationException.class, keys::clear)
            );
        }

        @Test
        @DisplayName("getExternalStateKeys(name): returns the keys the computation modified")
        void testGetExternalStatesByName() {
            // Given: A computation that writes one external state for every message, and a request
            // with four messages.
            var stateNames = List.of(EXT_STATE_A);
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctxWritingExternalStates(stateNames))
                    .setPipelineSpec(specDeclaringExternalStates(stateNames))
                    .addExternalStateSchema(EXT_STATE_A, PAYLOAD_SCHEMA)
                    .build();
            var messages = extMessages(4);
            var request = TestDoProcessRequest.builder(COMPUTATION_ID)
                    .setMessages(messages)
                    .build();

            // When: Processing the request.
            TestDoProcessResponse response = harness.doProcess(request);

            // Then: One entry per message key is returned for the requested state.
            var expectedKeys = keysOf(messages);
            Set<UnversionedRow> entries = response.allStates().externalKeys(EXT_STATE_A);
            assertAll(
                    () -> assertEquals(expectedKeys, entries),
                    () -> assertFalse(entries.isEmpty())
            );
        }

        @Test
        @DisplayName("getExternalStateKeys(name): returns empty set for unknown state")
        void testGetExternalStatesByNameUnknown() {
            // Given: A request without any external state.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(spec)
                    .build();
            var request = TestDoProcessRequest.builder(COMPUTATION_ID)
                    .setMessages(extMessages(1))
                    .build();

            // When: Processing the request.
            TestDoProcessResponse response = harness.doProcess(request);

            // Then: Querying an unknown state returns an empty set.
            assertTrue(response.allStates().externalKeys("missing").isEmpty());
        }

        @Test
        @DisplayName("getState(): exposes loaded states the computation did not modify")
        void testLoadedUnmodifiedExternalStatesAreVisible() {
            // Given: A passthrough computation (does not touch state) and a request that supplies
            // three external state entries.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(spec)
                    .addExternalStateSchema(EXT_STATE_A, PAYLOAD_SCHEMA)
                    .build();
            var loaded = loadedExternalEntries(3);
            var builder = TestDoProcessRequest.builder(COMPUTATION_ID)
                    .setMessages(extMessages(1));
            loaded.forEach((k, v) -> builder.setState(StateDescriptors.external(EXT_STATE_A), k, v));
            var request = builder.build();

            // When: Processing the request.
            TestDoProcessResponse response = harness.doProcess(request);

            // Then: The loaded states are visible via the all-states view, but the modified view is
            // empty because the computation changed nothing.
            var loadedKeys = loaded.keySet().stream().map(Payload::getRow).collect(Collectors.toSet());
            assertAll(
                    () -> assertEquals(loadedKeys, response.allStates().externalKeys(EXT_STATE_A)),
                    () -> assertEquals(3, response.allStates().externalSize(EXT_STATE_A)),
                    () -> assertTrue(response.modifiedStates().externalNames().isEmpty()),
                    () -> assertEquals(0, response.modifiedStates().externalSize(EXT_STATE_A))
            );
        }

        @Test
        @DisplayName("state readers are empty (not null) for unknown names and keys")
        void testStateAccessorsAreNullSafe() {
            // Given: A processed request with no states at all.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(spec)
                    .build();
            var response = harness.doProcess(TestDoProcessRequest.builder(COMPUTATION_ID)
                    .setMessages(extMessages(1))
                    .build());
            var someKey = new Payload(TestDataUtils.createUnversionedRow(KEY_SCHEMA, 10), KEY_SCHEMA);

            // Then: Unknown names/keys yield empty readers/sets rather than throwing.
            assertAll(
                    () -> assertTrue(response.allStates()
                            .get(StateDescriptors.external("/missing"), someKey).get().isEmpty()),
                    () -> assertTrue(response.allStates()
                            .get(StateDescriptors.raw("missing"), someKey).get().isEmpty()),
                    () -> assertTrue(response.modifiedStates()
                            .get(StateDescriptors.external("/missing"), someKey).get().isEmpty()),
                    () -> assertTrue(response.allStates().externalKeys("missing").isEmpty()),
                    () -> assertTrue(response.allStates().internalKeys("missing").isEmpty()),
                    () -> assertEquals(0, response.allStates().externalSize("missing"))
            );
        }

        @Test
        @DisplayName("getState(): a loaded key the computation modifies appears once with the modified value")
        void testLoadedAndModifiedExternalStateOverlay() {
            // Given: A computation that writes the external state, and a single key that is both
            // supplied in the request (loaded) and written by the computation (modified).
            var stateNames = List.of(EXT_STATE_A);
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctxWritingExternalStates(stateNames))
                    .setPipelineSpec(specDeclaringExternalStates(stateNames))
                    .addExternalStateSchema(EXT_STATE_A, PAYLOAD_SCHEMA)
                    .build();

            var key = new Payload(TestDataUtils.createUnversionedRow(KEY_SCHEMA, 10), KEY_SCHEMA);
            var prePayload = new Payload(TestDataUtils.createUnversionedRow(PAYLOAD_SCHEMA, 10), PAYLOAD_SCHEMA);
            var messagePayload = new Payload(TestDataUtils.createUnversionedRow(PAYLOAD_SCHEMA, 10), PAYLOAD_SCHEMA);
            var message = ExtendedMessage.builder()
                    .setStreamId("inputStream-0")
                    .setStreamSpecId(0L)
                    .setMessageId("<id>0")
                    .setSystemTimestamp(1712182928L)
                    .setEventTimestamp(1712182927L)
                    .setPayload(messagePayload)
                    .setKey(key)
                    .build();
            var request = TestDoProcessRequest.builder(COMPUTATION_ID)
                    .setMessages(List.of(message))
                    .setState(StateDescriptors.external(EXT_STATE_A), key, prePayload)
                    .build();

            // When: Processing the request.
            TestDoProcessResponse response = harness.doProcess(request);

            // Then: The key collapses to a single entry (no stale loaded duplicate), and the
            // all-states view returns the modified value, not the loaded one.
            var extA = StateDescriptors.external(EXT_STATE_A);
            Optional<Payload> allValue = response.allStates().get(extA, key).get();
            Optional<Payload> modifiedValue = response.modifiedStates().get(extA, key).get();
            assertAll(
                    () -> assertEquals(1, response.allStates().externalSize(EXT_STATE_A)),
                    () -> assertEquals(Set.of(key.getRow()), response.allStates().externalKeys(EXT_STATE_A)),
                    () -> assertEquals(1, response.modifiedStates().externalSize(EXT_STATE_A)),
                    () -> assertTrue(allValue.isPresent()),
                    () -> assertEquals(modifiedValue.orElseThrow(), allValue.orElseThrow()),
                    () -> assertNotEquals(prePayload, allValue.orElseThrow())
            );
        }
    }

    @Nested
    @DisplayName("Missing Dynamic Spec Tests")
    class MissingDynamicSpecTests {

        /**
         * Parses the base spec and removes the top-level {@code dynamic_spec} block entirely.
         */
        private YTreeNode specWithoutDynamicSpec() {
            var node = YTreeTextSerializer.deserialize(spec);
            node.asMap().remove("dynamic_spec");
            return node;
        }

        /**
         * Parses the base spec and removes the per-computation entries from
         * {@code dynamic_spec/computations}, leaving the surrounding blocks present but empty.
         */
        private YTreeNode specWithoutDynamicComputations() {
            var node = YTreeTextSerializer.deserialize(spec);
            node.asMap().get("dynamic_spec").asMap().get("computations").asMap().clear();
            return node;
        }

        @Test
        @DisplayName("doProcess: succeeds when dynamic_spec is absent")
        void testDoProcessWithoutDynamicSpec() {
            // Given: A spec with no dynamic_spec block at all.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(specWithoutDynamicSpec())
                    .build();
            var request = TestDoProcessRequest.builder(COMPUTATION_ID)
                    .setMessages(extMessages(5))
                    .build();

            // When: Processing the request.
            TestDoProcessResponse response = harness.doProcess(request);

            // Then: An empty dynamic block is synthesized on the fly and processing succeeds.
            assertAll(
                    () -> assertNotNull(response),
                    () -> assertEquals(5, response.getOutputMessagesFlatten().size())
            );
        }

        @Test
        @DisplayName("doProcess: succeeds when dynamic_spec has no computations entry")
        void testDoProcessWithoutDynamicComputationsEntry() {
            // Given: A spec whose dynamic_spec/computations does not contain the computation.
            var harness = TestComputationHarness.builder()
                    .setPipelineContext(ctx(COMPUTATION_ID, false))
                    .setPipelineSpec(specWithoutDynamicComputations())
                    .build();
            var request = TestDoProcessRequest.builder(COMPUTATION_ID)
                    .setMessages(extMessages(3))
                    .build();

            // When: Processing the request.
            TestDoProcessResponse response = harness.doProcess(request);

            // Then: Processing succeeds with a synthesized empty dynamic block.
            assertAll(
                    () -> assertNotNull(response),
                    () -> assertEquals(3, response.getOutputMessagesFlatten().size())
            );
        }
    }
}
