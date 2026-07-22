package tech.ytsaurus.flow.service;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.persistence.Entity;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.context.PipelineContextSnapshot;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.job.JobContext;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Message;
import tech.ytsaurus.flow.rpc.EResponseStatus;
import tech.ytsaurus.flow.rpc.TReqProcessBatch;
import tech.ytsaurus.flow.rpc.TReqPutJob;
import tech.ytsaurus.flow.stream.FlowStreams;
import tech.ytsaurus.flow.testutils.ProtobufRequestBuilder;
import tech.ytsaurus.flow.testutils.SchemaGenerator;
import tech.ytsaurus.flow.testutils.SpecGenerator;
import tech.ytsaurus.flow.testutils.StreamInfo;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.ytsaurus.flow.testutils.ComputationTestUtils.passthroughComputation;
import static tech.ytsaurus.flow.testutils.ComputationTestUtils.passthroughSourceComputation;

/**
 * Tests for {@link CompanionRequestProcessor} covering:
 * - ProcessBatch operations with various configurations
 * - State management (internal and external)
 * - Schema variations and edge cases
 * - Resource tracking and error handling
 */
@DisplayName("CompanionRequestProcessor Tests")
public class CompanionRequestProcessorTest {

    private static final String TEST_COMPUTATION_ID = "test-computation";
    private static final String TEST_SOURCE_COMPUTATION_ID = "test-source-computation";

    private TestFixture fixture;

    @BeforeEach
    void setUp() {
        fixture = new TestFixture();
    }

    /**
     * Test fixture providing common setup and helper methods.
     */
    private static class TestFixture {
        private final PipelineContext pipelineContext;
        private final JobContext jobContext;
        private final SchemaGenerator schemaGenerator;

        private CompanionRequestProcessor cachedProcessor;

        TestFixture() {
            this.pipelineContext = new PipelineContext();
            this.jobContext = new JobContext(Duration.ofMinutes(5));
            this.schemaGenerator = SchemaGenerator.builder().build();

            // Register test computations
            pipelineContext.registerComputation(passthroughComputation(TEST_COMPUTATION_ID));
            pipelineContext.registerComputation(passthroughSourceComputation(TEST_SOURCE_COMPUTATION_ID));
        }

        CompanionRequestProcessor processor() {
            if (cachedProcessor == null) {
                cachedProcessor = new CompanionRequestProcessor(
                        new PipelineContextSnapshot(pipelineContext),
                        jobContext
                );
            }
            return cachedProcessor;
        }

        ProtobufRequestBuilder createRequestBuilder() {
            return new ProtobufRequestBuilder()
                    .setSchemaGenerator(schemaGenerator);
        }

        CompanionRequestProcessor.ProcessBatchResult processBatch(TReqProcessBatch request) throws Exception {
            return processor().processBatch(request);
        }

        CompanionRequestProcessor.CompanionInfoResult getCompanionInfo() {
            return processor().getCompanionInfo();
        }

        void registerComputation(Computation computation) {
            pipelineContext.registerComputation(computation);
        }
    }

    @Nested
    @DisplayName("ProcessBatch Tests")
    class ProcessBatchTests {

        @Test
        @DisplayName("Basic processing: with job info")
        void testProcessBatchWithJobInfo() throws Exception {
            // Given: A request with job info.
            TReqProcessBatch request = fixture.createRequestBuilder()
                    .setComputationId(TEST_COMPUTATION_ID)
                    .setMessageCount(10)
                    .setAddJobInfo(true)
                    .createProcessBatch();

            // When: Processing batch
            CompanionRequestProcessor.ProcessBatchResult result = fixture.processBatch(request);

            // Then: Request succeeds with valid response.
            assertAll(
                    () -> assertEquals(EResponseStatus.RS_OK, result.getStatus()),
                    () -> assertNotNull(result.getData()),
                    () -> assertNotNull(result.getResourceStats()),
                    () -> assertTrue(result.getResourceStats().getAllocatedBytes().toBytes() > 0)
            );
        }

        @ParameterizedTest
        @ValueSource(ints = {1, 10, 100, 1000})
        @DisplayName("Variable message counts: process different batch sizes")
        void testProcessBatchWithDifferentMessageCounts(int messageCount) throws Exception {
            // Given: Requests with varying message counts.
            TReqProcessBatch request = fixture.createRequestBuilder()
                    .setComputationId(TEST_COMPUTATION_ID)
                    .setMessageCount(messageCount)
                    .createProcessBatch();

            // When: Processing batch.
            CompanionRequestProcessor.ProcessBatchResult result = fixture.processBatch(request);

            // Then: All requests succeed.
            assertAll(
                    () -> assertNotNull(result, "Result should not be null for message count: " + messageCount),
                    () -> assertEquals(EResponseStatus.RS_OK, result.getStatus()),
                    () -> assertNotNull(result.getData())
            );
        }

        @Test
        @DisplayName("Empty batch: no messages to process")
        void testProcessBatchEmptyMessages() throws Exception {
            // Given: A request with zero messages.
            TReqProcessBatch request = fixture.createRequestBuilder()
                    .setComputationId(TEST_COMPUTATION_ID)
                    .setMessageCount(0)
                    .createProcessBatch();

            // When: Processing empty batch.
            CompanionRequestProcessor.ProcessBatchResult result = fixture.processBatch(request);

            // Then: Request succeeds with no output.
            assertAll(
                    () -> assertEquals(EResponseStatus.RS_OK, result.getStatus()),
                    () -> assertNotNull(result.getData()),
                    () -> assertEquals(0, result.getData().getOutputCount())
            );
        }

        @Test
        @DisplayName("Error handling: computation not found")
        void testProcessBatchComputationNotFound() {
            // Given: A request for non-existent computation.
            TReqProcessBatch request = fixture.createRequestBuilder()
                    .setComputationId("non-existent-computation")
                    .setMessageCount(10)
                    .createProcessBatch();

            // When & Then: Exception is thrown.
            IllegalArgumentException exception = assertThrows(
                    IllegalArgumentException.class,
                    () -> fixture.processBatch(request)
            );
            assertTrue(exception.getMessage().contains("Computation not found"));
            assertTrue(exception.getMessage().contains("non-existent-computation"));
        }

        @Test
        @DisplayName("Error handling: job not found without job info")
        void testProcessBatchJobNotFound() throws Exception {
            // Given: A request without job info for a non-existent job.
            var nonExistentJobId = GUID.create();
            TReqProcessBatch request = fixture.createRequestBuilder()
                    .setComputationId(TEST_COMPUTATION_ID)
                    .setMessageCount(10)
                    .setJobId(nonExistentJobId)
                    .setAddJobInfo(false)
                    .createProcessBatch();

            // When: Processing batch.
            CompanionRequestProcessor.ProcessBatchResult result = fixture.processBatch(request);

            // Then: RS_JOB_NOT_FOUND status is returned.
            assertAll(
                    () -> assertEquals(EResponseStatus.RS_JOB_NOT_FOUND, result.getStatus()),
                    () -> assertNotNull(result.getResourceStats())
            );
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        @DisplayName("Job handling: with job info or via putJob")
        void testJobHandling(boolean useJobInfo) throws Exception {
            // Given: A new job ID.
            var newJobId = GUID.create();

            if (!useJobInfo) {
                // When using putJob: First put the job.
                TReqPutJob putJobRequest = fixture.createRequestBuilder()
                        .setComputationId(TEST_COMPUTATION_ID)
                        .setJobId(newJobId)
                        .createPutJob();
                CompanionRequestProcessor.PutJobResult putResult = fixture.processor().putJob(putJobRequest);
                assertEquals(EResponseStatus.RS_OK, putResult.getStatus());
            }

            // Given: ProcessBatch request with or without job info.
            TReqProcessBatch request = fixture.createRequestBuilder()
                    .setComputationId(TEST_COMPUTATION_ID)
                    .setMessageCount(10)
                    .setJobId(newJobId)
                    .setAddJobInfo(useJobInfo)
                    .createProcessBatch();

            // When: Processing batch.
            CompanionRequestProcessor.ProcessBatchResult result = fixture.processBatch(request);

            // Then: Request succeeds.
            assertAll(
                    () -> assertEquals(EResponseStatus.RS_OK, result.getStatus()),
                    () -> assertNotNull(result.getData()),
                    () -> assertNotNull(result.getResourceStats())
            );
        }
    }

    @Nested
    @DisplayName("State Management Tests")
    class InternalStateManagementTests {

        @Test
        @DisplayName("Internal state: single state processing")
        void testProcessBatchWithInternalState() throws Exception {
            // Given: A request with internal state.
            TReqProcessBatch request = fixture.createRequestBuilder()
                    .setComputationId(TEST_COMPUTATION_ID)
                    .setMessageCount(10)
                    .addInternalState("test-internal-state", 100)
                    .createProcessBatch();

            // When: Processing batch.
            CompanionRequestProcessor.ProcessBatchResult result = fixture.processBatch(request);

            // Then: The passthrough computation does not modify the state, so only modified states
            // are sent back -- none here.
            assertAll(
                    () -> assertEquals(EResponseStatus.RS_OK, result.getStatus()),
                    () -> assertNotNull(result.getData()),
                    () -> assertEquals(0, result.getData().getInternalStatesCount()),
                    () -> assertEquals(0, result.getData().getExternalStatesCount())
            );
        }

        @Test
        @DisplayName("External state: single state processing")
        void testProcessBatchWithExternalState() throws Exception {
            // Given: A request with external state.
            var externalStateSchema = fixture.schemaGenerator.createMixedSchema(10);
            TReqProcessBatch request = fixture.createRequestBuilder()
                    .setComputationId(TEST_COMPUTATION_ID)
                    .setMessageCount(10)
                    .addExternalState("test-external-state", 50, externalStateSchema)
                    .createProcessBatch();

            // When: Processing batch.
            CompanionRequestProcessor.ProcessBatchResult result = fixture.processBatch(request);

            // Then: The passthrough computation does not modify the state, so only modified states
            // are sent back -- none here.
            assertAll(
                    () -> assertEquals(EResponseStatus.RS_OK, result.getStatus()),
                    () -> assertNotNull(result.getData()),
                    () -> assertEquals(0, result.getData().getInternalStatesCount()),
                    () -> assertEquals(0, result.getData().getExternalStatesCount())
            );
        }

        @Test
        @DisplayName("Multiple states: both internal and external")
        void testProcessBatchWithMultipleStates() throws Exception {
            // Given: A request with multiple states.
            var externalStateSchema = fixture.schemaGenerator.createMixedSchema(10);
            TReqProcessBatch request = fixture.createRequestBuilder()
                    .setComputationId(TEST_COMPUTATION_ID)
                    .setMessageCount(10)
                    .addInternalState("internal-state-1", 100)
                    .addInternalState("internal-state-2", 200)
                    .addExternalState("external-state-1", 50, externalStateSchema)
                    .addExternalState("external-state-2", 75, externalStateSchema)
                    .createProcessBatch();

            // When: Processing batch.
            CompanionRequestProcessor.ProcessBatchResult result = fixture.processBatch(request);

            // Then: The passthrough computation does not modify any state, so no states are sent
            // back even though several were supplied in the request.
            assertAll(
                    () -> assertEquals(EResponseStatus.RS_OK, result.getStatus()),
                    () -> assertEquals(0, result.getData().getInternalStatesCount()),
                    () -> assertEquals(0, result.getData().getExternalStatesCount())
            );
        }
    }

    @Nested
    @DisplayName("Schema Variation Tests")
    class SchemaVariationTests {

        @ParameterizedTest
        @ValueSource(strings = {"string", "number", "mixed"})
        @DisplayName("Various schemas: process different column types")
        void testProcessBatchWithVariousSchemas(String schemaType) throws Exception {
            // Given: A request with specified schema type.
            var schema = switch (schemaType) {
                case "string" -> fixture.schemaGenerator.createStringSchema(10);
                case "number" -> fixture.schemaGenerator.createNumberSchema(10);
                case "mixed" -> fixture.schemaGenerator.createMixedSchema(20);
                default -> throw new IllegalArgumentException("Unknown schema type: " + schemaType);
            };

            TReqProcessBatch request = fixture.createRequestBuilder()
                    .setComputationId(TEST_COMPUTATION_ID)
                    .setMessageCount(10)
                    .setPayloadSchema(schema)
                    .createProcessBatch();

            // When: Processing batch.
            CompanionRequestProcessor.ProcessBatchResult result = fixture.processBatch(request);

            // Then: Request succeeds.
            assertAll(
                    () -> assertEquals(EResponseStatus.RS_OK, result.getStatus()),
                    () -> assertNotNull(result.getData())
            );
        }

        @Test
        @DisplayName("Large schema: process many columns")
        void testProcessBatchWithVeryLargeSchema() throws Exception {
            // Given: A request with large schema (1000 columns).
            var largeSchema = fixture.schemaGenerator.createMixedSchema(1000);
            TReqProcessBatch request = fixture.createRequestBuilder()
                    .setComputationId(TEST_COMPUTATION_ID)
                    .setMessageCount(10)
                    .setPayloadSchema(largeSchema)
                    .createProcessBatch();

            // When: Processing batch
            CompanionRequestProcessor.ProcessBatchResult result = fixture.processBatch(request);

            // Then: Request succeeds
            assertAll(
                    () -> assertEquals(EResponseStatus.RS_OK, result.getStatus()),
                    () -> assertNotNull(result.getData())
            );
        }

        @Test
        @DisplayName("Huge schema: exceeds column limit")
        void testProcessBatchHugeSchemaThrows() {
            // Given: A request with huge schema (1024 columns - exceeds limit).
            var largeSchema = fixture.schemaGenerator.createMixedSchema(1024);
            TReqProcessBatch request = fixture.createRequestBuilder()
                    .setComputationId(TEST_COMPUTATION_ID)
                    .setMessageCount(10)
                    .setPayloadSchema(largeSchema)
                    .createProcessBatch();

            // When & Then: Exception is thrown.
            IllegalStateException exception = assertThrows(
                    IllegalStateException.class,
                    () -> fixture.processBatch(request)
            );
            assertTrue(exception.getMessage().contains("Unsupported number of values"));
        }

        @Test
        @DisplayName("Custom key schema: process with custom keys")
        void testProcessBatchWithCustomKeySchema() throws Exception {
            // Given: A request with custom key schema.
            var customKeySchema = fixture.schemaGenerator.createStringSchema(3);
            TReqProcessBatch request = fixture.createRequestBuilder()
                    .setComputationId(TEST_COMPUTATION_ID)
                    .setMessageCount(10)
                    .setKeySchema(customKeySchema)
                    .setKeyStringsSize(50)
                    .createProcessBatch();

            // When: Processing batch.
            CompanionRequestProcessor.ProcessBatchResult result = fixture.processBatch(request);

            // Then: Request succeeds.
            assertAll(
                    () -> assertEquals(EResponseStatus.RS_OK, result.getStatus()),
                    () -> assertNotNull(result.getData())
            );
        }
    }

    @Nested
    @DisplayName("Custom Computation Tests")
    class CustomComputationTests {

        @Test
        @DisplayName("Filtering computation: emit only matching messages")
        void testProcessBatchWithFilteringComputation() throws Exception {
            // Given: A computation that filters messages (every second message).
            String filterComputationId = "filter-computation";
            Computation filterComputation = Computation.builder()
                    .setComputationId(filterComputationId)
                    .setProcessFunction(new RowFunction() {
                        private final AtomicInteger counter = new AtomicInteger(0);

                        @Override
                        public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
                            // Filter: only pass every second message
                            if (counter.incrementAndGet() % 2 == 0) {
                                output.addMessage(
                                        Message.builder()
                                                .setStreamId(message.getStreamId())
                                                .setPayload(message.getPayload())
                                                .build()
                                );
                            }
                        }
                    })
                    .build();
            fixture.registerComputation(filterComputation);

            TReqProcessBatch request = fixture.createRequestBuilder()
                    .setComputationId(filterComputationId)
                    .setMessageCount(10)
                    .createProcessBatch();

            // When: Processing batch.
            CompanionRequestProcessor.ProcessBatchResult result = fixture.processBatch(request);

            // Then: Only half the messages are emitted.
            assertAll(
                    () -> assertEquals(EResponseStatus.RS_OK, result.getStatus()),
                    () -> assertNotNull(result.getData()),
                    () -> assertEquals(5, result.getData().getOutputCount())
            );
        }
    }

    @Nested
    @DisplayName("Typed Computation Tests")
    class TypedComputationTests {

        @Test
        void testProcessBatchWithTypedComputation() throws Exception {
            // Given: A computation that filters messages (every second message).
            String typedComputationId = "typed-computation";
            fixture.pipelineContext.registerStream(FlowStreams.typed("typed-stream", Mixed2.class));
            fixture.pipelineContext.registerStream(FlowStreams.typed("inputStream-0", Mixed2.class));
            Computation filterComputation = Computation.builder()
                    .setComputationId(typedComputationId)
                    .setProcessFunction(new RowFunction() {
                        private final AtomicInteger counter = new AtomicInteger(0);

                        @Override
                        public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
                            // Filter: only pass every second message
                            if (counter.incrementAndGet() % 2 == 0) {
                                var outputPayload = new Mixed2();
                                Mixed2 inputPayload = message.getPayload();
                                outputPayload.hashColumn = inputPayload.hashColumn;
                                outputPayload.strColumn0 = inputPayload.strColumn0;
                                outputPayload.numColumn0 = inputPayload.numColumn0;
                                output.addMessage(
                                        Message.builder()
                                                .setStreamId("typed-stream")
                                                .setPayload(outputPayload)
                                                .build()
                                );
                            }
                        }
                    })
                    .build();
            fixture.registerComputation(filterComputation);

            // When: Processing batch.
            TableSchema mixedSchema = SchemaGenerator.builder().build().createMixedSchema(2);
            SpecGenerator specGenerator = SpecGenerator.builder()
                    .setOutputStreams(List.of(new StreamInfo("typed-stream", mixedSchema, 0L)))
                    .setInputStreams(List.of(new StreamInfo("inputStream-0", mixedSchema, 1L)))
                    .setOutputStreamSchema(mixedSchema)
                    .setInputStreamSchema(mixedSchema)
                    .build();
            TReqProcessBatch request = fixture.createRequestBuilder()
                    .setComputationId(typedComputationId)
                    .setSpecGenerator(specGenerator)
                    .setPayloadSchema(mixedSchema)
                    .setMessageCount(10)
                    .createProcessBatch();
            CompanionRequestProcessor.ProcessBatchResult result = fixture.processBatch(request);
            // Then: Only half the messages are emitted.
            assertAll(
                    () -> assertEquals(EResponseStatus.RS_OK, result.getStatus()),
                    () -> assertNotNull(result.getData()),
                    () -> assertEquals(5, result.getData().getOutputCount())
            );
        }

        @Entity
        static class Mixed2 {
            long hashColumn;
            String strColumn0;
            long numColumn0;
        }
    }

    @Nested
    @DisplayName("Resource Tracking Tests")
    class ResourceTrackingTests {

        @Test
        @DisplayName("ProcessBatch: tracks resource usage")
        void testProcessBatchResourceStatsTracking() throws Exception {
            // Given: A request with substantial payload.
            TReqProcessBatch request = fixture.createRequestBuilder()
                    .setComputationId(TEST_COMPUTATION_ID)
                    .setMessageCount(100)
                    .setValueStringsSize(500)
                    .createProcessBatch();

            // When: Processing batch.
            CompanionRequestProcessor.ProcessBatchResult result = fixture.processBatch(request);

            // Then: Resource stats are tracked.
            assertAll(
                    () -> assertNotNull(result.getResourceStats()),
                    () -> assertTrue(result.getResourceStats().getAllocatedBytes().toBytes() > 0,
                            "Should allocate memory for processing"),
                    () -> assertTrue(result.getResourceStats().getCpuTime().toNanos() >= 0,
                            "CPU time should be non-negative")
            );
        }

    }

    @Nested
    @DisplayName("CompanionInfo Tests")
    class CompanionInfoTests {

        @Test
        @DisplayName("Basic info: retrieve companion information")
        void testGetCompanionInfoSuccess() {
            // When: Getting companion info.
            CompanionRequestProcessor.CompanionInfoResult result = fixture.getCompanionInfo();

            // Then: Info is returned successfully.
            assertAll(
                    () -> assertEquals(EResponseStatus.RS_OK, result.getStatus()),
                    () -> assertNotNull(result.getPayload()),
                    () -> assertNotNull(result.getPayload().toString())
            );
        }

        @Test
        @DisplayName("With computations: info includes registered computations")
        void testGetCompanionInfoWithRegisteredComputations() {
            // Given: Additional registered computations.
            fixture.registerComputation(passthroughComputation("computation-1"));
            fixture.registerComputation(passthroughComputation("computation-2"));
            fixture.registerComputation(passthroughSourceComputation("source-computation-1"));

            // When: Getting companion info.
            CompanionRequestProcessor.CompanionInfoResult result = fixture.getCompanionInfo();

            // Then: Info includes computation details.
            assertAll(
                    () -> assertEquals(EResponseStatus.RS_OK, result.getStatus()),
                    () -> assertNotNull(result.getPayload()),
                    () -> {
                        String payloadString = result.getPayload().toString();
                        assertTrue(payloadString.contains("computation-1") || payloadString.contains("computations"));
                    }
            );
        }
    }

    @Nested
    @DisplayName("Edge Cases and Error Handling")
    class EdgeCaseTests {

        @Test
        @DisplayName("Constructor: null pipeline context throws")
        void testConstructorNullPipelineContext() {
            // When & Then: Null pipeline context throws.
            assertThrows(
                    NullPointerException.class,
                    () -> new CompanionRequestProcessor(null, fixture.jobContext)
            );
        }

        @Test
        @DisplayName("Constructor: null job context throws")
        void testConstructorNullJobContext() {
            // When & Then: Null job context throws.
            assertThrows(
                    NullPointerException.class,
                    () -> new CompanionRequestProcessor(new PipelineContextSnapshot(fixture.pipelineContext), null)
            );
        }

        @Test
        @DisplayName("Idempotency: consecutive calls produce consistent results")
        void testIdempotency() throws Exception {
            // Given: Same processBatch request processed multiple times.
            TReqProcessBatch batchRequest = fixture.createRequestBuilder()
                    .setComputationId(TEST_COMPUTATION_ID)
                    .setMessageCount(10)
                    .createProcessBatch();

            // When: Processing multiple batches consecutively.
            CompanionRequestProcessor.ProcessBatchResult batchResult1 = fixture.processBatch(batchRequest);
            CompanionRequestProcessor.ProcessBatchResult batchResult2 = fixture.processBatch(batchRequest);

            // Then: All operations are idempotent.
            assertAll(
                    () -> assertEquals(EResponseStatus.RS_OK, batchResult1.getStatus()),
                    () -> assertEquals(EResponseStatus.RS_OK, batchResult2.getStatus()),
                    () -> assertEquals(batchResult1.getData(), batchResult2.getData())
            );
        }

        @Test
        @DisplayName("Edge cases: small strings and result structure")
        void testEdgeCases() throws Exception {
            // Given: A request with very small strings.
            TReqProcessBatch request = fixture.createRequestBuilder()
                    .setComputationId(TEST_COMPUTATION_ID)
                    .setMessageCount(10)
                    .setValueStringsSize(1)
                    .setKeyStringsSize(1)
                    .createProcessBatch();

            // When: Processing batch.
            CompanionRequestProcessor.ProcessBatchResult result = fixture.processBatch(request);

            // Then: Request succeeds and all result components are present.
            assertAll(
                    () -> assertEquals(EResponseStatus.RS_OK, result.getStatus()),
                    () -> assertNotNull(result.getData()),
                    () -> assertNotNull(result.getResourceStats()),
                    () -> assertNotNull(result.getResourceStats().getAllocatedBytes()),
                    () -> assertNotNull(result.getResourceStats().getCpuTime())
            );
        }
    }
}
