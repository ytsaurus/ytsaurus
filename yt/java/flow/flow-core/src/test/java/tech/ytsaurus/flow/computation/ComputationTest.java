package tech.ytsaurus.flow.computation;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.persistence.Entity;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.function.BatchFunction;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.job.Job;
import tech.ytsaurus.flow.request.RequestContext;
import tech.ytsaurus.flow.request.ResponseContext;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Message;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.codec.ByteArrayCodec;
import tech.ytsaurus.flow.state.InternalState;
import tech.ytsaurus.flow.state.StateDescriptors;
import tech.ytsaurus.flow.state.StatesHolder;
import tech.ytsaurus.flow.stream.FlowStreams;
import tech.ytsaurus.flow.stream.StreamIdsMapping;
import tech.ytsaurus.flow.stream.StreamSpecs;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive tests for {@link Computation} covering different process function types:
 * - One-to-one mapping (RowFunction)
 * - One-to-many (RowFunction with multiple outputs)
 * - Many-to-one (BatchFunction with aggregation)
 * - Filtering (RowFunction with conditional output)
 */
@DisplayName("Computation Tests")
public class ComputationTest {

    private static final String COMPUTATION_ID = "test-computation";
    private static final String STREAM_ID = "output-stream";
    private static final String TYPED_STREAM_ID = "typed-output-stream";
    private static final long STREAM_SPEC_ID = 0L;
    private static final long TYPED_STREAM_SPEC_ID = 1L;

    private TestFixture fixture;

    @BeforeEach
    public void setUp() {
        fixture = new TestFixture();
    }

    /**
     * Test fixture providing common setup and helper methods.
     */
    private static class TestFixture {
        private final TableSchema inputSchema;
        private final TableSchema outputSchema;
        private final TableSchema keySchema;
        private final StreamSpecs streamSpecs;
        private final Job job;
        private final Map<String, StatesHolder<InternalState>> states;

        TestFixture() {
            this.inputSchema = TableSchema.builder()
                    .addValue("word", TiType.string())
                    .build();

            this.outputSchema = TableSchema.builder()
                    .addValue("word", TiType.string())
                    .addValue("length", TiType.int64())
                    .build();

            this.keySchema = TableSchema.builder()
                    .add(ColumnSchema.builder("hash", ColumnValueType.UINT64, true).build())
                    .addValue("word", TiType.string())
                    .build();

            this.states = new HashMap<>();
            states.put("word-state", new StatesHolder<>("word-state", keySchema, null));

            var stream = FlowStreams.raw(STREAM_ID, outputSchema);
            var typedStream = FlowStreams.typed(TYPED_STREAM_ID, WordLength.class);
            var mapping = StreamIdsMapping.builder()
                    .addMapping(STREAM_ID, STREAM_SPEC_ID)
                    .addMapping(TYPED_STREAM_ID, TYPED_STREAM_SPEC_ID)
                    .build();
            this.streamSpecs = new StreamSpecs(mapping, List.of(stream, typedStream));

            GUID jobId = GUID.create();
            YTreeNode staticSpec = YTree.mapBuilder()
                    .key("parameters")
                    .beginMap()
                    .key("internal_states")
                    .beginList()
                    .value("word-state")
                    .endList()
                    .endMap()
                    .endMap()
                    .build();

            YTreeNode dynamicSpec = YTree.mapBuilder()
                    .key("parameters")
                    .beginMap()
                    .endMap()
                    .endMap()
                    .build();

            this.job = new Job(
                    jobId,
                    COMPUTATION_ID,
                    streamSpecs,
                    staticSpec,
                    dynamicSpec,
                    inputSchema
            );
        }

        List<ExtendedMessage> createMessages(String... words) {
            List<ExtendedMessage> messages = new ArrayList<>();
            for (String word : words) {
                Payload key = new Payload(
                        new UnversionedRow(List.of(
                                new UnversionedValue(0, ColumnValueType.UINT64, false, 1L),
                                new UnversionedValue(1, ColumnValueType.STRING, false, word.getBytes())
                        )),
                        keySchema
                );

                Payload payload = new Payload(
                        new UnversionedRow(List.of(
                                new UnversionedValue(0, ColumnValueType.STRING, false, word.getBytes())
                        )),
                        inputSchema
                );

                messages.add(
                        ExtendedMessage.builder()
                                .setMessageId(GUID.create().toString())
                                .setStreamId(STREAM_ID)
                                .setPayload(payload)
                                .setKey(key)
                                .build()
                );
            }
            return messages;
        }

        ResponseContext processWithRowFunction(
                RowFunction function,
                List<ExtendedMessage> messages
        ) throws Exception {
            var computation = Computation.builder()
                    .setComputationId(COMPUTATION_ID)
                    .setProcessFunction(function)
                    .build();

            var request = RequestContext.builder()
                    .setComputationId(COMPUTATION_ID)
                    .setRequestId(GUID.create())
                    .setJobId(job.getJobId())
                    .setJob(job)
                    .setMessages(messages)
                    .setTimers(new ArrayList<>())
                    .setInternalStates(states)
                    .setStreamSpecsOverride(streamSpecs)
                    .build();

            return computation.doProcess(request);
        }

        ResponseContext processWithBatchFunction(
                BatchFunction function,
                List<ExtendedMessage> messages
        ) throws Exception {
            var computation = Computation.builder()
                    .setComputationId(COMPUTATION_ID)
                    .setProcessFunction(function)
                    .build();

            var request = RequestContext.builder()
                    .setComputationId(COMPUTATION_ID)
                    .setRequestId(GUID.create())
                    .setJobId(job.getJobId())
                    .setJob(job)
                    .setMessages(messages)
                    .setTimers(new ArrayList<>())
                    .setInternalStates(states)
                    .setStreamSpecsOverride(streamSpecs)
                    .build();

            return computation.doProcess(request);
        }
    }

    private static class WordCount {
        private String word;
        private long count;
    }

    private static class WordCountByteArrayCodec implements ByteArrayCodec<WordCount> {
        @Override
        public byte[] encode(WordCount value) {
            byte[] wordBytes = value.word.getBytes(StandardCharsets.UTF_8);
            ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + wordBytes.length + Long.BYTES);
            buf.putInt(wordBytes.length);
            buf.put(wordBytes);
            buf.putLong(value.count);
            return buf.array();
        }

        @Override
        public WordCount decode(byte[] value) {
            ByteBuffer buf = ByteBuffer.wrap(value);
            int wordLen = buf.getInt();
            byte[] wordBytes = new byte[wordLen];
            buf.get(wordBytes);
            WordCount wc = new WordCount();
            wc.word = new String(wordBytes, StandardCharsets.UTF_8);
            wc.count = buf.getLong();
            return wc;
        }
    }

    @Entity
    private static class WordLength {
        private String word;
        private long length;
    }


    @Nested
    @DisplayName("RowFunction Tests")
    class RowFunctionTests {

        @Test
        @DisplayName("One-to-one mapping: transform each message")
        public void testOneToOneMapping() throws Exception {
            // Given: A function that transforms each word to its length
            RowFunction function = (message, output, ctx) -> {
                String word = message.get("word", String.class);
                var builder = ctx.createMessageBuilder(STREAM_ID);
                builder.set("word", word.getBytes())
                        .set("length", (long) word.length());
                output.addMessage(builder.finish());
            };

            List<ExtendedMessage> messages = fixture.createMessages("hello", "world", "test");

            // When: Processing messages
            ResponseContext response = fixture.processWithRowFunction(function, messages);

            // Then: Each input produces exactly one output
            assertEquals(3, response.getTransformResults().size());
            for (var result : response.getTransformResults()) {
                assertEquals(1, result.getMessages().size());
                assertEquals(1, result.getParentIds().size());
            }
        }

        @Test
        @DisplayName("One-to-one mapping: transform each message with typed output")
        public void testOneToOneMappingTypedOutput() throws Exception {
            // Given: A function that transforms each word to its length
            RowFunction function = (message, output, ctx) -> {
                String word = message.get("word", String.class);
                var wordLength = new WordLength();
                wordLength.word = word;
                wordLength.length = word.length();
                var outputMessage = Message.builder().setStreamId(TYPED_STREAM_ID).setPayload(wordLength).build();
                output.addMessage(outputMessage);
            };

            List<ExtendedMessage> messages = fixture.createMessages("hello", "world", "test");

            // When: Processing messages
            ResponseContext response = fixture.processWithRowFunction(function, messages);

            // Then: Each input produces exactly one output
            assertEquals(3, response.getTransformResults().size());
            for (var result : response.getTransformResults()) {
                assertEquals(1, result.getMessages().size());
                assertEquals(1, result.getParentIds().size());
                assertInstanceOf(WordLength.class, result.getMessages().getFirst().getPayload());
            }
        }

        @Test
        @DisplayName("One-to-many: emit multiple messages per input")
        public void testOneToManyMapping() throws Exception {
            // Given: A function that splits each word into characters
            RowFunction function = (message, output, ctx) -> {
                String word = message.get("word", String.class);
                for (char c : word.toCharArray()) {
                    var builder = ctx.createMessageBuilder(STREAM_ID);
                    builder.set("word", String.valueOf(c).getBytes())
                            .set("length", 1L);
                    output.addMessage(builder.finish());
                }
            };

            List<ExtendedMessage> messages = fixture.createMessages("ab", "cd");

            // When: Processing messages
            ResponseContext response = fixture.processWithRowFunction(function, messages);

            // Then: Each input produces multiple outputs
            assertEquals(2, response.getTransformResults().size());
            int totalOutputs = response.getTransformResults().stream()
                    .mapToInt(r -> r.getMessages().size())
                    .sum();
            assertEquals(4, totalOutputs); // "ab" -> 2 chars, "cd" -> 2 chars
        }

        @Test
        @DisplayName("Filtering: emit only messages matching condition")
        public void testFiltering() throws Exception {
            // Given: A function that filters words longer than 2 characters
            RowFunction function = (message, output, ctx) -> {
                String word = message.get("word", String.class);
                if (word.length() > 2) {
                    var builder = ctx.createMessageBuilder(STREAM_ID);
                    builder.set("word", word.getBytes())
                            .set("length", (long) word.length());
                    output.addMessage(builder.finish());
                }
            };

            List<ExtendedMessage> messages = fixture.createMessages("a", "ab", "abc", "abcd");

            // When: Processing messages
            ResponseContext response = fixture.processWithRowFunction(function, messages);

            // Then: Only 2 messages pass the filter (length > 2)
            assertEquals(2, response.getTransformResults().size());
            response.getTransformResults().forEach(transformResult -> {
                assertEquals(1, transformResult.getMessages().size());
            });
        }

        @Test
        @DisplayName("State management: count word occurrences")
        public void testStateManagement() throws Exception {
            // Given: A function that counts word occurrences using state
            RowFunction function = (message, output, ctx) -> {
                String word = message.get("word", String.class);

                // Update state
                var state = ctx.getState(
                        StateDescriptors.customWithoutDefault("word-state", WordCount.class,
                                new WordCountByteArrayCodec()),
                        message
                );
                long newCount = state.get()
                        .map(wc -> wc.count + 1)
                        .orElse(1L);

                var stateEntry = new WordCount();
                stateEntry.word = word;
                stateEntry.count = newCount;
                state.set(stateEntry);

                // Output message with count
                var builder = ctx.createMessageBuilder(STREAM_ID);
                builder.set("word", word.getBytes())
                        .set("length", newCount);
                output.addMessage(builder.finish());
            };

            List<ExtendedMessage> messages = fixture.createMessages("aa", "aa", "ab", "aa");

            // When: Processing messages
            ResponseContext response = fixture.processWithRowFunction(function, messages);

            // Then: State should track word counts
            assertEquals(1, response.getInternalStates().size());
            assertNotNull(response.getInternalStates().get("word-state"));
            assertEquals(2, response.getInternalStates().get("word-state").getStates().size()); // "aa" and "ab"
        }
    }

    @Nested
    @DisplayName("BatchFunction Tests")
    class BatchFunctionTests {

        @Test
        @DisplayName("Many-to-one: aggregate batch into single output")
        public void testManyToOneAggregation() throws Exception {
            // Given: A function that counts total messages in batch.
            BatchFunction function = (messages, output, ctx) -> {
                var builder = ctx.createMessageBuilder(STREAM_ID);
                builder.set("word", "batch".getBytes())
                        .set("length", (long) messages.size());
                output.addMessage(builder.finish());
            };

            List<ExtendedMessage> messages = fixture.createMessages("a", "b", "c", "d", "e");

            // When: Processing batch
            ResponseContext response = fixture.processWithBatchFunction(function, messages);

            // Then: Single output for entire batch
            assertEquals(1, response.getTransformResults().size());
            var result = response.getTransformResults().get(0);
            assertEquals(1, result.getMessages().size());
            assertEquals(5, result.getParentIds().size());
        }

        @Test
        @DisplayName("Batch processing: transform all messages")
        public void testBatchTransformation() throws Exception {
            // Given: A function that transforms each message in batch
            BatchFunction function = (messages, output, ctx) -> {
                for (ExtendedMessage message : messages) {
                    String word = message.get("word", String.class);
                    var builder = ctx.createMessageBuilder(STREAM_ID);
                    builder.set("word", word.getBytes())
                            .set("length", (long) word.length());
                    output.addMessage(builder.finish());
                }
            };

            List<ExtendedMessage> messages = fixture.createMessages("hello", "world");

            // When: Processing batch
            ResponseContext response = fixture.processWithBatchFunction(function, messages);

            // Then: All messages transformed
            assertEquals(1, response.getTransformResults().size());
            var result = response.getTransformResults().get(0);
            assertEquals(2, result.getMessages().size());
            assertEquals(2, result.getParentIds().size());
        }

        @Test
        @DisplayName("Batch filtering: emit only matching messages")
        public void testBatchFiltering() throws Exception {
            // Given: A function that filters batch by condition.
            BatchFunction function = (messages, output, ctx) -> {
                for (ExtendedMessage message : messages) {
                    String word = message.get("word", String.class);
                    if (word.startsWith("a")) {
                        var builder = ctx.createMessageBuilder(STREAM_ID);
                        builder.set("word", word.getBytes())
                                .set("length", (long) word.length());
                        output.addMessage(builder.finish());
                    }
                }
            };

            List<ExtendedMessage> messages = fixture.createMessages("apple", "banana", "avocado", "cherry");

            // When: Processing batch
            ResponseContext response = fixture.processWithBatchFunction(function, messages);

            // Then: Only messages starting with 'a' are emitted
            assertEquals(1, response.getTransformResults().size());
            var result = response.getTransformResults().get(0);
            assertEquals(2, result.getMessages().size()); // "apple" and "avocado"
        }

        @Test
        @DisplayName("Empty batch: no output produced")
        public void testEmptyBatch() throws Exception {
            // Given: A function that processes messages
            BatchFunction function = (messages, output, ctx) -> {
                for (ExtendedMessage message : messages) {
                    String word = message.get("word", String.class);
                    var builder = ctx.createMessageBuilder(STREAM_ID);
                    builder.set("word", word.getBytes())
                            .set("length", (long) word.length());
                    output.addMessage(builder.finish());
                }
            };

            List<ExtendedMessage> messages = new ArrayList<>();

            // When: Processing empty batch
            ResponseContext response = fixture.processWithBatchFunction(function, messages);

            // Then: No transform results
            assertTrue(response.getTransformResults().isEmpty());
        }
    }

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCaseTests {

        @Test
        @DisplayName("No output: function produces no messages")
        public void testNoOutput() throws Exception {
            // Given: A function that never outputs
            RowFunction function = (message, output, ctx) -> {
                // Intentionally produce no output
            };

            List<ExtendedMessage> messages = fixture.createMessages("test");

            // When: Processing messages
            ResponseContext response = fixture.processWithRowFunction(function, messages);

            // Then: Transform results exist but contain no messages
            assertTrue(response.getTransformResults().isEmpty());
        }

        @Test
        @DisplayName("Multiple outputs per message with different parent tracking")
        public void testMultipleOutputsParentTracking() throws Exception {
            // Given: A function that produces multiple outputs
            AtomicInteger counter = new AtomicInteger(0);
            RowFunction function = (message, output, ctx) -> {
                String word = message.get("word", String.class);
                for (int i = 0; i < 3; i++) {
                    var builder = ctx.createMessageBuilder(STREAM_ID);
                    builder.set("word", (word + "_" + i).getBytes())
                            .set("length", (long) word.length());
                    output.addMessage(builder.finish());
                    counter.incrementAndGet();
                }
            };

            List<ExtendedMessage> messages = fixture.createMessages("test");

            // When: Processing messages
            ResponseContext response = fixture.processWithRowFunction(function, messages);

            // Then: Parent IDs are correctly tracked
            assertEquals(1, response.getTransformResults().size());
            var result = response.getTransformResults().get(0);
            assertEquals(3, result.getMessages().size());
            assertEquals(1, result.getParentIds().size());
            assertEquals(3, counter.get());
        }
    }
}
