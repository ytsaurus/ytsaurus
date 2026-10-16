package tech.ytsaurus.flow.computation;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.internal.request.mapper.ResponseProtoMapper;
import tech.ytsaurus.flow.job.Job;
import tech.ytsaurus.flow.request.RequestContext;
import tech.ytsaurus.flow.request.ResponseContext;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.codec.ByteArrayCodec;
import tech.ytsaurus.flow.row.codec.CodecRegistry;
import tech.ytsaurus.flow.row.codec.InternalStateValueCodec;
import tech.ytsaurus.flow.state.InternalStateDescriptor;
import tech.ytsaurus.flow.state.State;
import tech.ytsaurus.flow.state.StateDescriptors;
import tech.ytsaurus.flow.state.StatesHolder;
import tech.ytsaurus.flow.stream.FlowStreams;
import tech.ytsaurus.flow.stream.StreamIdsMapping;
import tech.ytsaurus.flow.stream.StreamSpecs;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test for internal state tracking: a computation that only mutates the state value in
 * place, without calling {@code set()}, sends the state back in its proto response.
 */
@DisplayName("Internal State Flush Computation Tests")
public class InternalStateFlushComputationTest {

    private static final String COMPUTATION_ID = "internal-state-computation";
    private static final String STREAM_ID = "output-stream";
    private static final String COUNTER_STATE = "word-state";
    private static final long STREAM_SPEC_ID = 0L;

    private static final CounterCodec CODEC = new CounterCodec();
    private static final InternalStateDescriptor<Counter> COUNTER =
            StateDescriptors.custom(COUNTER_STATE, Counter.class, CODEC, Counter::new);

    private TableSchema inputSchema;
    private TableSchema keySchema;
    private StreamSpecs streamSpecs;
    private Job job;
    private Map<String, StatesHolder> states;

    static final class Counter {
        long count;
    }

    static final class CounterCodec implements ByteArrayCodec<Counter> {
        @Override
        public byte[] encode(Counter value) {
            return ByteBuffer.allocate(Long.BYTES).putLong(value.count).array();
        }

        @Override
        public Counter decode(byte[] bytes) {
            var counter = new Counter();
            counter.count = ByteBuffer.wrap(bytes).getLong();
            return counter;
        }
    }

    @BeforeEach
    public void setUp() {
        inputSchema = TableSchema.builder()
                .addValue("word", TiType.string())
                .build();
        keySchema = TableSchema.builder()
                .add(ColumnSchema.builder("hash", ColumnValueType.UINT64, true).build())
                .addValue("word", TiType.string())
                .build();

        states = new HashMap<>();
        states.put(COUNTER_STATE, new StatesHolder(COUNTER_STATE, keySchema));

        var mapping = StreamIdsMapping.builder()
                .addMapping(STREAM_ID, STREAM_SPEC_ID)
                .build();
        streamSpecs = new StreamSpecs(
                mapping,
                List.of(FlowStreams.raw(STREAM_ID, inputSchema)));

        YTreeNode staticSpec = YTree.mapBuilder()
                .key("parameters")
                .beginMap()
                .key("internal_states")
                .beginList()
                .value(COUNTER_STATE)
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

        job = new Job(
                GUID.create(),
                COMPUTATION_ID,
                streamSpecs,
                staticSpec,
                dynamicSpec,
                keySchema
        );
    }

    @Test
    @DisplayName("in-place mutation without set() is reported as a modified state")
    public void mutationWithoutSetIsReported() throws Exception {
        // Given: a function that only mutates the state value in place.
        RowFunction function = (message, output, ctx) -> {
            ctx.getState(COUNTER, message).getOrDefault().count += 1;
        };

        // When: processing two messages of one word and one message of another.
        ResponseContext response = process(function, createMessages("aa", "aa", "ab"));

        // Then: the proto response carries both keys with the accumulated counts.
        var protoStates = new ResponseProtoMapper().toProto(response, streamSpecs).getInternalStatesList();
        assertEquals(1, protoStates.size());
        assertEquals(COUNTER_STATE, protoStates.get(0).getName());
        var counts = counts(protoStates.get(0));
        assertEquals(2, counts.size());
        assertEquals(2L, counts.get(key("aa")));
        assertEquals(1L, counts.get(key("ab")));
    }

    @Test
    @DisplayName("reading the state without mutating it reports nothing")
    public void readingStateReportsNothing() throws Exception {
        seed("aa", 3L);

        RowFunction function = (message, output, ctx) -> {
            var counter = ctx.getState(COUNTER, message).get();
            if (counter != null) {
                assertEquals(3L, counter.count);
            }
        };

        ResponseContext response = process(function, createMessages("aa", "ab"));

        assertTrue(new ResponseProtoMapper().toProto(response, streamSpecs).getInternalStatesList().isEmpty());
    }

    @Test
    @DisplayName("a change made through readOnly() is not reported")
    public void readOnlyChangeIsNotReported() throws Exception {
        seed("aa", 3L);

        RowFunction function = (message, output, ctx) -> {
            ctx.getState(COUNTER, message).readOnly().get().count += 1;
        };

        ResponseContext response = process(function, createMessages("aa"));

        assertTrue(new ResponseProtoMapper().toProto(response, streamSpecs).getInternalStatesList().isEmpty());
    }

    private ResponseContext process(
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

    private void seed(String word, long count) {
        var counter = new Counter();
        counter.count = count;
        states.get(COUNTER_STATE).load(key(word), new State(wireCodec().encode(CODEC.encode(counter))));
    }

    private List<ExtendedMessage> createMessages(String... words) {
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

    private Map<UnversionedRow, Long> counts(tech.ytsaurus.flow.rpc.TState state) {
        var keyCodec = CodecRegistry.getInstance().getKeyCodec();
        Map<UnversionedRow, Long> counts = new HashMap<>();
        for (var item : state.getStateItemsList()) {
            assertFalse(item.getReset());
            counts.put(keyCodec.decode(item.getKey()), CODEC.decode(wireCodec().decode(item.getState())).count);
        }
        return counts;
    }

    private UnversionedRow key(String word) {
        return createMessages(word).get(0).getKey().getRow();
    }

    private static InternalStateValueCodec wireCodec() {
        return CodecRegistry.getInstance().getInternalStateValueCodec();
    }
}
