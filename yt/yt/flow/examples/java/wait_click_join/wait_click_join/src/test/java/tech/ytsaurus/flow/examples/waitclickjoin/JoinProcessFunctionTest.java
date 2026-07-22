package tech.ytsaurus.flow.examples.waitclickjoin;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.examples.waitclickjoin.model.Action;
import tech.ytsaurus.flow.examples.waitclickjoin.model.Hit;
import tech.ytsaurus.flow.examples.waitclickjoin.model.JoinedAction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.row.Timer;
import tech.ytsaurus.flow.stream.FlowStreams;
import tech.ytsaurus.flow.testutils.TestComputationHarness;
import tech.ytsaurus.flow.testutils.TestDoProcessRequest;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JoinProcessFunctionTest {

    // wait_for_actions = "10s" as defined in pipeline.yson
    private static final long WAIT_SECONDS = 10L;

    // Base hit time used across tests
    private static final long BASE_HIT_TIME = 1000L;

    // Watermark set to 0 so that messages with eventTimestamp >= 0 are not dropped as late.
    // The late-drop check in JoinProcessFunction is:
    //   message.getEventTimestamp() < ctx.getEpochInputEventWatermark()
    // With watermark=0, only messages with eventTimestamp < 0 would be dropped.
    private static final long WATERMARK = 0L;

    private TestComputationHarness testHarness;
    private TableSchema joinStateSchema;

    // [BEGIN init]
    @BeforeEach
    void init() throws IOException {
        var pipelineContext = new PipelineContext();

        Computation join = Computation.builder()
                .setComputationId("join")
                .setProcessFunction(new JoinProcessFunction())
                .build();
        pipelineContext.registerComputation(join);

        pipelineContext.registerStream(FlowStreams.typed("hit", Hit.class));
        pipelineContext.registerStream(FlowStreams.typed("action", Action.class));
        pipelineContext.registerStream(FlowStreams.typed("joined_action", JoinedAction.class));

        // External state schema for "/join-state"
        this.joinStateSchema = TableSchema.builder()
                .addValue("hit_payload", ColumnValueType.STRING)
                .addValue("show_time", ColumnValueType.UINT64)
                .addValue("click_time", ColumnValueType.UINT64)
                .build();
        // The pipeline spec is packaged as a classpath resource in the app module's
        // src/main/resources, so the test reads the single source of truth directly.
        try (var specStream = getClass().getResourceAsStream("/pipeline.yson")) {
            YTreeNode spec = YTreeTextSerializer.deserialize(specStream);
            this.testHarness = TestComputationHarness.builder()
                    .setPipelineContext(pipelineContext)
                    .setPipelineSpec(spec)
                    .addExternalStateSchema("/join-state", joinStateSchema)
                    .build();
        }
    }
    // [END init]

    /**
     * Builds a key Payload for the given hitId and hitTime.
     * hash field is set to 0 for simplicity (farm_hash is computed at worker side).
     */
    private Payload buildKey(String hitId, long hitTime) {
        return new PayloadBuilder(testHarness.getGroupBySchema("join"))
                .set("hash", 0L)
                .set("hit_id", hitId)
                .set("hit_time", hitTime)
                .finish();
    }

    /**
     * Builds an ExtendedMessage for the "hit" stream.
     * eventTimestamp is set to hitTime so the message is within the wait window
     * (hitTime < hitTime + WAIT_SECONDS) and not late (hitTime >= WATERMARK=0).
     */
    private ExtendedMessage buildHitMessage(
            String hitId,
            long hitTime,
            String hitPayload
    ) {
        return ExtendedMessage.builder()
                .setStreamId("hit")
                .setKey(buildKey(hitId, hitTime))
                .setPayload(new Hit(hitId, hitTime, hitPayload))
                .setEventTimestamp(hitTime)
                .build();
    }

    /**
     * Builds an ExtendedMessage for the "action" stream with explicit eventTimestamp.
     */
    private ExtendedMessage buildActionMessage(
            String hitId,
            long hitTime,
            long actionTime,
            boolean isClick,
            long eventTimestamp
    ) {
        return ExtendedMessage.builder()
                .setStreamId("action")
                .setKey(buildKey(hitId, hitTime))
                .setPayload(new Action(hitId, hitTime, actionTime, isClick))
                .setEventTimestamp(eventTimestamp)
                .build();

    }

    /**
     * Builds a Timer for the "timer" stream.
     * triggerTimestamp = hitTime + WAIT_SECONDS (maxTime).
     * eventTimestamp is set to triggerTimestamp so the timer is not considered late.
     */
    private Timer buildTimer(String hitId, long hitTime) {
        long maxTime = hitTime + WAIT_SECONDS;
        return new Timer(
                null,
                maxTime,
                0L,
                "timer",
                0L,
                maxTime,
                buildKey(hitId, hitTime)
        );
    }

    /**
     * Returns a watermarks map with both input streams set to WATERMARK=0.
     * This ensures messages with eventTimestamp >= 0 are not dropped as late.
     */
    private Map<String, Long> defaultWatermarks() {
        return Map.of(
                "hit", WATERMARK,
                "action", WATERMARK
        );
    }

    // -------------------------------------------------------------------------
    // onMessage tests
    // -------------------------------------------------------------------------

    // [BEGIN test_hit]
    @Test
    void testHitMessageStoresHitPayloadInState() {
        String hitId = "hit-1";
        long hitTime = BASE_HIT_TIME;

        var messages = List.of(buildHitMessage(hitId, hitTime, "payload-data"));

        var request = TestDoProcessRequest.builder("join")
                .setMessages(messages)
                .setWatermarks(defaultWatermarks())
                .build();

        var response = testHarness.doProcess(request);

        // No output messages yet — timer is scheduled, output comes on timer fire
        assertTrue(response.getOutputMessagesFlatten().isEmpty());

        // A timer must be scheduled
        assertEquals(1, response.getOutputTimersFlatten().size());
        var timer = response.getOutputTimersFlatten().getFirst();
        assertEquals(hitTime + WAIT_SECONDS, timer.getTriggerTimestamp());

        // External state must be updated with hit_payload
        assertEquals(1, response.allStates().externalSize("/join-state"));
        Payload key = buildKey(hitId, hitTime);
        var state = response.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key).get().orElseThrow();
        assertEquals("payload-data", state.get("hit_payload", String.class));
    }
    // [END test_hit]

    @Test
    void testShowActionMessageStoresShowTimeInState() {
        String hitId = "hit-2";
        long hitTime = BASE_HIT_TIME;
        long actionTime = hitTime + 5L;
        // eventTimestamp must be < hitTime + WAIT_SECONDS and >= WATERMARK
        long eventTimestamp = hitTime + 1L;

        var messages = List.of(buildActionMessage(hitId, hitTime, actionTime, false, eventTimestamp));

        var request = TestDoProcessRequest.builder("join")
                .setMessages(messages)
                .setWatermarks(defaultWatermarks())
                .build();

        var response = testHarness.doProcess(request);

        assertTrue(response.getOutputMessagesFlatten().isEmpty());
        assertEquals(1, response.getOutputTimersFlatten().size());

        Payload key = buildKey(hitId, hitTime);
        var state = response.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key).get().orElseThrow();
        assertEquals(actionTime, state.get("show_time", Long.class));
    }

    @Test
    void testClickActionMessageStoresClickTimeInState() {
        String hitId = "hit-3";
        long hitTime = BASE_HIT_TIME;
        long actionTime = hitTime + 5L;
        long eventTimestamp = hitTime + 1L;

        var messages = List.of(buildActionMessage(hitId, hitTime, actionTime, true, eventTimestamp));

        var request = TestDoProcessRequest.builder("join")
                .setMessages(messages)
                .setWatermarks(defaultWatermarks())
                .build();

        var response = testHarness.doProcess(request);

        assertTrue(response.getOutputMessagesFlatten().isEmpty());
        assertEquals(1, response.getOutputTimersFlatten().size());

        Payload key = buildKey(hitId, hitTime);
        var state = response.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key).get().orElseThrow();
        assertEquals(actionTime, state.get("click_time", Long.class));
    }

    @Test
    void testMessageOutOfWaitRangeIsDropped() {
        String hitId = "hit-4";
        long hitTime = BASE_HIT_TIME;
        // eventTimestamp >= hitTime + WAIT_SECONDS → message is out of wait range
        long eventTimestamp = hitTime + WAIT_SECONDS;

        var messages = List.of(buildActionMessage(hitId, hitTime, hitTime + 5L, false, eventTimestamp));

        var request = TestDoProcessRequest.builder("join")
                .setMessages(messages)
                .setWatermarks(defaultWatermarks())
                .build();

        var response = testHarness.doProcess(request);

        // Message is dropped — no output, no timer, no state update
        assertTrue(response.getOutputMessagesFlatten().isEmpty());
        assertTrue(response.getOutputTimersFlatten().isEmpty());
        assertEquals(0, response.allStates().externalSize("/join-state"));
    }

    @Test
    void testLateMessageIsDropped() {
        String hitId = "hit-5";
        long hitTime = BASE_HIT_TIME;
        // Set watermark to hitTime + 5, so messages with eventTimestamp < hitTime + 5 are late
        long watermark = hitTime + 5L;
        // eventTimestamp < watermark → late message
        long eventTimestamp = hitTime + 3L;

        var messages = List.of(buildActionMessage(hitId, hitTime, hitTime + 2L, false, eventTimestamp));

        var request = TestDoProcessRequest.builder("join")
                .setMessages(messages)
                .setWatermarks(Map.of("hit", watermark, "action", watermark))
                .build();

        var response = testHarness.doProcess(request);

        // Late message is dropped — no output, no timer, no state update
        assertTrue(response.getOutputMessagesFlatten().isEmpty());
        assertTrue(response.getOutputTimersFlatten().isEmpty());
        assertEquals(0, response.allStates().externalSize("/join-state"));
    }

    // -------------------------------------------------------------------------
    // onTimer tests
    // -------------------------------------------------------------------------

    @Test
    void testTimerEmitsJoinedActionWithShowAndClick() {
        String hitId = "hit-10";
        long hitTime = BASE_HIT_TIME;
        long showTime = hitTime + 3L;
        long clickTime = hitTime + 7L;

        Payload key = buildKey(hitId, hitTime);

        // Pre-populate state: hit_payload + show_time + click_time
        Payload preState = new PayloadBuilder(joinStateSchema)
                .set("hit_payload", "some-payload")
                .set("show_time", showTime)
                .set("click_time", clickTime)
                .finish();

        Timer timer = buildTimer(hitId, hitTime);

        var request = TestDoProcessRequest.builder("join")
                .setTimers(List.of(timer))
                .setState(JoinProcessFunction.JOINED_ACTION_STATE, key, preState)
                .build();

        var response = testHarness.doProcess(request);

        // One JoinedAction must be emitted
        assertEquals(1, response.getOutputMessagesFlatten().size());
        var msg = response.getOutputMessagesFlatten().getFirst();
        assertEquals("joined_action", msg.getStreamId());

        JoinedAction joinedAction = msg.getPayload();
        assertEquals(hitId, joinedAction.getHitId());
        assertEquals(hitTime, joinedAction.getHitTime());
        assertEquals("some-payload", joinedAction.getHitPayload());
        assertEquals(showTime, joinedAction.getShowTime());
        assertTrue(joinedAction.getClick());
        assertEquals(clickTime, joinedAction.getClickTime());

        // State must be cleared after timer fires
        assertTrue(response.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key).get().isEmpty());
    }

    @Test
    void testTimerEmitsJoinedActionWithShowButNoClick() {
        String hitId = "hit-11";
        long hitTime = BASE_HIT_TIME;
        long showTime = hitTime + 5L;

        Payload key = buildKey(hitId, hitTime);

        Payload preState = new PayloadBuilder(joinStateSchema)
                .set("hit_payload", "payload-11")
                .set("show_time", showTime)
                .set("click_time", 0L)
                .finish();

        Timer timer = buildTimer(hitId, hitTime);

        var request = TestDoProcessRequest.builder("join")
                .setTimers(List.of(timer))
                .setState(JoinProcessFunction.JOINED_ACTION_STATE, key, preState)
                .build();

        var response = testHarness.doProcess(request);

        assertEquals(1, response.getOutputMessagesFlatten().size());
        JoinedAction joinedAction = response.getOutputMessagesFlatten().getFirst().getPayload();
        assertEquals(hitId, joinedAction.getHitId());
        assertEquals(hitTime, joinedAction.getHitTime());
        assertEquals("payload-11", joinedAction.getHitPayload());
        assertEquals(showTime, joinedAction.getShowTime());
        assertFalse(joinedAction.getClick());
        assertEquals(0L, joinedAction.getClickTime());

        assertTrue(response.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key).get().isEmpty());
    }

    @Test
    void testTimerDoesNotEmitWhenShowTimeIsZero() {
        String hitId = "hit-12";
        long hitTime = BASE_HIT_TIME;

        Payload key = buildKey(hitId, hitTime);

        // State has hit_payload but show_time == 0 (no show event received)
        Payload preState = new PayloadBuilder(joinStateSchema)
                .set("hit_payload", "payload-12")
                .set("show_time", 0L)
                .set("click_time", 0L)
                .finish();

        Timer timer = buildTimer(hitId, hitTime);

        var request = TestDoProcessRequest.builder("join")
                .setTimers(List.of(timer))
                .setState(JoinProcessFunction.JOINED_ACTION_STATE, key, preState)
                .build();

        var response = testHarness.doProcess(request);

        // No output: show_time == 0 means no show event was received
        assertTrue(response.getOutputMessagesFlatten().isEmpty());

        // State is still cleared
        assertTrue(response.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key).get().isEmpty());
    }

    @Test
    void testTimerDoesNotEmitWhenHitPayloadIsAbsent() {
        String hitId = "hit-13";
        long hitTime = BASE_HIT_TIME;
        long showTime = hitTime + 5L;

        Payload key = buildKey(hitId, hitTime);

        // State has show_time but no hit_payload (null)
        Payload preState = new PayloadBuilder(joinStateSchema)
                .set("show_time", showTime)
                .set("click_time", 0L)
                .finish();

        Timer timer = buildTimer(hitId, hitTime);

        var request = TestDoProcessRequest.builder("join")
                .setTimers(List.of(timer))
                .setState(JoinProcessFunction.JOINED_ACTION_STATE, key, preState)
                .build();

        var response = testHarness.doProcess(request);

        // No output: hit_payload is null
        assertTrue(response.getOutputMessagesFlatten().isEmpty());

        assertTrue(response.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key).get().isEmpty());
    }

    // -------------------------------------------------------------------------
    // Full flow integration test
    // -------------------------------------------------------------------------

    // [BEGIN test_full_flow]
    @Test
    void testFullJoinFlow_HitThenShowThenClickThenTimer() {
        String hitId = "hit-20";
        long hitTime = BASE_HIT_TIME;
        long showTime = hitTime + 3L;
        long clickTime = hitTime + 7L;

        // Step 1: process hit message
        var hitMessages = List.of(buildHitMessage(hitId, hitTime, "full-payload"));
        var hitRequest = TestDoProcessRequest.builder("join")
                .setMessages(hitMessages)
                .setWatermarks(defaultWatermarks())
                .build();
        var hitResponse = testHarness.doProcess(hitRequest);

        assertTrue(hitResponse.getOutputMessagesFlatten().isEmpty());
        assertEquals(1, hitResponse.getOutputTimersFlatten().size());
        assertEquals(1, hitResponse.allStates().externalSize("/join-state"));

        Payload key = buildKey(hitId, hitTime);
        Payload stateAfterHit = hitResponse.allStates()
                .get(JoinProcessFunction.JOINED_ACTION_STATE, key).get().orElseThrow();
        assertEquals("full-payload", stateAfterHit.get("hit_payload", String.class));

        // Step 2: process show action message, passing the state from step 1
        var showMessages = List.of(buildActionMessage(hitId, hitTime, showTime, false, hitTime + 1L));
        var showRequest = TestDoProcessRequest.builder("join")
                .setMessages(showMessages)
                .setState(JoinProcessFunction.JOINED_ACTION_STATE, key, stateAfterHit)
                .setWatermarks(defaultWatermarks())
                .build();
        var showResponse = testHarness.doProcess(showRequest);

        assertTrue(showResponse.getOutputMessagesFlatten().isEmpty());
        Payload stateAfterShow = showResponse.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key)
                .get().orElseThrow();
        assertEquals("full-payload", stateAfterShow.get("hit_payload", String.class));
        assertEquals(showTime, stateAfterShow.get("show_time", Long.class));

        // Step 3: process click action message, passing the state from step 2
        var clickMessages = List.of(buildActionMessage(hitId, hitTime, clickTime, true, hitTime + 2L));
        var clickRequest = TestDoProcessRequest.builder("join")
                .setMessages(clickMessages)
                .setState(JoinProcessFunction.JOINED_ACTION_STATE, key, stateAfterShow)
                .setWatermarks(defaultWatermarks())
                .build();
        var clickResponse = testHarness.doProcess(clickRequest);

        assertTrue(clickResponse.getOutputMessagesFlatten().isEmpty());
        Payload stateAfterClick = clickResponse.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key)
                .get().orElseThrow();
        assertEquals(clickTime, stateAfterClick.get("click_time", Long.class));

        // Step 4: fire the timer with the accumulated state
        Timer timer = buildTimer(hitId, hitTime);
        var timerRequest = TestDoProcessRequest.builder("join")
                .setTimers(List.of(timer))
                .setState(JoinProcessFunction.JOINED_ACTION_STATE, key, stateAfterClick)
                .build();
        var timerResponse = testHarness.doProcess(timerRequest);

        assertEquals(1, timerResponse.getOutputMessagesFlatten().size());
        JoinedAction joinedAction = timerResponse.getOutputMessagesFlatten().getFirst().getPayload();
        assertEquals(hitId, joinedAction.getHitId());
        assertEquals(hitTime, joinedAction.getHitTime());
        assertEquals("full-payload", joinedAction.getHitPayload());
        assertEquals(showTime, joinedAction.getShowTime());
        assertTrue(joinedAction.getClick());
        assertEquals(clickTime, joinedAction.getClickTime());

        assertTrue(timerResponse.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key).get().isEmpty());
    }
    // [END test_full_flow]

    @Test
    void testMultipleHitsAreProcessedIndependently() {
        String hitId1 = "hit-30";
        String hitId2 = "hit-31";
        long hitTime1 = BASE_HIT_TIME;
        long hitTime2 = BASE_HIT_TIME + 100L;

        var messages = List.of(
                buildHitMessage(hitId1, hitTime1, "payload-30"),
                buildHitMessage(hitId2, hitTime2, "payload-31")
        );

        var request = TestDoProcessRequest.builder("join")
                .setMessages(messages)
                .setWatermarks(defaultWatermarks())
                .build();

        var response = testHarness.doProcess(request);

        assertTrue(response.getOutputMessagesFlatten().isEmpty());
        assertEquals(2, response.getOutputTimersFlatten().size());
        assertEquals(2, response.allStates().externalSize("/join-state"));

        Payload key1 = buildKey(hitId1, hitTime1);
        Payload key2 = buildKey(hitId2, hitTime2);

        Payload state1 = response.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key1).get().orElseThrow();
        assertEquals("payload-30", state1.get("hit_payload", String.class));

        Payload state2 = response.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key2).get().orElseThrow();
        assertEquals("payload-31", state2.get("hit_payload", String.class));
    }
}
