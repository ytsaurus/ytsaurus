package tech.ytsaurus.flow.examples.waitclickjoin

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import tech.ytsaurus.core.tables.ColumnValueType
import tech.ytsaurus.core.tables.TableSchema
import tech.ytsaurus.flow.computation.Computation
import tech.ytsaurus.flow.context.PipelineContext
import tech.ytsaurus.flow.examples.waitclickjoin.model.Action
import tech.ytsaurus.flow.examples.waitclickjoin.model.Hit
import tech.ytsaurus.flow.examples.waitclickjoin.model.JoinedAction
import tech.ytsaurus.flow.row.ExtendedMessage
import tech.ytsaurus.flow.row.Payload
import tech.ytsaurus.flow.row.PayloadBuilder
import tech.ytsaurus.flow.row.Timer
import tech.ytsaurus.flow.stream.FlowStreams
import tech.ytsaurus.flow.testutils.TestComputationHarness
import tech.ytsaurus.flow.testutils.TestDoProcessRequest
import tech.ytsaurus.ysontree.YTreeTextSerializer

class JoinProcessFunctionTest {

    // wait_for_actions = "10s" as defined in pipeline.yson
    companion object {
        private const val WAIT_SECONDS = 10L

        // Base hit time used across tests
        private const val BASE_HIT_TIME = 1000L

        // Watermark set to 0 so that messages with eventTimestamp >= 0 are not dropped as late.
        // The late-drop check in JoinProcessFunction is:
        //   message.getEventTimestamp() < ctx.getEpochInputEventWatermark()
        // With watermark=0, only messages with eventTimestamp < 0 would be dropped.
        private const val WATERMARK = 0L
    }

    private lateinit var testHarness: TestComputationHarness
    private lateinit var joinStateSchema: TableSchema

    // [BEGIN init]
    @BeforeEach
    fun init() {
        val pipelineContext = PipelineContext()

        val join = Computation.builder()
            .setComputationId("join")
            .setProcessFunction(JoinProcessFunction())
            .build()
        pipelineContext.registerComputation(join)

        pipelineContext.registerStream(FlowStreams.typed("hit", Hit::class.java))
        pipelineContext.registerStream(FlowStreams.typed("action", Action::class.java))
        pipelineContext.registerStream(FlowStreams.typed("joined_action", JoinedAction::class.java))

        // External state schema for "/join-state"
        this.joinStateSchema = TableSchema.builder()
            .addValue("hit_payload", ColumnValueType.STRING)
            .addValue("show_time", ColumnValueType.UINT64)
            .addValue("click_time", ColumnValueType.UINT64)
            .build()
        // The pipeline spec is packaged as a classpath resource in the app module's
        // src/main/resources, so the test reads the single source of truth directly.
        javaClass.getResourceAsStream("/pipeline.yson")!!.use { specStream ->
            val spec = YTreeTextSerializer.deserialize(specStream)
            this.testHarness = TestComputationHarness.builder()
                .setPipelineContext(pipelineContext)
                .setPipelineSpec(spec)
                .addExternalStateSchema("/join-state", joinStateSchema)
                .build()
        }
    }
    // [END init]

    /**
     * Builds a key Payload for the given hitId and hitTime.
     * hash field is set to 0 for simplicity (farm_hash is computed at worker side).
     */
    private fun buildKey(hitId: String, hitTime: Long): Payload {
        return PayloadBuilder(testHarness.getGroupBySchema("join")!!)
            .set("hash", 0L)
            .set("hit_id", hitId)
            .set("hit_time", hitTime)
            .finish()
    }

    /**
     * Builds an ExtendedMessage for the "hit" stream.
     * eventTimestamp is set to hitTime so the message is within the wait window
     * (hitTime < hitTime + WAIT_SECONDS) and not late (hitTime >= WATERMARK=0).
     */
    private fun buildHitMessage(
        hitId: String,
        hitTime: Long,
        hitPayload: String
    ): ExtendedMessage {
        return ExtendedMessage.builder()
            .setStreamId("hit")
            .setKey(buildKey(hitId, hitTime))
            .setPayload(Hit(hitId, hitTime, hitPayload))
            .setEventTimestamp(hitTime)
            .build()
    }

    /**
     * Builds an ExtendedMessage for the "action" stream with explicit eventTimestamp.
     */
    private fun buildActionMessage(
        hitId: String,
        hitTime: Long,
        actionTime: Long,
        isClick: Boolean,
        eventTimestamp: Long
    ): ExtendedMessage {
        return ExtendedMessage.builder()
            .setStreamId("action")
            .setKey(buildKey(hitId, hitTime))
            .setPayload(Action(hitId, hitTime, actionTime, isClick))
            .setEventTimestamp(eventTimestamp)
            .build()
    }

    /**
     * Builds a Timer for the "timer" stream.
     * triggerTimestamp = hitTime + WAIT_SECONDS (maxTime).
     * eventTimestamp is set to triggerTimestamp so the timer is not considered late.
     */
    private fun buildTimer(hitId: String, hitTime: Long): Timer {
        val maxTime = hitTime + WAIT_SECONDS
        return Timer(
            "",
            maxTime,
            0L,
            "timer",
            0L,
            maxTime,
            buildKey(hitId, hitTime)
        )
    }

    /**
     * Returns a watermarks map with both input streams set to WATERMARK=0.
     * This ensures messages with eventTimestamp >= 0 are not dropped as late.
     */
    private fun defaultWatermarks(): Map<String, Long> {
        return mapOf(
            "hit" to WATERMARK,
            "action" to WATERMARK
        )
    }

    // -------------------------------------------------------------------------
    // onMessage tests
    // -------------------------------------------------------------------------

    // [BEGIN test_hit]
    @Test
    fun testHitMessageStoresHitPayloadInState() {
        val hitId = "hit-1"
        val hitTime = BASE_HIT_TIME

        val messages = listOf(buildHitMessage(hitId, hitTime, "payload-data"))

        val request = TestDoProcessRequest.builder("join")
            .setMessages(messages)
            .setWatermarks(defaultWatermarks())
            .build()

        val response = testHarness.doProcess(request)

        // No output messages yet — timer is scheduled, output comes on timer fire
        assertTrue(response.outputMessagesFlatten.isEmpty())

        // A timer must be scheduled
        assertEquals(1, response.outputTimersFlatten.size)
        val timer = response.outputTimersFlatten.first()
        assertEquals(hitTime + WAIT_SECONDS, timer.triggerTimestamp)

        // External state must be updated with hit_payload
        assertEquals(1, response.allStates().externalSize("/join-state"))
        val key = buildKey(hitId, hitTime)
        val state = response.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key).get().orElseThrow()
        assertEquals("payload-data", state.get("hit_payload", String::class.java))
    }
    // [END test_hit]

    @Test
    fun testShowActionMessageStoresShowTimeInState() {
        val hitId = "hit-2"
        val hitTime = BASE_HIT_TIME
        val actionTime = hitTime + 5L
        // eventTimestamp must be < hitTime + WAIT_SECONDS and >= WATERMARK
        val eventTimestamp = hitTime + 1L

        val messages = listOf(buildActionMessage(hitId, hitTime, actionTime, false, eventTimestamp))

        val request = TestDoProcessRequest.builder("join")
            .setMessages(messages)
            .setWatermarks(defaultWatermarks())
            .build()

        val response = testHarness.doProcess(request)

        assertTrue(response.outputMessagesFlatten.isEmpty())
        assertEquals(1, response.outputTimersFlatten.size)

        val key = buildKey(hitId, hitTime)
        val state = response.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key).get().orElseThrow()
        assertEquals(actionTime, state.get("show_time", Long::class.java))
    }

    @Test
    fun testClickActionMessageStoresClickTimeInState() {
        val hitId = "hit-3"
        val hitTime = BASE_HIT_TIME
        val actionTime = hitTime + 5L
        val eventTimestamp = hitTime + 1L

        val messages = listOf(buildActionMessage(hitId, hitTime, actionTime, true, eventTimestamp))

        val request = TestDoProcessRequest.builder("join")
            .setMessages(messages)
            .setWatermarks(defaultWatermarks())
            .build()

        val response = testHarness.doProcess(request)

        assertTrue(response.outputMessagesFlatten.isEmpty())
        assertEquals(1, response.outputTimersFlatten.size)

        val key = buildKey(hitId, hitTime)
        val state = response.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key).get().orElseThrow()
        assertEquals(actionTime, state.get("click_time", Long::class.java))
    }

    @Test
    fun testMessageOutOfWaitRangeIsDropped() {
        val hitId = "hit-4"
        val hitTime = BASE_HIT_TIME
        // eventTimestamp >= hitTime + WAIT_SECONDS → message is out of wait range
        val eventTimestamp = hitTime + WAIT_SECONDS

        val messages = listOf(buildActionMessage(hitId, hitTime, hitTime + 5L, false, eventTimestamp))

        val request = TestDoProcessRequest.builder("join")
            .setMessages(messages)
            .setWatermarks(defaultWatermarks())
            .build()

        val response = testHarness.doProcess(request)

        // Message is dropped — no output, no timer, no state update
        assertTrue(response.outputMessagesFlatten.isEmpty())
        assertTrue(response.outputTimersFlatten.isEmpty())
        assertEquals(0, response.allStates().externalSize("/join-state"))
    }

    @Test
    fun testLateMessageIsDropped() {
        val hitId = "hit-5"
        val hitTime = BASE_HIT_TIME
        // Set watermark to hitTime + 5, so messages with eventTimestamp < hitTime + 5 are late
        val watermark = hitTime + 5L
        // eventTimestamp < watermark → late message
        val eventTimestamp = hitTime + 3L

        val messages = listOf(buildActionMessage(hitId, hitTime, hitTime + 2L, false, eventTimestamp))

        val request = TestDoProcessRequest.builder("join")
            .setMessages(messages)
            .setWatermarks(mapOf("hit" to watermark, "action" to watermark))
            .build()

        val response = testHarness.doProcess(request)

        // Late message is dropped — no output, no timer, no state update
        assertTrue(response.outputMessagesFlatten.isEmpty())
        assertTrue(response.outputTimersFlatten.isEmpty())
        assertEquals(0, response.allStates().externalSize("/join-state"))
    }

    // -------------------------------------------------------------------------
    // onTimer tests
    // -------------------------------------------------------------------------

    @Test
    fun testTimerEmitsJoinedActionWithShowAndClick() {
        val hitId = "hit-10"
        val hitTime = BASE_HIT_TIME
        val showTime = hitTime + 3L
        val clickTime = hitTime + 7L

        val key = buildKey(hitId, hitTime)

        // Pre-populate state: hit_payload + show_time + click_time
        val preState = PayloadBuilder(joinStateSchema)
            .set("hit_payload", "some-payload")
            .set("show_time", showTime)
            .set("click_time", clickTime)
            .finish()

        val timer = buildTimer(hitId, hitTime)

        val request = TestDoProcessRequest.builder("join")
            .setTimers(listOf(timer))
            .setState(JoinProcessFunction.JOINED_ACTION_STATE, key, preState)
            .build()

        val response = testHarness.doProcess(request)

        // One JoinedAction must be emitted
        assertEquals(1, response.outputMessagesFlatten.size)
        val msg = response.outputMessagesFlatten.first()
        assertEquals("joined_action", msg.streamId)

        val joinedAction = msg.getPayload<JoinedAction>()
        assertEquals(hitId, joinedAction.hitId)
        assertEquals(hitTime, joinedAction.hitTime)
        assertEquals("some-payload", joinedAction.hitPayload)
        assertEquals(showTime, joinedAction.showTime)
        assertTrue(joinedAction.isClick!!)
        assertEquals(clickTime, joinedAction.clickTime)

        // State must be cleared after timer fires
        assertTrue(response.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key).get().isEmpty)
    }

    @Test
    fun testTimerEmitsJoinedActionWithShowButNoClick() {
        val hitId = "hit-11"
        val hitTime = BASE_HIT_TIME
        val showTime = hitTime + 5L

        val key = buildKey(hitId, hitTime)

        val preState = PayloadBuilder(joinStateSchema)
            .set("hit_payload", "payload-11")
            .set("show_time", showTime)
            .set("click_time", 0L)
            .finish()

        val timer = buildTimer(hitId, hitTime)

        val request = TestDoProcessRequest.builder("join")
            .setTimers(listOf(timer))
            .setState(JoinProcessFunction.JOINED_ACTION_STATE, key, preState)
            .build()

        val response = testHarness.doProcess(request)

        assertEquals(1, response.outputMessagesFlatten.size)
        val joinedAction = response.outputMessagesFlatten.first().getPayload<JoinedAction>()
        assertEquals(hitId, joinedAction.hitId)
        assertEquals(hitTime, joinedAction.hitTime)
        assertEquals("payload-11", joinedAction.hitPayload)
        assertEquals(showTime, joinedAction.showTime)
        assertFalse(joinedAction.isClick!!)
        assertEquals(0L, joinedAction.clickTime)

        assertTrue(response.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key).get().isEmpty)
    }

    @Test
    fun testTimerDoesNotEmitWhenShowTimeIsZero() {
        val hitId = "hit-12"
        val hitTime = BASE_HIT_TIME

        val key = buildKey(hitId, hitTime)

        // State has hit_payload but show_time == 0 (no show event received)
        val preState = PayloadBuilder(joinStateSchema)
            .set("hit_payload", "payload-12")
            .set("show_time", 0L)
            .set("click_time", 0L)
            .finish()

        val timer = buildTimer(hitId, hitTime)

        val request = TestDoProcessRequest.builder("join")
            .setTimers(listOf(timer))
            .setState(JoinProcessFunction.JOINED_ACTION_STATE, key, preState)
            .build()

        val response = testHarness.doProcess(request)

        // No output: show_time == 0 means no show event was received
        assertTrue(response.outputMessagesFlatten.isEmpty())

        // State is still cleared
        assertTrue(response.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key).get().isEmpty)
    }

    @Test
    fun testTimerDoesNotEmitWhenHitPayloadIsAbsent() {
        val hitId = "hit-13"
        val hitTime = BASE_HIT_TIME
        val showTime = hitTime + 5L

        val key = buildKey(hitId, hitTime)

        // State has show_time but no hit_payload (null)
        val preState = PayloadBuilder(joinStateSchema)
            .set("show_time", showTime)
            .set("click_time", 0L)
            .finish()

        val timer = buildTimer(hitId, hitTime)

        val request = TestDoProcessRequest.builder("join")
            .setTimers(listOf(timer))
            .setState(JoinProcessFunction.JOINED_ACTION_STATE, key, preState)
            .build()

        val response = testHarness.doProcess(request)

        // No output: hit_payload is null
        assertTrue(response.outputMessagesFlatten.isEmpty())

        assertTrue(response.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key).get().isEmpty)
    }

    // -------------------------------------------------------------------------
    // Full flow integration test
    // -------------------------------------------------------------------------

    // [BEGIN test_full_flow]
    @Test
    fun testFullJoinFlow_HitThenShowThenClickThenTimer() {
        val hitId = "hit-20"
        val hitTime = BASE_HIT_TIME
        val showTime = hitTime + 3L
        val clickTime = hitTime + 7L

        // Step 1: process hit message
        val hitMessages = listOf(buildHitMessage(hitId, hitTime, "full-payload"))
        val hitRequest = TestDoProcessRequest.builder("join")
            .setMessages(hitMessages)
            .setWatermarks(defaultWatermarks())
            .build()
        val hitResponse = testHarness.doProcess(hitRequest)

        assertTrue(hitResponse.outputMessagesFlatten.isEmpty())
        assertEquals(1, hitResponse.outputTimersFlatten.size)
        assertEquals(1, hitResponse.allStates().externalSize("/join-state"))

        val key = buildKey(hitId, hitTime)
        val stateAfterHit = hitResponse.allStates()
            .get(JoinProcessFunction.JOINED_ACTION_STATE, key).get().orElseThrow()
        assertEquals("full-payload", stateAfterHit.get("hit_payload", String::class.java))

        // Step 2: process show action message, passing the state from step 1
        val showMessages = listOf(buildActionMessage(hitId, hitTime, showTime, false, hitTime + 1L))
        val showRequest = TestDoProcessRequest.builder("join")
            .setMessages(showMessages)
            .setState(JoinProcessFunction.JOINED_ACTION_STATE, key, stateAfterHit)
            .setWatermarks(defaultWatermarks())
            .build()
        val showResponse = testHarness.doProcess(showRequest)

        assertTrue(showResponse.outputMessagesFlatten.isEmpty())
        val stateAfterShow = showResponse.allStates()
            .get(JoinProcessFunction.JOINED_ACTION_STATE, key).get().orElseThrow()
        assertEquals("full-payload", stateAfterShow.get("hit_payload", String::class.java))
        assertEquals(showTime, stateAfterShow.get("show_time", Long::class.java))

        // Step 3: process click action message, passing the state from step 2
        val clickMessages = listOf(buildActionMessage(hitId, hitTime, clickTime, true, hitTime + 2L))
        val clickRequest = TestDoProcessRequest.builder("join")
            .setMessages(clickMessages)
            .setState(JoinProcessFunction.JOINED_ACTION_STATE, key, stateAfterShow)
            .setWatermarks(defaultWatermarks())
            .build()
        val clickResponse = testHarness.doProcess(clickRequest)

        assertTrue(clickResponse.outputMessagesFlatten.isEmpty())
        val stateAfterClick = clickResponse.allStates()
            .get(JoinProcessFunction.JOINED_ACTION_STATE, key).get().orElseThrow()
        assertEquals(clickTime, stateAfterClick.get("click_time", Long::class.java))

        // Step 4: fire the timer with the accumulated state
        val timer = buildTimer(hitId, hitTime)
        val timerRequest = TestDoProcessRequest.builder("join")
            .setTimers(listOf(timer))
            .setState(JoinProcessFunction.JOINED_ACTION_STATE, key, stateAfterClick)
            .build()
        val timerResponse = testHarness.doProcess(timerRequest)

        assertEquals(1, timerResponse.outputMessagesFlatten.size)
        val joinedAction = timerResponse.outputMessagesFlatten.first().getPayload<JoinedAction>()
        assertEquals(hitId, joinedAction.hitId)
        assertEquals(hitTime, joinedAction.hitTime)
        assertEquals("full-payload", joinedAction.hitPayload)
        assertEquals(showTime, joinedAction.showTime)
        assertTrue(joinedAction.isClick!!)
        assertEquals(clickTime, joinedAction.clickTime)

        assertTrue(timerResponse.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key).get().isEmpty)
    }
    // [END test_full_flow]

    @Test
    fun testMultipleHitsAreProcessedIndependently() {
        val hitId1 = "hit-30"
        val hitId2 = "hit-31"
        val hitTime1 = BASE_HIT_TIME
        val hitTime2 = BASE_HIT_TIME + 100L

        val messages = listOf(
            buildHitMessage(hitId1, hitTime1, "payload-30"),
            buildHitMessage(hitId2, hitTime2, "payload-31")
        )

        val request = TestDoProcessRequest.builder("join")
            .setMessages(messages)
            .setWatermarks(defaultWatermarks())
            .build()

        val response = testHarness.doProcess(request)

        assertTrue(response.outputMessagesFlatten.isEmpty())
        assertEquals(2, response.outputTimersFlatten.size)
        assertEquals(2, response.allStates().externalSize("/join-state"))

        val key1 = buildKey(hitId1, hitTime1)
        val key2 = buildKey(hitId2, hitTime2)

        val state1 = response.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key1).get().orElseThrow()
        assertEquals("payload-30", state1.get("hit_payload", String::class.java))

        val state2 = response.allStates().get(JoinProcessFunction.JOINED_ACTION_STATE, key2).get().orElseThrow()
        assertEquals("payload-31", state2.get("hit_payload", String::class.java))
    }
}
