package tech.ytsaurus.flow.examples.wordcount

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import tech.ytsaurus.flow.context.PipelineContext
import tech.ytsaurus.flow.examples.wordcount.model.Word
import tech.ytsaurus.flow.row.ExtendedMessage
import tech.ytsaurus.flow.row.PayloadBuilder
import tech.ytsaurus.flow.testutils.TestComputationHarness
import tech.ytsaurus.flow.testutils.TestDoProcessRequest
import tech.ytsaurus.ysontree.YTreeTextSerializer

@SpringBootTest(classes = [WordCountTestConfiguration::class])
class WordCountMapperTest {

    @Autowired
    private lateinit var pipelineContext: PipelineContext

    private lateinit var testHarness: TestComputationHarness

    @BeforeEach
    fun init() {
        javaClass.getResourceAsStream("/pipeline.yson")!!.use { specStream ->
            val spec = YTreeTextSerializer.deserialize(specStream)
            this.testHarness = TestComputationHarness.builder()
                .setPipelineContext(pipelineContext)
                .setPipelineSpec(spec)
                .build()
        }
    }

    @Test
    fun testMapper() {
        val messages = ArrayList<ExtendedMessage>()
        val keyBuilder = PayloadBuilder(testHarness.getStream("words")!!.schema)
        keyBuilder.set("word", "hello")
        val key = keyBuilder.finish()
        val message = ExtendedMessage.builder()
            .setStreamId("words")
            .setKey(key)
            .setPayload(Word("hello"))
            .build()
        messages.add(message)
        val request = TestDoProcessRequest.builder("mapper")
            .setMessages(messages)
            .build()
        val response = testHarness.doProcess(request)
        assertNotNull(response)
        assertEquals(1, response.allStates().internalSize("word-state"))
        val wordCountStateItem = response.allStates().get(WordCountMapper.WORD_STATE, key).get().orElseThrow()
        assertEquals(1, wordCountStateItem.count)
        assertEquals("hello", wordCountStateItem.word)
    }
}
