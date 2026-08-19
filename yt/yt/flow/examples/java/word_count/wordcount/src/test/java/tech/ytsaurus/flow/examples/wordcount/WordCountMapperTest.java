package tech.ytsaurus.flow.examples.wordcount;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.examples.wordcount.model.Word;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.testutils.TestComputationHarness;
import tech.ytsaurus.flow.testutils.TestDoProcessRequest;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(classes = WordCountTestConfiguration.class)
class WordCountMapperTest {

    @Autowired
    private PipelineContext pipelineContext;

    private TestComputationHarness testHarness;

    @BeforeEach
    void init() throws IOException, URISyntaxException {
        try (var specStream = getClass().getResourceAsStream("/pipeline.yson")) {
            YTreeNode spec = YTreeTextSerializer.deserialize(specStream);
            this.testHarness = TestComputationHarness.builder()
                    .setPipelineContext(pipelineContext)
                    .setPipelineSpec(spec)
                    .build();
        }
    }

    @Test
    void testMapper() {
        var messages = new ArrayList<ExtendedMessage>();
        var keyBuilder = new PayloadBuilder(testHarness.getStream("words").getSchema());
        keyBuilder.set("word", "hello");
        Payload key = keyBuilder.finish();
        ExtendedMessage message = ExtendedMessage.builder()
                .setStreamId("words")
                .setKey(key)
                .setPayload(new Word("hello"))
                .build();
        messages.add(message);
        var request = TestDoProcessRequest.builder("mapper")
                .setMessages(messages)
                .build();
        var response = testHarness.doProcess(request);
        assertNotNull(response);
        assertEquals(1, response.allStates().internalSize("word-state"));
        var wordCountStateItem = response.allStates().get(WordCountMapper.WORD_STATE, key).get().orElseThrow();
        assertEquals(1, wordCountStateItem.getCount());
        assertEquals("hello", wordCountStateItem.getWord());
    }
}
