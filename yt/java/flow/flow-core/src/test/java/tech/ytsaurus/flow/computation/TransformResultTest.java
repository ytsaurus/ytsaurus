package tech.ytsaurus.flow.computation;

import java.util.List;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.flow.row.Message;
import tech.ytsaurus.flow.row.Payload;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransformResultTest {

    @Test
    void nullOptionsDoNotMutateResult() {
        var result = new TransformResult(List.of("parent"));
        var message = Message.builder().setStreamId("output").setPayload(Payload.EMPTY).build();

        assertThrows(NullPointerException.class, () -> result.addMessage(message, null));

        assertTrue(result.getMessages().isEmpty());
        assertTrue(result.getDistribute().isEmpty());
        assertTrue(result.getMessageIdSuffixes().isEmpty());
    }
}
