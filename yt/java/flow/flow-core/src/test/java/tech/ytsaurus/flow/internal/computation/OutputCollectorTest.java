package tech.ytsaurus.flow.internal.computation;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.flow.computation.AddMessageOptions;
import tech.ytsaurus.flow.computation.MessageIdSuffix;
import tech.ytsaurus.flow.row.Message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OutputCollectorTest {
    @Test
    void recordsAddMessageOptions() {
        var root = new RootCollector();
        var output = root.setParentIds("input");
        output.addMessage(
                Message.builder().setMessageId("output").build(),
                AddMessageOptions.builder()
                        .setDistribute(false)
                        .setMessageIdSuffix(MessageIdSuffix.payloadHash())
                        .build());

        var result = root.collectResults().get(0);
        assertFalse(result.getDistribute().get(0));
        assertEquals(MessageIdSuffix.Mode.PAYLOAD_HASH, result.getMessageIdSuffixes().get(0).getMode());
    }

    @Test
    void rejectsEmptyUserDefinedSuffix() {
        assertThrows(IllegalArgumentException.class, () -> MessageIdSuffix.userDefined(""));
    }
}
