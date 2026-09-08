package tech.ytsaurus.flow.testutils;

import java.util.Map;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.flow.state.StateDescriptors;
import tech.ytsaurus.flow.test.TTestMessage;
import tech.ytsaurus.typeinfo.TiType;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestDoProcessRequestTest {

    private static final String STATE = "/state";
    private static final TableSchema KEY_SCHEMA = TableSchema.builder()
            .addValue("k", TiType.string())
            .build();
    private static final TableSchema STATE_SCHEMA = TableSchema.builder()
            .addValue("count", TiType.int64())
            .build();
    private static final TTestMessage MESSAGE = TTestMessage.newBuilder()
            .setId("id").setTime(0).setCount(1).setValue(0).setIsValue(false).build();

    private static Payload key(String k) {
        return new PayloadBuilder(KEY_SCHEMA).set("k", k).finish();
    }

    private static Payload row() {
        return new PayloadBuilder(STATE_SCHEMA).set("count", 1L).finish();
    }

    @Test
    void rowSeedAfterProtoSeedIsRejected() {
        var request = TestDoProcessRequest.builder("c")
                .setState(StateDescriptors.externalProto(STATE, TTestMessage.class), key("a"), MESSAGE)
                .setState(StateDescriptors.external(STATE), key("b"), row())
                .build();
        var e = assertThrows(IllegalArgumentException.class, () -> request.seedStates(Map.of(STATE, STATE_SCHEMA)));
        assertTrue(e.getMessage().contains("mixes proto-format and row-format"), e.getMessage());
    }

    @Test
    void protoSeedAfterRowSeedIsRejected() {
        var request = TestDoProcessRequest.builder("c")
                .setState(StateDescriptors.external(STATE), key("a"), row())
                .setState(StateDescriptors.externalProto(STATE, TTestMessage.class), key("b"), MESSAGE)
                .build();
        assertThrows(IllegalArgumentException.class, () -> request.seedStates(Map.of(STATE, STATE_SCHEMA)));
    }
}
