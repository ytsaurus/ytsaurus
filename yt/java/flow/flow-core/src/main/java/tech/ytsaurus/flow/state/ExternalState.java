package tech.ytsaurus.flow.state;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.flow.row.Payload;

public class ExternalState extends State<Payload> {

    public static final ExternalState RESET = new ExternalState(true, null);

    public ExternalState(Payload payload) {
        super(payload);
    }

    public ExternalState(boolean reset, @Nullable Payload payload) {
        super(reset, payload);
    }
}
