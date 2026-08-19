package tech.ytsaurus.flow.state;

import org.jspecify.annotations.Nullable;

/**
 * Wrapper for binary state.
 */
public class InternalState extends State<byte[]> {
    public static final InternalState RESET = new InternalState(true, null);

    public InternalState(byte[] bytes) {
        super(bytes);
    }

    public InternalState(boolean reset, byte @Nullable [] bytes) {
        super(reset, bytes);
    }
}
