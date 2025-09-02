package tech.ytsaurus.client.request;

/**
 * Flow Pipeline state. NYT::NFlow::EPipelineState
 * <p>
 * 1) Stopped - everything stopped and all internal queues is empty.
 * 2) Paused - everything stopped but some internal queues may not be empty.
 * 3) Working - usual work.
 * 4) Draining - preparing to stop pipeline.
 * Injection of new messages and trigger activation disabled, waiting for emptiness of internal queues.
 * 5) Pausing - preparing to pause pipeline.
 * 6) Completed - everything has been processed.
 */
public enum PipelineState {
    UNKNOWN("Unknown", 0),
    STOPPED("Stopped", 1),
    PAUSED("Paused", 2),
    WORKING("Working", 3),
    DRAINING("Draining", 4),
    PAUSING("Pausing", 5),
    COMPLETED("Completed", 6);

    private final String name;
    private final int protoValue;

    PipelineState(String name, int protoValue) {
        this.name = name;
        this.protoValue = protoValue;
    }

    public static PipelineState valueOf(int protoValue) {
        for (PipelineState state : values()) {
            if (state.protoValue == protoValue) {
                return state;
            }
        }
        throw new IllegalArgumentException("Unknown pipeline state: " + protoValue);
    }

    public String getName() {
        return name;
    }

    public int getProtoValue() {
        return protoValue;
    }
}
