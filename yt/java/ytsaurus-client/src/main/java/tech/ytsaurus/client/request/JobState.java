package tech.ytsaurus.client.request;

import java.util.HashMap;
import java.util.Map;

import tech.ytsaurus.rpcproxy.EJobState;

public enum JobState {
    Unknown("unknown", 100),
    Waiting("waiting", 0),
    Running("running", 1),
    Aborting("aborting", 2),
    Completed("completed", 3),
    Failed("failed", 4),
    Aborted("aborted", 5),
    Lost("lost", 6),
    None("none", 7);

    private final String wireName;
    private final int protoValue;

    private static final Map<Integer, JobState> INDEX = new HashMap<>();

    JobState(String wireName, int protoValue) {
        this.wireName = wireName;
        this.protoValue = protoValue;
    }

    public static JobState fromProto(EJobState state) {
        return INDEX.getOrDefault(state.getNumber(), JobState.Unknown);
    }

    public int getProtoValue() {
        return protoValue;
    }

    public String getWireName() {
        return wireName;
    }

    static {
        for (JobState entity : JobState.values()) {
            INDEX.put(entity.getProtoValue(), entity);
        }
    }
}
