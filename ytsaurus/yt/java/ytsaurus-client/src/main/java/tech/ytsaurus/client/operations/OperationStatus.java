package tech.ytsaurus.client.operations;

import tech.ytsaurus.core.StringValueEnum;
import tech.ytsaurus.core.StringValueEnumResolver;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

@NonNullApi
@NonNullFields
public enum OperationStatus implements StringValueEnum {

    UNKNOWN(false, false, "unknown"),
    STARTING(false, false, "starting"),
    INITIALIZING(false, false, "initializing"),
    PREPARING(false, false, "preparing"),
    MATERIALIZING(false, false, "materializing"),
    REVIVING(false, false, "reviving"),
    REVIVE_INITIALIZING(false, false, "revive_initializing"),
    ORPHANED(false, false, "orphaned"),
    WAITING_FOR_AGENT(false, false, "waiting_for_agent"),
    PENDING(false, false, "pending"),
    RUNNING(false, false, "running"),

    SUSPENDED(false, false, "suspended"),

    COMPLETING(false, false, "completing"),
    COMPLETED(true, true, "completed"),

    ABORTING(false, false, "aborting"),
    ABORTED(true, false, "aborted"),

    FAILING(false, false, "failing"),
    FAILED(true, false, "failed");

    public static final StringValueEnumResolver<OperationStatus> R = StringValueEnumResolver.of(OperationStatus.class);

    private final boolean finished;
    private final boolean success;

    private final String value;

    OperationStatus(boolean finished, boolean success, String value) {
        this.finished = finished;
        this.success = success;
        this.value = value;
    }

    public boolean isFinished() {
        return finished;
    }

    public boolean isSuccess() {
        return success;
    }

    @Override
    public String value() {
        return value;
    }

}
