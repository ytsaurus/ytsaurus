package tech.ytsaurus.client.request;

import tech.ytsaurus.rpcproxy.EQueryState;

/**
 * Immutable query state.
 *
 * @see Query#getState()
 */
public enum QueryState {
    Draft(EQueryState.QS_DRAFT, "draft"),
    Pending(EQueryState.QS_PENDING, "pending"),
    Running(EQueryState.QS_RUNNING, "running"),
    Aborting(EQueryState.QS_ABORTING, "aborting"),
    Aborted(EQueryState.QS_ABORTED, "aborted"),
    Completing(EQueryState.QS_COMPLETING, "completing"),
    Completed(EQueryState.QS_COMPLETED, "completed"),
    Failing(EQueryState.QS_FAILING, "failing"),
    Failed(EQueryState.QS_FAILED, "failed");


    private final EQueryState protoValue;
    private final String stringValue;

    QueryState(EQueryState protoValue, String stringValue) {
        this.protoValue = protoValue;
        this.stringValue = stringValue;
    }

    public static QueryState fromProtoValue(EQueryState protoValue) {
        switch (protoValue) {
            case QS_DRAFT:
                return Draft;
            case QS_PENDING:
                return Pending;
            case QS_RUNNING:
                return Running;
            case QS_ABORTING:
                return Aborting;
            case QS_ABORTED:
                return Aborted;
            case QS_COMPLETING:
                return Completing;
            case QS_COMPLETED:
                return Completed;
            case QS_FAILING:
                return Failing;
            case QS_FAILED:
                return Failed;
            default:
                throw new IllegalArgumentException("Illegal query state value " + protoValue);
        }
    }

    @Override
    public String toString() {
        return stringValue;
    }

    EQueryState getProtoValue() {
        return protoValue;
    }
}
