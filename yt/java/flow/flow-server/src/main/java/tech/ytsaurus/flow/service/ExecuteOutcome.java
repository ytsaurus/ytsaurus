package tech.ytsaurus.flow.service;

import java.util.Objects;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.flow.rpc.EResourceExecuteStatus;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Outcome of one resource command; user-code failures travel in-band.
 *
 * @param status       the in-band command status.
 * @param errorMessage the error message; empty on success.
 * @param result       the per-command result, or {@code null} when the command returns none.
 */
public record ExecuteOutcome(EResourceExecuteStatus status, String errorMessage, @Nullable YTreeNode result) {

    /**
     * Creates an outcome without a result.
     */
    public ExecuteOutcome(EResourceExecuteStatus status, String errorMessage) {
        this(status, errorMessage, null);
    }

    /**
     * Validates that the status and the error message are present.
     */
    public ExecuteOutcome {
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(errorMessage, "errorMessage must not be null");
    }

    /**
     * Returns the successful outcome.
     */
    public static ExecuteOutcome ok() {
        return new ExecuteOutcome(EResourceExecuteStatus.RES_OK, "");
    }
}
