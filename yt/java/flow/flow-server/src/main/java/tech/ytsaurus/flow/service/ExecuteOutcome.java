package tech.ytsaurus.flow.service;

import java.util.Objects;

import tech.ytsaurus.flow.rpc.EResourceExecuteStatus;

/**
 * Outcome of one resource command; user-code failures travel in-band.
 *
 * @param status       the in-band command status.
 * @param errorMessage the error message; empty on success.
 */
public record ExecuteOutcome(EResourceExecuteStatus status, String errorMessage) {

    /**
     * Validates that all components are present.
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
