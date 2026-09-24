package tech.ytsaurus.flow.service;

import tech.ytsaurus.flow.rpc.TJobInfo;

/**
 * Validates native job metadata before it is registered or used to decode a request.
 */
@FunctionalInterface
public interface JobSpecValidator {
    JobSpecValidator NOOP = (computationId, jobInfo) -> { };

    /**
     * Throws if the job is incompatible with the application's computation contract.
     */
    void validate(String computationId, TJobInfo jobInfo);
}
