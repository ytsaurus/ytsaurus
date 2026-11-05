package tech.ytsaurus.flow.service;

import tech.ytsaurus.flow.job.Job;
import tech.ytsaurus.flow.rpc.TJobInfo;
import tech.ytsaurus.flow.rpc.TReqProcessBatch;

/**
 * Validates native job metadata before it is registered or used to decode a request.
 * <p>
 * Both checks are abstract, so an implementation states explicitly whether it validates batches.
 */
public interface JobSpecValidator {
    JobSpecValidator NOOP = new JobSpecValidator() {
        @Override
        public void validate(String computationId, TJobInfo jobInfo) {
        }

        @Override
        public void validateRequest(Job job, TReqProcessBatch request) {
        }
    };

    /**
     * Throws if the job is incompatible with the application's computation contract.
     */
    void validate(String computationId, TJobInfo jobInfo);

    /**
     * Checks batch metadata, including source stream schemas, before decoding or invoking user code.
     */
    void validateRequest(Job job, TReqProcessBatch request);
}
