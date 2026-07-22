package tech.ytsaurus.flow.testutils;

import java.time.Duration;
import java.util.Optional;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.flow.job.Job;
import tech.ytsaurus.flow.job.JobContext;

/**
 * A {@link JobContext} implementation that does not cache jobs.
 * All operations related to job retrieval return empty or null values,
 * and operations related to job storage are no-ops.
 */
public class NoCacheJobContext extends JobContext {

    public NoCacheJobContext() {
        super(Duration.ofSeconds(1));
    }

    @Override
    public Optional<Job> getJobOptional(GUID jobId) {
        return Optional.empty();
    }

    @Override
    public @Nullable Job getJob(GUID jobId) {
        return null;
    }

    @Override
    public Job getJobOrCrash(GUID jobId) {
        throw new IllegalStateException("Job not found at companion: " + jobId);
    }

    @Override
    public void putJob(GUID jobId, Job job) {
        //Noop.
    }

    @Override
    public void removeJob(GUID jobId) {
        //Noop.
    }
}
