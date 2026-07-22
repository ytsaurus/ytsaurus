package tech.ytsaurus.flow.job;

import java.time.Duration;
import java.util.Optional;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.GUID;

/**
 * A context manager for jobs that provides caching functionality with TTL (Time To Live) support.
 * This class uses Caffeine cache to store and retrieve Job instances by their GUID identifiers.
 * Jobs are automatically expired from the cache after the specified TTL period of inactivity.
 */
public class JobContext {

    private final Cache<GUID, Job> jobCache;

    /**
     * Creates a new JobContext with the specified TTL for job entries.
     *
     * @param jobTTL the duration after which job entries expire if not accessed
     */
    public JobContext(Duration jobTTL) {
        this.jobCache = Caffeine.newBuilder()
                .expireAfterAccess(jobTTL)
                .build();
    }

    /**
     * Retrieves a job by its ID as an Optional.
     *
     * @param jobId the GUID of the job to retrieve
     * @return an Optional containing the job if found, or empty if not found
     */
    public Optional<Job> getJobOptional(GUID jobId) {
        return Optional.ofNullable(jobCache.getIfPresent(jobId));
    }

    /**
     * Retrieves a job by its ID, returning null if not found.
     *
     * @param jobId the GUID of the job to retrieve
     * @return the job if found, or null if not found
     */
    public @Nullable Job getJob(GUID jobId) {
        return jobCache.getIfPresent(jobId);
    }

    /**
     * Retrieves a job by its ID, throwing an IllegalStateException if not found.
     * This method is used when the job is expected to exist and its absence indicates an error.
     *
     * @param jobId the GUID of the job to retrieve
     * @return the job with the specified ID
     * @throws IllegalStateException if the job is not found in the cache
     */
    public Job getJobOrCrash(GUID jobId) {
        var job = jobCache.getIfPresent(jobId);
        if (job == null) {
            throw new IllegalStateException("Job not found at companion: " + jobId);
        }
        return job;
    }

    /**
     * Stores a job in the cache with the specified ID.
     *
     * @param jobId the GUID to associate with the job (must not be null)
     * @param job   the job to store in the cache (must not be null)
     */
    public void putJob(GUID jobId, Job job) {
        jobCache.put(jobId, job);
    }

    /**
     * Removes a job from the cache by its ID.
     *
     * @param jobId the GUID of the job to remove (must not be null)
     */
    public void removeJob(GUID jobId) {
        jobCache.invalidate(jobId);
    }

}
