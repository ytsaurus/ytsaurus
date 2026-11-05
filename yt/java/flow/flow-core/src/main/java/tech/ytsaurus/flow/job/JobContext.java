package tech.ytsaurus.flow.job;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.GUID;

/**
 * Registry of jobs owned by the worker: entries are created and updated by
 * {@code PutJob} and removed by {@code RemoveJob}, so an entry lives exactly
 * as long as its job.
 */
public class JobContext {

    private final Map<GUID, Job> jobs = new ConcurrentHashMap<>();

    /**
     * Retrieves a job by its ID as an Optional.
     *
     * @param jobId the GUID of the job to retrieve
     * @return an Optional containing the job if found, or empty if not found
     */
    public Optional<Job> getJobOptional(GUID jobId) {
        return Optional.ofNullable(jobs.get(jobId));
    }

    /**
     * Retrieves a job by its ID, returning null if not found.
     *
     * @param jobId the GUID of the job to retrieve
     * @return the job if found, or null if not found
     */
    public @Nullable Job getJob(GUID jobId) {
        return jobs.get(jobId);
    }

    /**
     * Retrieves a job by its ID, throwing an IllegalStateException if not found.
     * This method is used when the job is expected to exist and its absence indicates an error.
     *
     * @param jobId the GUID of the job to retrieve
     * @return the job with the specified ID
     * @throws IllegalStateException if the job is not found in the registry
     */
    public Job getJobOrCrash(GUID jobId) {
        var job = jobs.get(jobId);
        if (job == null) {
            throw new IllegalStateException("Job not found at companion: " + jobId);
        }
        return job;
    }

    /**
     * Stores a job in the registry with the specified ID.
     *
     * @param jobId the GUID to associate with the job (must not be null)
     * @param job   the job to store (must not be null)
     */
    public void putJob(GUID jobId, Job job) {
        jobs.put(jobId, job);
    }

    /**
     * Removes a job from the registry by its ID. Removal is idempotent:
     * unknown ids are ignored.
     *
     * @param jobId the GUID of the job to remove (must not be null)
     */
    public void removeJob(GUID jobId) {
        jobs.remove(jobId);
    }

    /**
     * @return the ids of every registered job.
     */
    public Set<GUID> listJobIds() {
        return Set.copyOf(jobs.keySet());
    }

    /**
     * Forgets every job, so a server that starts serving again does not answer
     * for jobs of its previous generation.
     */
    public void clear() {
        jobs.clear();
    }

}
