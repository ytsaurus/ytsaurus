package tech.ytsaurus.flow.job;

import java.util.Set;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.core.GUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class JobContextTest {

    @Test
    void testGetJobOptional_whenJobExists_thenReturnJob() {
        // Arrange
        var jobContext = new JobContext();
        var jobId = GUID.create();
        var job = mock(Job.class);
        jobContext.putJob(jobId, job);

        // Act
        var result = jobContext.getJobOptional(jobId);

        // Assert
        assertTrue(result.isPresent());
        assertEquals(job, result.get());
    }

    @Test
    void testGetJobOptional_whenJobNotExists_thenReturnEmpty() {
        // Arrange
        var jobContext = new JobContext();
        var jobId = GUID.create();

        // Act
        var result = jobContext.getJobOptional(jobId);

        // Assert
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetJob_whenJobExists_thenReturnJob() {
        // Arrange
        var jobContext = new JobContext();
        var jobId = GUID.create();
        var job = mock(Job.class);
        jobContext.putJob(jobId, job);

        // Act
        var result = jobContext.getJob(jobId);

        // Assert
        assertEquals(job, result);
    }

    @Test
    void testGetJob_whenJobNotExists_thenReturnNull() {
        // Arrange
        var jobContext = new JobContext();
        var jobId = GUID.create();

        // Act
        var result = jobContext.getJob(jobId);

        // Assert
        assertNull(result);
    }

    @Test
    void testGetJobOrCrash_whenJobExists_thenReturnJob() {
        // Arrange
        var jobContext = new JobContext();
        var jobId = GUID.create();
        var job = mock(Job.class);
        jobContext.putJob(jobId, job);

        // Act
        var result = jobContext.getJobOrCrash(jobId);

        // Assert
        assertEquals(job, result);
    }

    @Test
    void testGetJobOrCrash_whenJobNotExists_thenThrowException() {
        // Arrange
        var jobContext = new JobContext();
        var jobId = GUID.create();

        // Act & Assert
        assertThrows(IllegalStateException.class, () -> jobContext.getJobOrCrash(jobId));
    }

    @Test
    void testPutJob_thenJobIsStored() {
        // Arrange
        var jobContext = new JobContext();
        var jobId = GUID.create();
        var job = mock(Job.class);

        // Act
        jobContext.putJob(jobId, job);

        // Assert
        assertEquals(job, jobContext.getJob(jobId));
        assertTrue(jobContext.getJobOptional(jobId).isPresent());
        assertEquals(job, jobContext.getJobOrCrash(jobId));
    }

    @Test
    void testRemoveJob() {
        // Arrange
        var jobContext = new JobContext();
        var jobId = GUID.create();
        var job = mock(Job.class);
        jobContext.putJob(jobId, job);

        jobContext.removeJob(jobId);

        assertFalse(jobContext.getJobOptional(jobId).isPresent());
    }

    @Test
    void testPutJob_registersARemovedJobAgain() {
        // Arrange
        var jobContext = new JobContext();
        var jobId = GUID.create();
        var job = mock(Job.class);
        jobContext.putJob(jobId, job);
        jobContext.removeJob(jobId);

        // Act: a registration processed after a removal recreates the entry;
        // if its job is gone from the worker, the reconcile pass reclaims it.
        jobContext.putJob(jobId, job);

        // Assert
        assertEquals(job, jobContext.getJob(jobId));
    }

    @Test
    void testClear_forgetsJobs() {
        // Arrange
        var jobContext = new JobContext();
        var jobId = GUID.create();
        var job = mock(Job.class);
        jobContext.putJob(jobId, job);

        // Act
        jobContext.clear();

        // Assert: a fresh serving generation starts empty.
        assertNull(jobContext.getJob(jobId));
        jobContext.putJob(jobId, job);
        assertEquals(job, jobContext.getJob(jobId));
    }

    @Test
    void testListJobIds() {
        var jobContext = new JobContext();
        assertTrue(jobContext.listJobIds().isEmpty());

        var jobId = GUID.create();
        jobContext.putJob(jobId, mock(Job.class));
        assertEquals(Set.of(jobId), jobContext.listJobIds());

        jobContext.removeJob(jobId);
        assertTrue(jobContext.listJobIds().isEmpty());
    }

    @Test
    void testRemoveJob_isIdempotent() {
        // Arrange
        var jobContext = new JobContext();
        var jobId = GUID.create();

        // Removing an unknown job is a no-op.
        jobContext.removeJob(jobId);

        var job = mock(Job.class);
        jobContext.putJob(jobId, job);
        jobContext.removeJob(jobId);
        jobContext.removeJob(jobId);

        assertNull(jobContext.getJob(jobId));
    }
}
