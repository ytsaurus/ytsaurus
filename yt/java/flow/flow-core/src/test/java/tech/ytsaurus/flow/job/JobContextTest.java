package tech.ytsaurus.flow.job;

import java.time.Duration;

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
        var jobContext = new JobContext(Duration.ofMinutes(10));
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
        var jobContext = new JobContext(Duration.ofMinutes(10));
        var jobId = GUID.create();

        // Act
        var result = jobContext.getJobOptional(jobId);

        // Assert
        assertTrue(result.isEmpty());
    }

    @Test
    void testGetJob_whenJobExists_thenReturnJob() {
        // Arrange
        var jobContext = new JobContext(Duration.ofMinutes(10));
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
        var jobContext = new JobContext(Duration.ofMinutes(10));
        var jobId = GUID.create();

        // Act
        var result = jobContext.getJob(jobId);

        // Assert
        assertNull(result);
    }

    @Test
    void testGetJobOrCrash_whenJobExists_thenReturnJob() {
        // Arrange
        var jobContext = new JobContext(Duration.ofMinutes(10));
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
        var jobContext = new JobContext(Duration.ofMinutes(10));
        var jobId = GUID.create();

        // Act & Assert
        assertThrows(IllegalStateException.class, () -> jobContext.getJobOrCrash(jobId));
    }

    @Test
    void testPutJob_thenJobIsStored() {
        // Arrange
        var jobContext = new JobContext(Duration.ofMinutes(10));
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
    void testGetJob_expired() throws InterruptedException {
        // Arrange
        var jobContext = new JobContext(Duration.ofMillis(10));
        var jobId = GUID.create();
        var job = mock(Job.class);

        // Act
        jobContext.putJob(jobId, job);
        Thread.sleep(20);
        // Assert
        assertFalse(jobContext.getJobOptional(jobId).isPresent());
    }

    @Test
    void testRemoveJob() {
        // Arrange
        var jobContext = new JobContext(Duration.ofMinutes(10));
        var jobId = GUID.create();
        var job = mock(Job.class);
        jobContext.putJob(jobId, job);

        jobContext.removeJob(jobId);

        assertFalse(jobContext.getJobOptional(jobId).isPresent());
    }
}
