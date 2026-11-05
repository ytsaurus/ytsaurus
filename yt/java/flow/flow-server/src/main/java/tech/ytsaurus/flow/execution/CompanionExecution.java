package tech.ytsaurus.flow.execution;

import java.io.IOException;

/**
 * Run-phase contract for a Flow companion execution.
 *
 * <p>This interface exposes the run-phase operations.
 *
 * @see GrpcServerExecution
 * @see CompanionExecutionSpec
 */
public interface CompanionExecution {

    /**
     * Start the companion in blocking mode, blocking until termination.
     *
     * @throws InterruptedException In case of thread interruption.
     * @throws IOException          In case of I/O error.
     */
    void start() throws InterruptedException, IOException;

    /**
     * Start the companion asynchronously, returning immediately.
     *
     * @throws IOException In case of I/O error during startup.
     */
    void startAsync() throws IOException;

    /**
     * Stop the companion gracefully.
     */
    void stop();

    /**
     * Returns whether the companion is currently running.
     *
     * @return true if running, false otherwise.
     */
    boolean isRunning();

    /**
     * Returns the port the gRPC server is listening on, or {@code -1} if it is not running.
     *
     * @return the gRPC server port, or -1.
     */
    int getPort();

    /**
     * Returns the port the monitoring HTTP server is listening on, or {@code -1} if it is
     * not running.
     *
     * @return the monitoring HTTP server port, or -1.
     */
    int getMonitoringPort();
}
