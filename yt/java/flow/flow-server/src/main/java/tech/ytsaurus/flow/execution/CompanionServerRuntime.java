package tech.ytsaurus.flow.execution;

import java.io.IOException;

/**
 * Runtime components of the gRPC and monitoring servers running inside one companion JVM.
 */
interface CompanionServerRuntime {
    /**
     * Starts and returns the one server runtime owned by a companion execution.
     */
    interface Starter {
        CompanionServerRuntime start() throws IOException;
    }

    boolean isServing();

    int grpcPort();

    int monitoringPort();

    void awaitTermination() throws InterruptedException;

    void shutdown();
}
