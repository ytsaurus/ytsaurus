package tech.ytsaurus.flow.spring;

import java.io.IOException;

import org.springframework.context.SmartLifecycle;
import tech.ytsaurus.flow.execution.CompanionExecution;

/**
 * Spring lifecycle component that manages the Flow companion gRPC server.
 * <p>
 * This component implements {@link SmartLifecycle} to properly integrate with
 * Spring's application lifecycle. The gRPC server is started automatically when
 * the Spring context is ready and stopped gracefully during shutdown.
 * <p>
 * The server is started with the highest phase value to ensure it starts after
 * all other beans are initialized and stops before other beans are destroyed.
 * <p>
 * This class delegates to {@link CompanionExecution} for the actual server management.
 *
 * @see SmartLifecycle
 * @see FlowAutoConfiguration
 * @see CompanionExecution
 */
public class FlowCompanionLifecycle implements SmartLifecycle {

    private final CompanionExecution companionExecution;

    /**
     * Creates a new FlowCompanionLifecycle.
     *
     * @param companionExecution flow companion execution.
     */
    public FlowCompanionLifecycle(CompanionExecution companionExecution) {
        this.companionExecution = companionExecution;
    }

    @Override
    public void start() {
        try {
            companionExecution.startAsync();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start gRPC companion server", e);
        }
    }

    @Override
    public void stop() {
        companionExecution.stop();
    }

    @Override
    public boolean isRunning() {
        return companionExecution.isRunning();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        // Start last (after all other beans), stop first (before other beans).
        return Integer.MAX_VALUE;
    }

    /**
     * Returns the port on which the gRPC server is listening.
     * <p>
     * This method is useful for testing when using port 0 (auto-assigned port).
     *
     * @return The server port, or -1 if the server is not running.
     */
    public int getPort() {
        return companionExecution.getPort();
    }
}
