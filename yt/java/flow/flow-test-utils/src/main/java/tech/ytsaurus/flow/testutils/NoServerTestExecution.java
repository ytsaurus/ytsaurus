package tech.ytsaurus.flow.testutils;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.config.CompanionExecutionConfig;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.execution.CompanionExecution;
import tech.ytsaurus.flow.job.JobContext;

/**
 * A test-only {@link CompanionExecution} that skips starting an actual gRPC server.
 * Mostly used for Spring Boot related testing.
 */
public class NoServerTestExecution implements CompanionExecution {
    private static final Logger log = LoggerFactory.getLogger(NoServerTestExecution.class);

    private final CompanionExecutionConfig config;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public NoServerTestExecution(PipelineContext context) {
        this(context, CompanionExecutionConfig.fromEnvironment());
    }

    public NoServerTestExecution(PipelineContext context, CompanionExecutionConfig config) {
        this(context, config, new JobContext(config.jobTtl()));
    }

    public NoServerTestExecution(PipelineContext context, CompanionExecutionConfig config, JobContext jobContext) {
        // context and jobContext are accepted for source compatibility with the real execution;
        // no real server is started, so only the config (for the reported port) is retained.
        this.config = config;
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Starting NoServerTestExecution");
        }
    }

    @Override
    public void startAsync() {
        if (running.compareAndSet(false, true)) {
            log.info("Starting NoServerTestExecution async");
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Stopping NoServerTestExecution");
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPort() {
        if (!running.get()) {
            return -1;
        }
        return config.port();
    }

    @Override
    public int getMonitoringPort() {
        return -1;
    }
}
