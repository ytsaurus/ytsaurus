package tech.ytsaurus.flow.execution;

import java.util.HashMap;
import java.util.Map;

import com.sun.net.httpserver.HttpHandler;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.flow.config.CompanionExecutionConfig;
import tech.ytsaurus.flow.context.PipelineContext;
import tech.ytsaurus.flow.job.JobContext;

/**
 * Mutable configuration-phase holder for a {@link GrpcServerExecution}.
 *
 * <p>This is a plain bean-style "parameter object": it gathers everything needed to construct
 * an execution — the pipeline context, the {@link CompanionExecutionConfig}, the optional
 * {@link JobContext}, and the monitoring HTTP handlers.
 *
 * <pre>
 * var spec = new CompanionExecutionSpec(context)
 *         .setConfig(config)
 *         .addMonitoringHttpHandler(handler);
 * var execution = new GrpcServerExecution(spec);
 * </pre>
 *
 * <p>The spec is intended to be configured single-threaded during setup; it is not thread-safe.
 */
public final class CompanionExecutionSpec {

    /**
     * Default HTTP path for the metrics endpoint exposed by the monitoring server.
     */
    static final String MONITORING_HTTP_PATH = "/metrics";

    private final PipelineContext pipelineContext;
    private @Nullable CompanionExecutionConfig config;
    private @Nullable JobContext jobContext;
    private final Map<String, HttpHandler> httpHandlers = new HashMap<>();

    /**
     * Creates a spec bound to the given pipeline context. The {@link CompanionExecutionConfig}
     * defaults to {@link CompanionExecutionConfig#fromEnvironment()} (resolved by the execution)
     * unless set with {@link #setConfig(CompanionExecutionConfig)}.
     *
     * @param pipelineContext the pipeline context to use for execution.
     */
    public CompanionExecutionSpec(PipelineContext pipelineContext) {
        this.pipelineContext = pipelineContext;
    }

    /**
     * @return the pipeline context.
     */
    public PipelineContext getPipelineContext() {
        return pipelineContext;
    }

    /**
     * @return the companion execution configuration, or {@code null} if not set.
     */
    public @Nullable CompanionExecutionConfig getConfig() {
        return config;
    }

    /**
     * Sets the companion execution configuration.
     *
     * @param config the configuration for the gRPC server.
     * @return this spec.
     */
    public CompanionExecutionSpec setConfig(CompanionExecutionConfig config) {
        this.config = config;
        return this;
    }

    /**
     * @return the job context, or {@code null} if not set.
     */
    public @Nullable JobContext getJobContext() {
        return jobContext;
    }

    /**
     * Sets the job context. When not set, the execution creates a {@link JobContext} from the
     * configuration's job TTL.
     *
     * @param jobContext the job context to use for execution.
     * @return this spec.
     */
    public CompanionExecutionSpec setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
        return this;
    }

    /**
     * @return an immutable snapshot of the registered HTTP handlers by path.
     */
    public Map<String, HttpHandler> getHttpHandlers() {
        return Map.copyOf(httpHandlers);
    }

    /**
     * Registers an HTTP handler for the default monitoring path ({@code /metrics}).
     *
     * @param handler the HTTP handler to be served at the monitoring endpoint.
     * @return this spec.
     * @throws IllegalArgumentException if a handler for the monitoring path already exists.
     */
    public CompanionExecutionSpec addMonitoringHttpHandler(HttpHandler handler) {
        return addHttpHandler(MONITORING_HTTP_PATH, handler);
    }

    /**
     * Registers an HTTP handler for the specified path.
     *
     * @param path    the path for which the handler is registered.
     * @param handler the HTTP handler to be served at the path.
     * @return this spec.
     * @throws IllegalArgumentException if a handler for the specified path already exists.
     */
    CompanionExecutionSpec addHttpHandler(String path, HttpHandler handler) {
        if (httpHandlers.putIfAbsent(path, handler) != null) {
            throw new IllegalArgumentException("Handler for path " + path + " already exists");
        }
        return this;
    }
}
