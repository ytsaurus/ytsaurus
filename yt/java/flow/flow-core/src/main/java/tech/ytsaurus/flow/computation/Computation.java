package tech.ytsaurus.flow.computation;

import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.context.DefaultRuntimeContext;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.BatchFunction;
import tech.ytsaurus.flow.function.FlowFunction;
import tech.ytsaurus.flow.function.ProcessFunction;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.internal.computation.RootCollector;
import tech.ytsaurus.flow.request.RequestContext;
import tech.ytsaurus.flow.request.ResponseContext;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Timer;
import tech.ytsaurus.flow.row.Visit;
import tech.ytsaurus.flow.state.DefaultStateBackend;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeConvertible;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Basic building block of Flow Pipeline.
 * <p>
 * Computation represents a vertex at pipeline execution graph.
 */
public class Computation implements YTreeConvertible {
    private static final Logger log = LoggerFactory.getLogger(Computation.class);

    /**
     * Computation id from pipeline static spec.
     * <p>
     * It is used for matching requests c++ computation at worker side to particular java computation at companion side.
     */
    private final String computationId;

    /**
     * User-provided function applied to incoming messages.
     * <p>
     * Must always be set.
     */
    private final ProcessFunction processFunction;

    Computation(Builder<?> builder) {
        this.computationId = Objects.requireNonNull(builder.computationId);
        if (builder.processFunction == null) {
            throw new IllegalArgumentException("Process function must be set");
        }
        this.processFunction = builder.processFunction;
    }

    /**
     * @return Builder for Computation.
     */
    public static Builder<?> builder() {
        return new Builder<>();
    }

    /**
     * Processes the incoming request by executing the computation logic and generating a response.
     * This method orchestrates the entire computation flow, including initializing the process function,
     * processing messages, collecting results, and constructing the response.
     *
     * @param request the incoming request context containing messages and stream information
     * @return the response context containing the results of the computation
     * @throws Exception if an error occurs during processing
     */
    public final ResponseContext doProcess(RequestContext request) throws Exception {
        var jobId = request.getJobId();
        var requestId = request.getRequestId();
        var job = request.getJob();
        log.debug(
                "Start doProcess: (ComputationId: {}, MessagesSize: {}, TimersSize: {}, VisitsSize: {}, " +
                        "JobId: {}, RequestId: {})",
                computationId,
                request.getMessages().size(),
                request.getTimers().size(),
                request.getVisits().size(),
                jobId,
                requestId);
        var rootCollector = new RootCollector();

        // Initialize runtime context.
        var stateBackend = new DefaultStateBackend(
                job.getInternalStatesNames(),
                job.getExternalStatesNames(),
                job.getExternalStateJoinersNames(),
                request.getInternalStates(),
                request.getExternalStates(),
                request.getJoinedExternalStates(),
                job.getGroupBySchema()
        );
        var runtimeContext = new DefaultRuntimeContext(
                stateBackend,
                request.getStreamSpecs(),
                request.getWatermarks(),
                request.getMinWatermark(),
                job.getStaticParameters(),
                job.getDynamicParameters()
        );
        log.debug(
                "States (InternalStateNames: {}, ExternalStateNames: {} InternalStatesSize: {}, " +
                        "ExternalStatesSize: {}, JobId: {}, RequestId: {})",
                job.getInternalStatesNames(),
                job.getExternalStatesNames(),
                request.getInternalStates().size(),
                request.getExternalStates().size(),
                jobId,
                requestId);
        // Message processing.
        doProcessMessages(
                request.getMessages(),
                request.getTimers(),
                request.getVisits(),
                rootCollector,
                runtimeContext);
        // Construct response.
        var response = new ResponseContext(
                jobId,
                requestId,
                rootCollector.collectResults(),
                request.getInternalStates(),
                request.getExternalStates()
        );
        log.debug(
                "doProcess finished (TransformResultsSize: {}, InternalStatesSize: {}, ExternalStatesSize: {}," +
                        " JobId: {}, RequestId: {})",
                response.getTransformResults().size(),
                response.getInternalStates().size(),
                response.getExternalStates().size(),
                jobId,
                requestId
        );
        return response;
    }

    /**
     * Processes a list of messages using the process function and collects the results into the root collector.
     * This method might be overridden by subclasses to implement some complicated message processing logic.
     *
     * @param messages      The list of messages to process.
     * @param timers        The list of timers to process.
     * @param rootCollector The root collector to collect the results.
     * @param ctx           The runtime context.
     */
    protected void doProcessMessages(
            List<ExtendedMessage> messages,
            List<Timer> timers,
            List<Visit> visits,
            RootCollector rootCollector,
            RuntimeContext ctx
    ) {
        switch (processFunction) {
            case RowFunction rowFunction -> {
                // Process messages.
                for (ExtendedMessage message : messages) {
                    rowFunction.onMessage(
                            message,
                            rootCollector.setParentIds(message.getMessageId()),
                            ctx
                    );
                }
                // Process timers.
                for (Timer timer : timers) {
                    rowFunction.onTimer(
                            timer,
                            rootCollector.setParentIds(timer.getMessageId()),
                            ctx
                    );
                }
                // Process visits.
                for (Visit visit : visits) {
                    rowFunction.onVisit(
                            visit,
                            rootCollector.setParentIds(visit.getMessageId()),
                            ctx
                    );
                }
            }
            case BatchFunction batchFunction -> {
                // Process messages.
                if (!messages.isEmpty()) {
                    List<String> parentIds = messages.stream()
                            .map(ExtendedMessage::getMessageId)
                            .toList();
                    batchFunction.onMessages(
                            messages,
                            rootCollector.setParentIds(parentIds),
                            ctx
                    );
                }
                // Process timers.
                if (!timers.isEmpty()) {
                    List<String> timerParentIds = timers.stream()
                            .map(Timer::getMessageId)
                            .toList();
                    batchFunction.onTimers(
                            timers,
                            rootCollector.setParentIds(timerParentIds),
                            ctx
                    );
                }
                // Process visits.
                if (!visits.isEmpty()) {
                    List<String> visitParentIds = visits.stream()
                            .map(Visit::getMessageId)
                            .toList();
                    batchFunction.onVisits(
                            visits,
                            rootCollector.setParentIds(visitParentIds),
                            ctx
                    );
                }
            }
        }
    }

    public String getComputationId() {
        return computationId;
    }

    public FlowFunction getProcessFunction() {
        return processFunction;
    }

    public ComputationType getComputationType() {
        return ComputationType.Transform;
    }

    @Override
    public String toString() {
        return toYTree().toString();
    }

    @Override
    public YTreeNode toYTree() {
        YTreeBuilder builder = YTree.builder()
                .beginMap()
                .key("computation_id").value(computationId)
                .key("computation_type").value(getComputationType().toString());
        return builder.endMap().build();
    }

    /**
     * Builder for {@link Computation}.
     */
    public static class Builder<B extends Computation.Builder<B>> {
        private @Nullable String computationId;
        private @Nullable ProcessFunction processFunction;

        Builder() {
        }

        @SuppressWarnings("unchecked")
        protected B self() {
            return (B) this;
        }

        public B setComputationId(String computationId) {
            this.computationId = computationId;
            return self();
        }

        public B setProcessFunction(ProcessFunction processFunction) {
            this.processFunction = processFunction;
            return self();
        }

        public Computation build() {
            return new Computation(this);
        }
    }
}
