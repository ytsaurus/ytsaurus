package tech.ytsaurus.client.operations;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.ApiServiceClient;
import tech.ytsaurus.client.request.AbortOperation;
import tech.ytsaurus.client.request.GetJobStderr;
import tech.ytsaurus.client.request.GetOperation;
import tech.ytsaurus.client.request.JobResult;
import tech.ytsaurus.client.request.JobState;
import tech.ytsaurus.client.request.ListJobs;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.common.YTsaurusError;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeListNode;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;


@NonNullApi
@NonNullFields
public class OperationImpl implements Operation {
    private static final Logger logger = LoggerFactory.getLogger(OperationImpl.class);

    private final GUID id;
    private final ApiServiceClient client;
    private final ScheduledExecutorService executorService;
    private final Duration pingPeriod;
    private final CompletableFuture<Void> watchResult = new CompletableFuture<>();

    private Instant previousBriefProgressBuildTime;

    public OperationImpl(
            GUID id,
            ApiServiceClient client,
            ScheduledExecutorService executorService,
            Duration pingPeriod
    ) {
        this.id = id;
        this.client = client;
        this.executorService = executorService;
        this.pingPeriod = pingPeriod;
        this.previousBriefProgressBuildTime = Instant.now();
    }

    @Override
    public GUID getId() {
        return id;
    }

    @Override
    public CompletableFuture<OperationStatus> getStatus() {
        return getOperation("state")
                .thenApply(node -> OperationStatus.R.fromName(node.mapNode().getOrThrow("state").stringValue()));
    }

    @Override
    public CompletableFuture<YTreeNode> getResult() {
        return getOperation("result")
                .thenApply(node -> node.mapNode().getOrThrow("result"));
    }

    @Override
    public CompletableFuture<Void> watch() {
        executorService.schedule(this::watchImpl, pingPeriod.toNanos(), TimeUnit.NANOSECONDS);
        return watchResult;
    }

    @Override
    public CompletableFuture<Void> watchAndThrowIfNotSuccess() {
        return watch()
                .thenCompose(unused -> getStatus())
                .thenCompose(operationStatus -> {
                    if (operationStatus.isSuccess()) {
                        return CompletableFuture.completedFuture(null);
                    }

                    return getResult()
                            .thenAccept(operationResult -> {
                                Map<String, YTreeNode> error = Collections.emptyMap();
                                if (operationResult != null) {
                                    Map<String, YTreeNode> result = operationResult.asMap();
                                    if (result.containsKey("error")) {
                                        error = result.get("error").asMap();
                                    }
                                }

                                throw YTsaurusError.parseFrom(error);
                            });
                });
    }

    @Override
    public CompletableFuture<Void> abort() {
        return client.abortOperation(new AbortOperation(id));
    }

    private CompletableFuture<YTreeNode> getOperation(String attribute) {
        return client.getOperation(GetOperation.builder()
                .setOperationId(id)
                .addAttribute(attribute)
                .build());
    }

    private void watchImpl() {
        logger.debug("Operation's watch iteration was started (OperationId: {})", id);
        client.getOperation(GetOperation.builder()
                        .setOperationId(id)
                        .addAttribute("state")
                        .addAttribute("brief_progress")
                        .addAttribute("type")
                        .addAttribute("operation_type")
                        .build())
                .thenApply(this::getAndLogStatus)
                .thenCompose(status -> {
                    if (status.isFinished()) {
                        return getAndLogFailedJobs(id).handle((unused, ex) -> {
                            if (ex != null) {
                                logger.warn("Cannot get failed jobs info", ex);
                            }

                            watchResult.complete(null);
                            return null;
                        });
                    }
                    return CompletableFuture.completedFuture(null);
                }).handle((unused, ex) -> {
                    if (!watchResult.isDone()) {
                        executorService.schedule(this::watchImpl, pingPeriod.toNanos(), TimeUnit.NANOSECONDS);
                    }
                    return null;
                });
    }

    private OperationStatus getAndLogStatus(YTreeNode getOperationResult) {
        Map<String, YTreeNode> attrs = getOperationResult.asMap();
        String state = attrs.get("state").stringValue();
        OperationStatus status;
        try {
            status = OperationStatus.R.fromName(state);
        } catch (IllegalArgumentException e) {
            status = OperationStatus.UNKNOWN;
        }

        String statusDescription = state;
        if (attrs.containsKey("brief_progress") && attrs.get("brief_progress").mapNode().containsKey("jobs")) {
            YTreeMapNode briefProgress = attrs.get("brief_progress").mapNode();
            YTreeMapNode progress = briefProgress.getOrThrow("jobs").mapNode();
            Instant buildTime;
            try {
                buildTime = Instant.parse(briefProgress.getOrThrow("build_time").stringValue());
            } catch (DateTimeParseException e) {
                buildTime = previousBriefProgressBuildTime; // do not log unless received build_time
            }

            if (buildTime.compareTo(previousBriefProgressBuildTime) > 0 && progress.containsKey("total")) {
                StringBuilder sb = new StringBuilder();
                if (progress.containsKey("running")) {
                    sb.append("running ").append(progress.getOrThrow("running").longValue()).append(", ");
                }
                if (progress.containsKey("completed")) {
                    YTreeNode node = progress.getOrThrow("completed");
                    long count = 0;
                    if (node.isIntegerNode()) {
                        count = node.longValue();
                    } else if (node.isMapNode()) {
                        count = node.mapNode().getLong("total");
                    }
                    sb.append("completed ").append(count).append(", ");
                }
                sb.append("total ").append(progress.getOrThrow("total").longValue());
                sb.append(" (").append(state).append(")");
                statusDescription = sb.toString();
            }
            previousBriefProgressBuildTime = buildTime;
        }
        try {
            logger.info("Operation {} ({}): {}", id, attrs.get("type").stringValue(), statusDescription);
        } catch (Exception ex) {
            logger.info("Operation {}: {}", id, statusDescription);
        }
        return status;
    }

    private CompletableFuture<Void> getAndLogFailedJobs(GUID operationId) {
        return client.listJobs(ListJobs.builder()
                        .setOperationId(operationId)
                        .setState(JobState.Failed)
                        .setLimit(5L).build())
                .thenCompose(listJobsResult -> CompletableFuture.allOf(listJobsResult
                        .getJobs()
                        .stream()
                        .map(j -> getAndLogFailedJob(operationId, j))
                        .collect(Collectors.toList()).toArray(new CompletableFuture<?>[0])));
    }

    private CompletableFuture<Void> getAndLogFailedJob(GUID operationId, JobResult job) {
        FailedJobInfo failedJobInfo = new FailedJobInfo(job.getId());
        if (job.getError().isPresent()) {
            traverseInnerErrors(
                    job.getError().get().mapNode(),
                    errorNode -> failedJobInfo.addErrorMessage(errorNode.getString("message")));
        }

        return CompletableFuture.completedFuture(failedJobInfo).thenCompose(
                info -> client
                        .getJobStderr(new GetJobStderr(operationId, job.getId()))
                        .thenApply(getJobStderrResult -> {
                            if (getJobStderrResult.getStderr().isPresent()) {
                                failedJobInfo.setStderr(new String(getJobStderrResult.getStderr().get()));
                            }
                            return failedJobInfo;
                        })
                        .handle((result, ex) -> {
                            if (ex != null) {
                                logger.error("Failed to fetch job details: {}, exception: {}", job.getId(), ex);
                            } else {
                                logger.error(result.toString());
                            }
                            return null;
                        })
        );
    }

    void traverseInnerErrors(YTreeMapNode errorNode, Consumer<YTreeMapNode> errorNodeConsumer) {
        errorNodeConsumer.accept(errorNode);

        Optional<YTreeNode> innerErrors = errorNode.get("inner_errors");
        if (innerErrors.isPresent()) {
            YTreeNode node = innerErrors.get();
            if (node instanceof YTreeListNode) {
                for (YTreeNode innerError : node.asList()) {
                    traverseInnerErrors(innerError.mapNode(), errorNodeConsumer);
                }
            }
        }
    }
}
