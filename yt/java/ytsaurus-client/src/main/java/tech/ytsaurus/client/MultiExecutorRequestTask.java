package tech.ytsaurus.client;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;


public class MultiExecutorRequestTask<R> {

    private final AtomicReference<Status> status = new AtomicReference<>(Status.IN_PROGRESS);
    private final AtomicInteger finishedClientsCount = new AtomicInteger(0);
    private final Function<BaseYTsaurusClient, CompletableFuture<R>> callback;
    private final MultiExecutorMonitoring executorMonitoring;
    private final CompletableFuture<R> requestFuture = new CompletableFuture<>();
    private final List<DelayedSubrequestTask> delayedSubrequestTasks;
    private final Instant requestStartTime = Instant.now();
    private final AtomicBoolean terminateFlag = new AtomicBoolean(false);

    public MultiExecutorRequestTask(
            List<ClientEntry> clients,
            Function<BaseYTsaurusClient, CompletableFuture<R>> callback,
            MultiExecutorMonitoring executorMonitoring
    ) {
        this.callback = callback;
        this.executorMonitoring = executorMonitoring;
        this.delayedSubrequestTasks = clients.stream().map((clientEntry) -> new DelayedSubrequestTask(
                clientEntry
        )).collect(Collectors.toUnmodifiableList());

        requestFuture.whenComplete((value, error) -> {

            terminateFlag.set(true);

            if (error != null && status.compareAndSet(Status.IN_PROGRESS, Status.CANCELED)) {
                for (DelayedSubrequestTask subrequestTask : delayedSubrequestTasks) {
                    subrequestTask.cancel(error);
                }
                executorMonitoring.onRequestFailure(Duration.between(requestStartTime, Instant.now()), error);
            }

            if (status.get().equals(Status.COMPLETED)) {
                for (DelayedSubrequestTask subrequestTask : delayedSubrequestTasks) {
                    subrequestTask.suppress();
                }
            }
        });
    }

    CompletableFuture<R> getFuture() {
        return requestFuture;
    }

    /**
     * IN_PROGRESS -> COMPLETED or CANCELED
     */
    enum Status {
        IN_PROGRESS,
        COMPLETED, // Normally or exceptionally.
        CANCELED // When the whole task is canceled.
    }

    public static class ClientEntry {
        private final MultiYTsaurusClient.YTsaurusClientOptions clientOptions;
        private final Duration effectivePenalty;
        private final Consumer<Boolean> finishRequestConsumer;

        public ClientEntry(
                MultiYTsaurusClient.YTsaurusClientOptions clientOptions,
                Duration effectivePenalty,
                Consumer<Boolean> finishRequestConsumer
        ) {
            this.clientOptions = clientOptions;
            this.effectivePenalty = effectivePenalty;
            this.finishRequestConsumer = finishRequestConsumer;
        }

        public void onFinishRequest(Boolean success) {
            this.finishRequestConsumer.accept(success);
        }
    }

    private class DelayedSubrequestTask {

        private final ClientEntry clientEntry;
        private volatile @Nullable SubrequestDescriptor subrequestDescriptor = null;
        private final ScheduledFuture<CompletableFuture<?>> scheduledFuture;

        DelayedSubrequestTask(ClientEntry clientEntry) {
            this.clientEntry = clientEntry;
            this.scheduledFuture = clientEntry.clientOptions.client.getExecutor().schedule(
                    () -> {
                        if (terminateFlag.get()) {
                            return CompletableFuture.completedFuture(CompletableFuture.completedFuture(null));
                        }

                        SubrequestDescriptor localSubrequestDescriptor = new SubrequestDescriptor(
                                Instant.now(),
                                callback.apply(clientEntry.clientOptions.client)
                        );

                        this.subrequestDescriptor = localSubrequestDescriptor;
                        executorMonitoring.onSubrequestStart(clientEntry.clientOptions.getClusterName());

                        return localSubrequestDescriptor.callbackFuture.handle((value, error) -> {
                            Instant now = Instant.now();
                            Duration requestCompletionTime = Duration.between(requestStartTime, now);
                            Duration subrequestCompletionTime = Duration.between(
                                    localSubrequestDescriptor.startTime, now
                            );

                            boolean isLastTask =
                                    finishedClientsCount.incrementAndGet() == delayedSubrequestTasks.size();

                            if (terminateFlag.get()) {
                                return null;
                            }

                            if (localSubrequestDescriptor.reportedToMonitoring.compareAndSet(false, true)) {
                                if (error == null) {
                                    executorMonitoring.onSubrequestSuccess(
                                            clientEntry.clientOptions.getClusterName(),
                                            subrequestCompletionTime
                                    );
                                } else {
                                    executorMonitoring.onSubrequestFailure(
                                            clientEntry.clientOptions.getClusterName(),
                                            subrequestCompletionTime,
                                            error
                                    );
                                }
                            }

                            if ((error == null || isLastTask) &&
                                    status.compareAndSet(Status.IN_PROGRESS, Status.COMPLETED)) {
                                if (error == null) {
                                    requestFuture.complete(value);
                                    executorMonitoring.onRequestSuccess(
                                            clientEntry.clientOptions.getClusterName(),
                                            requestCompletionTime
                                    );
                                } else {
                                    requestFuture.completeExceptionally(error);
                                    executorMonitoring.onRequestFailure(
                                            requestCompletionTime,
                                            error
                                    );
                                }
                            }

                            if (!status.get().equals(Status.CANCELED)) {
                                clientEntry.onFinishRequest(error == null);
                            }
                            return null;
                        });
                    },
                    clientEntry.effectivePenalty.toMillis(),
                    TimeUnit.MILLISECONDS
            );
        }

        /**
         * Report that result of this subrequest is no longer needed.
         */
        void suppress() {
            cancel(null);
        }

        /**
         * Cancel execution of subrequest task.
         *
         * @param throwable if not null, throwable is treated as a foreign exception because
         *                  of which the cancelation takes place. if throwable is null, it means
         *                  that another subrequest has succeeded and a suppression of
         *                  this subrequest task is needed.
         */
        void cancel(@Nullable Throwable throwable) {
            SubrequestDescriptor localSubrequestDescriptor = this.subrequestDescriptor;
            if (localSubrequestDescriptor == null) {
                scheduledFuture.cancel(false);
                return;
            }
            localSubrequestDescriptor.callbackFuture.cancel(false);
            if (localSubrequestDescriptor.reportedToMonitoring.compareAndSet(false, true)) {
                Duration completionTime = Duration.between(localSubrequestDescriptor.startTime, Instant.now());
                if (throwable == null) {
                    executorMonitoring.onSubrequestCancelation(
                            clientEntry.clientOptions.getClusterName(),
                            completionTime
                    );
                } else {
                    executorMonitoring.onSubrequestFailure(
                            clientEntry.clientOptions.getClusterName(),
                            completionTime,
                            throwable
                    );
                }
            }
        }

        private class SubrequestDescriptor {
            Instant startTime;
            CompletableFuture<R> callbackFuture;
            AtomicBoolean reportedToMonitoring = new AtomicBoolean(false);

            SubrequestDescriptor(Instant startTime, CompletableFuture<R> callbackFuture) {
                this.startTime = startTime;
                this.callbackFuture = callbackFuture;
            }
        }

    }

}
