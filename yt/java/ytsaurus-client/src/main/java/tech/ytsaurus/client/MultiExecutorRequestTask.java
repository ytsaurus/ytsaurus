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
        )).toList();

        requestFuture.whenComplete((value, error) -> {

            terminateFlag.set(true);

            if (error != null && status.compareAndSet(Status.IN_PROGRESS, Status.CANCELLED)) {
                for (DelayedSubrequestTask subrequestTask : delayedSubrequestTasks) {
                    subrequestTask.cancel(error);
                }
                executorMonitoring.reportRequestFailure(Duration.between(requestStartTime, Instant.now()), error);
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
     * IN_PROGRESS -> COMPLETED or CANCELLED
     */
    enum Status {
        IN_PROGRESS,
        COMPLETED, // normally or exceptionally
        CANCELLED // when the whole task is cancelled
    }

    public static class ClientEntry {
        private final MultiYTsaurusClient.YTsaurusClientOptions clientOptions;
        private final Duration effectivePenalty;
        private final Consumer<Boolean> completionConsumer;

        public ClientEntry(
                MultiYTsaurusClient.YTsaurusClientOptions clientOptions,
                Duration effectivePenalty,
                Consumer<Boolean> completionConsumer
        ) {
            this.clientOptions = clientOptions;
            this.effectivePenalty = effectivePenalty;
            this.completionConsumer = completionConsumer;
        }

        public void reportComplete(Boolean success) {
            this.completionConsumer.accept(success);
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
                        executorMonitoring.reportSubrequestStart(clientEntry.clientOptions.getClusterName());

                        return localSubrequestDescriptor.callbackFuture.handle((value, error) -> {

                            Duration requestCompletionTime = Duration.between(requestStartTime, Instant.now());
                            Duration subrequestCompletionTime =
                                    Duration.between(localSubrequestDescriptor.startTime, Instant.now());
                            boolean isLastTask =
                                    finishedClientsCount.incrementAndGet() == delayedSubrequestTasks.size();

                            if (terminateFlag.get()) {
                                return null;
                            }

                            if (localSubrequestDescriptor.reportedToMonitoring.compareAndSet(false, true)) {
                                if (error == null) {
                                    executorMonitoring.reportSubrequestSuccess(
                                            clientEntry.clientOptions.getClusterName(),
                                            subrequestCompletionTime
                                    );
                                } else {
                                    executorMonitoring.reportSubrequestFailure(
                                            clientEntry.clientOptions.getClusterName(),
                                            subrequestCompletionTime,
                                            error
                                    );
                                }
                            }

                            if ((error == null || isLastTask) &&
                                    status.compareAndSet(Status.IN_PROGRESS, Status.COMPLETED)) {
                                clientEntry.reportComplete(error == null);
                                if (error == null) {
                                    requestFuture.complete(value);
                                    executorMonitoring.reportRequestSuccess(
                                            clientEntry.clientOptions.getClusterName(),
                                            requestCompletionTime
                                    );
                                } else {
                                    requestFuture.completeExceptionally(error);
                                    executorMonitoring.reportRequestFailure(
                                            requestCompletionTime,
                                            error
                                    );
                                }
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
         *                  of which the cancellation takes place. if throwable is null, it means
         *                  that another subrequest has succeeded and a suppression of
         *                  this subrequest task is needed.
         */
        void cancel(@Nullable Throwable throwable) {
            SubrequestDescriptor localSubrequestDescriptor = this.subrequestDescriptor;
            if (localSubrequestDescriptor != null) {
                localSubrequestDescriptor.callbackFuture.cancel(false);
                if (localSubrequestDescriptor.reportedToMonitoring.compareAndSet(false, true)) {
                    Duration completionTime = Duration.between(localSubrequestDescriptor.startTime, Instant.now());
                    if (throwable == null) {
                        executorMonitoring.reportSubrequestCancelled(
                                clientEntry.clientOptions.getClusterName(),
                                completionTime
                        );
                    } else {
                        executorMonitoring.reportSubrequestFailure(
                                clientEntry.clientOptions.getClusterName(),
                                completionTime,
                                throwable
                        );
                    }
                }
            }
        }

        private class SubrequestDescriptor {
            public Instant startTime;
            public CompletableFuture<R> callbackFuture;
            public AtomicBoolean reportedToMonitoring = new AtomicBoolean(false);

            public SubrequestDescriptor(Instant startTime, CompletableFuture<R> callbackFuture) {
                this.startTime = startTime;
                this.callbackFuture = callbackFuture;
            }
        }

    }

}
