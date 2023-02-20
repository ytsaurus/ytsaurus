package tech.ytsaurus.client;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import tech.ytsaurus.client.request.AbstractLookupRowsRequest;
import tech.ytsaurus.client.request.SelectRowsRequest;
import tech.ytsaurus.client.rows.ConsumerSource;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.client.rows.VersionedRowset;
import tech.ytsaurus.core.rows.YTreeRowSerializer;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * YT Client for requests in several clusters
 * Example:
 * ```
 *   MultiYtClient multiClient = MultiYtClient.builder()
 *     .addPreferredCluster("hahn")
 *     .addCluster("arnold")
 *     .build();
 *   multiClient.lookupRows(...).join();
 * ```
 *
 * How it works:
 * MultiYtClient contains several YtClients. Each client corresponds to one cluster.
 * In some clusters, the request is made immediately (at least in one), and in some with some delay.
 * When a response is received from one of the clusters, the pending request is canceled.
 * The delay increases when errors occur. If there were no errors for some time, then the delay decreases.
 *
 * Some names:
 *   preferredCluster - such a cluster has zero minimum initial delay. It can be increased if errors occur.
 *   preferredAllowance - initial delay for non-preferred clusters.
 */
@NonNullFields
@NonNullApi
public class MultiYtClient implements ImmutableTransactionalClient, Closeable {
    final MultiExecutor executor;

    private MultiYtClient(Builder builder) {
        executor = new MultiExecutor(builder);
    }

    @Override
    public void close() {
        try {
            for (MultiExecutor.YtClientEntry entry : executor.clients) {
                entry.client.close();
            }
            executor.close();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Create MultiYtClient builder
     * @return Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public CompletableFuture<UnversionedRowset> lookupRows(AbstractLookupRowsRequest<?, ?> request) {
        return executor.execute((client) -> client.lookupRows(request));
    }

    @Override
    public <T> CompletableFuture<List<T>> lookupRows(
            AbstractLookupRowsRequest<?, ?> request,
            YTreeRowSerializer<T> serializer
    ) {
        return executor.execute((client) -> client.lookupRows(request, serializer));
    }

    @Override
    public CompletableFuture<VersionedRowset> versionedLookupRows(AbstractLookupRowsRequest<?, ?> request) {
        return executor.execute((client) -> client.versionedLookupRows(request));
    }

    @Override
    public CompletableFuture<SelectRowsResult> selectRowsV2(SelectRowsRequest request) {
        return executor.execute((client) -> client.selectRowsV2(request));
    }

    @Override
    public CompletableFuture<UnversionedRowset> selectRows(SelectRowsRequest request) {
        return executor.execute((client) -> client.selectRows(request));
    }

    @Override
    public <T> CompletableFuture<List<T>> selectRows(
            SelectRowsRequest request,
            YTreeRowSerializer<T> serializer
    ) {
        return executor.execute((client) -> client.selectRows(request, serializer));
    }

    @Override
    public <T> CompletableFuture<Void> selectRows(SelectRowsRequest request, YTreeRowSerializer<T> serializer,
                                                  ConsumerSource<T> consumer) {
        return executor.execute((client) -> client.selectRows(request, serializer, consumer));
    }

    /**
     * Yt client with initial penalty.
     * Initial penalty is needed to prioritize clusters.
     */
    public static class YtClientOptions {
        final BaseYtClient client;
        final Duration initialPenalty;

        YtClientOptions(Builder builder) {
            this.client = builder.client;
            this.initialPenalty = builder.initialPenalty;
        }

        public static Builder builder(BaseYtClient client) {
            return new Builder(client);
        }

        public static class Builder {
            final BaseYtClient client;
            Duration initialPenalty = Duration.ZERO;

            public Builder(BaseYtClient client) {
                this.client = client;
            }

            /**
             * Set initial penalty for this client.
             * @return self
             */
            public Builder setInitialPenalty(Duration initialPenalty) {
                this.initialPenalty = initialPenalty;
                return this;
            }

            public YtClientOptions build() {
                return new YtClientOptions(this);
            }
        }
    }

    @NonNullApi
    @NonNullFields
    public static class Builder extends YTsaurusClient.BaseBuilder<MultiYtClient, Builder> {
        List<YtClientOptions> clientsOptions = new ArrayList<>();
        List<YtClient> clients = new ArrayList<>();
        List<String> clusters = new ArrayList<>();
        List<String> preferredClusters = new ArrayList<>();

        Duration banPenalty = Duration.ofMillis(1);
        Duration banDuration = Duration.ofMillis(50);
        PenaltyProvider penaltyProvider = PenaltyProvider.dummyPenaltyProviderBuilder().build();
        Duration preferredAllowance = Duration.ofMillis(100);

        Builder() {
        }

        public MultiYtClient build() {
            return new MultiYtClient(this);
        }

        /**
         * Add one more client with penalty. Client must be for only one cluster.
         * @return self
         */
        public Builder addClient(YtClientOptions clientOptions) {
            this.clientsOptions.add(clientOptions);
            return this;
        }

        /**
         * Add more clients with penalties. Each client must be for only one cluster.
         * @return self
         */
        public Builder addClients(YtClientOptions first, YtClientOptions... rest) {
            addClient(first);
            for (YtClientOptions clientOptions : rest) {
                addClient(clientOptions);
            }
            return this;
        }

        /**
         * Add one more client. Client must be for only one cluster.
         * @return self
         */
        public Builder addClient(YtClient client) {
            this.clients.add(client);
            return this;
        }

        /**
         * Add more clients. Each client must be for only one cluster.
         * @return self
         */
        public Builder addClients(YtClient first, YtClient... rest) {
            addClient(first);
            for (YtClient client : rest) {
                addClient(client);
            }
            return this;
        }

        /**
         * Add usual cluster (for requests without preferredAllowance).
         * @return self
         */
        public Builder addCluster(String cluster) {
            this.clusters.add(cluster);
            return this;
        }

        /**
         * Add preferred cluster (for requests with preferredAllowance).
         * @return self
         */
        public Builder addPreferredCluster(String cluster) {
            this.preferredClusters.add(cluster);
            return this;
        }

        /**
         * Set a penalty that will be assigned in case of errors.
         * @return self
         */
        public Builder setBanPenalty(Duration banPenalty) {
            this.banPenalty = banPenalty;
            return this;
        }

        /**
         * Set the duration after which the error penalty can be reset (if there are no more errors).
         * @return self
         */
        public Builder setBanDuration(Duration banDuration) {
            this.banDuration = banDuration;
            return this;
        }

        /**
         * Set duration between requests in usual and preferred clusters.
         * @return self
         */
        public Builder setPreferredAllowance(Duration preferredAllowance) {
            this.preferredAllowance = preferredAllowance;
            return this;
        }

        /**
         * Set penalty provider for assigning additional penalties depended on external events,
         * for instance, if replica is `lagged`.
         * @return self
         */
        public Builder setPenaltyProvider(PenaltyProvider penaltyProvider) {
            this.penaltyProvider = penaltyProvider;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}

@NonNullApi
@NonNullFields
class MultiExecutor implements Closeable {
    final Duration banPenalty;
    final Duration banDuration;
    final PenaltyProvider penaltyProvider;
    final List<YtClientEntry> clients;

    static final CancellationException CANCELLATION_EXCEPTION = new CancellationException();
    static final Duration MAX_DURATION = ChronoUnit.FOREVER.getDuration();

    MultiExecutor(MultiYtClient.Builder builder) {
        if (builder.clientsOptions.isEmpty() &&
                builder.clusters.isEmpty() &&
                builder.clients.isEmpty() &&
                builder.preferredClusters.isEmpty()
        ) {
            throw new IllegalArgumentException("No clients and no clusters in MultiYtClient's constructor");
        }

        this.banPenalty = builder.banPenalty;
        this.banDuration = builder.banDuration;
        this.penaltyProvider = builder.penaltyProvider;

        this.clients = new ArrayList<>();

        if (!builder.clientsOptions.isEmpty()) {
            addClients(builder.clientsOptions);
        }

        if (!builder.clients.isEmpty()) {
            List<MultiYtClient.YtClientOptions> clientOptions =
                    builder.clients.stream()
                            .map(client -> MultiYtClient.YtClientOptions.builder(client)
                                    .setInitialPenalty(builder.preferredAllowance)
                                    .build())
                            .collect(Collectors.toList());
            addClients(clientOptions);
        }

        if (!builder.preferredClusters.isEmpty()) {
            List<YtClient> clientsFromClusters = createClientsFromClusters(builder.preferredClusters, builder);
            List<MultiYtClient.YtClientOptions> clientOptions =
                    clientsFromClusters.stream()
                            .map(client -> MultiYtClient.YtClientOptions.builder(client).build())
                            .collect(Collectors.toList());
            addClients(clientOptions);
        }

        if (!builder.clusters.isEmpty()) {
            List<YtClient> clientsFromClusters = createClientsFromClusters(builder.clusters, builder);
            List<MultiYtClient.YtClientOptions> clientOptions = clientsFromClusters.stream()
                    .map(client -> MultiYtClient.YtClientOptions.builder(client)
                            .setInitialPenalty(builder.preferredAllowance)
                            .build())
                    .collect(Collectors.toList());
            addClients(clientOptions);
        }

        if (this.clients.size() < 2) {
            throw new IllegalArgumentException("Count of clients is less than 2");
        }

        HashSet<String> clusters = new HashSet<>();
        for (YtClientEntry client : this.clients) {
            String clusterName = client.getClusterName();
            if (clusters.contains(clusterName)) {
                throw new IllegalArgumentException("Got more than one clients for cluster: '" + clusterName + "'");
            }
            clusters.add(clusterName);
        }
    }

    private void addClients(List<MultiYtClient.YtClientOptions> clientOptions) {
        for (MultiYtClient.YtClientOptions options : clientOptions) {
            this.clients.add(new YtClientEntry(options));
        }
    }

    private static List<YtClient> createClientsFromClusters(
            List<String> clusters, MultiYtClient.Builder builder) {
        List<YtClient> result = new ArrayList<>();
        for (String cluster : clusters) {
            YtClient.Builder clientBuilder = YtClient.builder()
                    .setClusters(cluster)
                    .setYtClientConfiguration(builder.configuration)
                    .setRpcCompression(builder.compression);
            if (builder.auth != null) {
                clientBuilder.setAuth(builder.auth);
            }
            result.add(clientBuilder.build());
        }
        return result;
    }

    private List<YtClientEntry> updateAdaptivePenalty(Instant now) {
        List<YtClientEntry> currentClients = new ArrayList<>(clients.size());

        synchronized (this) {
            for (YtClientEntry client : clients) {
                if (client.banUntil != null && client.banUntil.compareTo(now) < 0) {
                    client.adaptivePenalty = Duration.ZERO;
                }
                currentClients.add(new YtClientEntry(client));
            }

            return currentClients;
        }
    }

    private Duration getMinPenalty(List<YtClientEntry> clients) {
        Duration minPenalty = MAX_DURATION;
        for (YtClientEntry client : clients) {
            String shortClusterName = client.getClusterName().split("\\.")[0];
            client.externalPenalty = penaltyProvider.getPenalty(shortClusterName);
            Duration currentPenalty = client.getPenalty();
            if (currentPenalty.compareTo(minPenalty) < 0) {
                minPenalty = currentPenalty;
            }
        }
        return minPenalty;
    }

    private void onFinishRequest(
            int clientId,
            Duration effectivePenalty,
            Duration adaptivePenalty,
            @Nullable Throwable error
    ) {
        YtClientEntry client = clients.get(clientId);
        boolean isCancel = (effectivePenalty.compareTo(Duration.ZERO) > 0 && (error instanceof CancellationException));

        if (error == null) {
            if (adaptivePenalty.compareTo(Duration.ZERO) > 0) {
                synchronized (this) {
                    client.banUntil = null;
                    client.adaptivePenalty = Duration.ZERO;
                }
            }
        } else if (!isCancel) {
            synchronized (this) {
                client.banUntil = Instant.now().plus(banDuration);
                client.adaptivePenalty = client.adaptivePenalty.plus(banPenalty);
            }
        }
    }

    <R> CompletableFuture<R> execute(Function<BaseYtClient, CompletableFuture<R>> callback) {
        Instant now = Instant.now();
        List<YtClientEntry> currentClients = updateAdaptivePenalty(now);
        Duration minPenalty = getMinPenalty(currentClients);
        List<DelayedTask<R>> delayedTasks = new ArrayList<>();
        CompletableFuture<R> result = new CompletableFuture<>();

        AtomicInteger finishedClientsCount = new AtomicInteger(0);

        for (int clientId = 0; clientId < currentClients.size(); ++clientId) {
            YtClientEntry client = currentClients.get(clientId);
            Duration effectivePenalty = client.getPenalty().minus(minPenalty);

            CompletableFuture<R> future;
            if (effectivePenalty.isZero()) {
                future = callback.apply(client.client);
            } else {
                future = new CompletableFuture<>();
                ScheduledFuture<?> task = client.client.getExecutor().schedule(
                        () -> callback.apply(client.client)
                                .handle((value, ex) -> {
                                    if (ex == null) {
                                        future.complete(value);
                                    } else {
                                        future.completeExceptionally(ex);
                                    }
                                    return null;
                                }),
                        effectivePenalty.toMillis(),
                        TimeUnit.MILLISECONDS);
                delayedTasks.add(new DelayedTask<>(task, future, clientId, effectivePenalty, client, now));
            }

            int copyClientId = clientId;
            future.whenComplete((value, error) -> {
                if (error == null) {
                    result.complete(value);
                } else if (finishedClientsCount.incrementAndGet() == currentClients.size()) {
                    result.completeExceptionally(error);
                }

                onFinishRequest(copyClientId, effectivePenalty, client.adaptivePenalty, error);
            });
        }

        return result.whenComplete((unused, ex) -> {
            for (DelayedTask<R> delayedTask : delayedTasks) {
                delayedTask.cancel();
            }
        });
    }

    @Override
    public void close() throws IOException {
        penaltyProvider.close();
    }

    class DelayedTask<R> {
        final ScheduledFuture<?> task;
        final CompletableFuture<R> result;
        final int clientId;
        final Duration effectivePenalty;
        final YtClientEntry client;

        DelayedTask(ScheduledFuture<?> task, CompletableFuture<R> result,
                    int clientId, Duration effectivePenalty, YtClientEntry client, Instant start) {
            this.task = task;
            this.result = result;
            this.clientId = clientId;
            this.effectivePenalty = effectivePenalty;
            this.client = client;
        }

        void cancel() {
            if (task.cancel(false)) {
                onFinishRequest(clientId, effectivePenalty, client.adaptivePenalty, CANCELLATION_EXCEPTION);
            }
        }
    }

    @NonNullApi
    @NonNullFields
    static class YtClientEntry {
        BaseYtClient client;
        Duration initialPenalty;
        Duration adaptivePenalty = Duration.ZERO;
        Duration externalPenalty = Duration.ZERO;
        @Nullable Instant banUntil = null;

        YtClientEntry(
                MultiYtClient.YtClientOptions clientOptions
        ) {
            if (clientOptions.client.getClusters().size() != 1) {
                throw new IllegalArgumentException("Got YtClient with more than 1 cluster");
            }
            this.client = clientOptions.client;
            this.initialPenalty = clientOptions.initialPenalty;
        }

        YtClientEntry(YtClientEntry other) {
            this.client = other.client;
            this.initialPenalty = Duration.ofMillis(other.initialPenalty.toMillis());
            this.adaptivePenalty = Duration.ofMillis(other.adaptivePenalty.toMillis());
            this.externalPenalty = Duration.ofMillis(other.externalPenalty.toMillis());
            if (other.banUntil != null) {
                this.banUntil = Instant.ofEpochMilli(other.banUntil.toEpochMilli());
            }
        }

        Duration getPenalty() {
            return initialPenalty.plus(adaptivePenalty).plus(externalPenalty);
        }

        String getClusterName() {
            return client.getClusters().get(0).getName();
        }
    }
}
