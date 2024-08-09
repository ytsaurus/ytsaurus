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
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import tech.ytsaurus.client.request.AbstractLookupRowsRequest;
import tech.ytsaurus.client.request.MultiLookupRowsRequest;
import tech.ytsaurus.client.request.SelectRowsRequest;
import tech.ytsaurus.client.rows.ConsumerSource;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.client.rows.VersionedRowset;
import tech.ytsaurus.core.rows.YTreeRowSerializer;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * YTsaurus client for requests in several clusters.
 * <p>
 * Example:
 * <pre>
 *   MultiYTsaurusClient multiClient = MultiYTsaurusClient.builder()
 *     .addPreferredCluster("hahn")
 *     .addCluster("arnold")
 *     .build();
 *   multiClient.lookupRows(...).join();
 * </pre>
 * <p>
 * How it works:
 * MultiYTsaurusClient contains several YTsaurusClients. Each client corresponds to one cluster.
 * In some clusters, the request is made immediately (at least in one), and in some with some delay.
 * When a response is received from one of the clusters, the pending request is canceled.
 * The delay increases when errors occur. If there were no errors for some time, then the delay decreases.
 * <p>
 * Some names:
 * preferredCluster - such a cluster has zero minimum initial delay. It can be increased if errors occur.
 * preferredAllowance - initial delay for non-preferred clusters.
 */
@NonNullFields
@NonNullApi
public class MultiYTsaurusClient implements ImmutableTransactionalClient, Closeable {
    final MultiExecutor executor;

    private MultiYTsaurusClient(Builder builder) {
        executor = new MultiExecutor(builder);
    }

    @Override
    public void close() {
        try {
            for (MultiExecutor.YTsaurusClientEntry entry : executor.clients) {
                entry.client.close();
            }
            executor.close();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Create MultiYTsaurusClient builder
     *
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
    public CompletableFuture<List<UnversionedRowset>> multiLookupRows(MultiLookupRowsRequest request) {
        return executor.execute((client) -> client.multiLookupRows(request));
    }

    @Override
    public <T> CompletableFuture<List<List<T>>> multiLookupRows(
            MultiLookupRowsRequest request,
            YTreeRowSerializer<T> serializer
    ) {
        return executor.execute((client) -> client.multiLookupRows(request, serializer));
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
     * YTsaurus client with initial penalty.
     * Initial penalty is needed to prioritize clusters.
     */
    public static class YTsaurusClientOptions {
        final BaseYTsaurusClient client;
        final Duration initialPenalty;

        YTsaurusClientOptions(Builder builder) {
            this.client = builder.client;
            this.initialPenalty = builder.initialPenalty;
        }

        public static Builder builder(BaseYTsaurusClient client) {
            return new Builder(client);
        }

        public static class Builder {
            final BaseYTsaurusClient client;
            Duration initialPenalty = Duration.ZERO;

            public Builder(BaseYTsaurusClient client) {
                this.client = client;
            }

            /**
             * Set initial penalty for this client.
             *
             * @return self
             */
            public Builder setInitialPenalty(Duration initialPenalty) {
                this.initialPenalty = initialPenalty;
                return this;
            }

            public YTsaurusClientOptions build() {
                return new YTsaurusClientOptions(this);
            }
        }
    }

    @NonNullApi
    @NonNullFields
    public static class Builder extends YTsaurusClient.BaseBuilder<MultiYTsaurusClient, Builder> {
        List<YTsaurusClientOptions> clientsOptions = new ArrayList<>();
        List<YTsaurusClient> clients = new ArrayList<>();
        List<String> clusters = new ArrayList<>();
        List<String> preferredClusters = new ArrayList<>();

        Duration banPenalty = Duration.ofMillis(1);
        Duration banDuration = Duration.ofMillis(50);
        PenaltyProvider penaltyProvider = PenaltyProvider.dummyPenaltyProviderBuilder().build();
        Duration preferredAllowance = Duration.ofMillis(100);
        Supplier<YTsaurusClient.ClientBuilder<? extends YTsaurusClient, ?>> clientBuilderSupplier =
                YTsaurusClient::builder;

        Builder() {
        }

        public MultiYTsaurusClient build() {
            return new MultiYTsaurusClient(this);
        }

        /**
         * Add one more client with penalty. Client must be for only one cluster.
         *
         * @return self
         */
        public Builder addClient(YTsaurusClientOptions clientOptions) {
            this.clientsOptions.add(clientOptions);
            return this;
        }

        /**
         * Add more clients with penalties. Each client must be for only one cluster.
         *
         * @return self
         */
        public Builder addClients(YTsaurusClientOptions first, YTsaurusClientOptions... rest) {
            addClient(first);
            for (YTsaurusClientOptions clientOptions : rest) {
                addClient(clientOptions);
            }
            return this;
        }

        /**
         * Add one more client. Client must be for only one cluster.
         *
         * @return self
         */
        public Builder addClient(YTsaurusClient client) {
            this.clients.add(client);
            return this;
        }

        /**
         * Add more clients. Each client must be for only one cluster.
         *
         * @return self
         */
        public Builder addClients(YTsaurusClient first, YTsaurusClient... rest) {
            addClient(first);
            for (YTsaurusClient client : rest) {
                addClient(client);
            }
            return this;
        }

        /**
         * Add usual cluster (for requests without preferredAllowance).
         *
         * @return self
         */
        public Builder addCluster(String cluster) {
            this.clusters.add(cluster);
            return this;
        }

        /**
         * Add preferred cluster (for requests with preferredAllowance).
         *
         * @return self
         */
        public Builder addPreferredCluster(String cluster) {
            this.preferredClusters.add(cluster);
            return this;
        }

        /**
         * Set a penalty that will be assigned in case of errors.
         *
         * @return self
         */
        public Builder setBanPenalty(Duration banPenalty) {
            this.banPenalty = banPenalty;
            return this;
        }

        /**
         * Set the duration after which the error penalty can be reset (if there are no more errors).
         *
         * @return self
         */
        public Builder setBanDuration(Duration banDuration) {
            this.banDuration = banDuration;
            return this;
        }

        /**
         * Set duration between requests in usual and preferred clusters.
         *
         * @return self
         */
        public Builder setPreferredAllowance(Duration preferredAllowance) {
            this.preferredAllowance = preferredAllowance;
            return this;
        }

        /**
         * Set penalty provider for assigning additional penalties depended on external events,
         * for instance, if replica is `lagged`.
         *
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
    final List<YTsaurusClientEntry> clients;

    static final CancellationException CANCELLATION_EXCEPTION = new CancellationException();
    static final Duration MAX_DURATION = ChronoUnit.FOREVER.getDuration();

    MultiExecutor(MultiYTsaurusClient.Builder builder) {
        if (builder.clientsOptions.isEmpty() &&
                builder.clusters.isEmpty() &&
                builder.clients.isEmpty() &&
                builder.preferredClusters.isEmpty()
        ) {
            throw new IllegalArgumentException("No clients and no clusters in MultiYTsaurusClient's constructor");
        }

        this.banPenalty = builder.banPenalty;
        this.banDuration = builder.banDuration;
        this.penaltyProvider = builder.penaltyProvider;

        this.clients = new ArrayList<>();

        if (!builder.clientsOptions.isEmpty()) {
            addClients(builder.clientsOptions);
        }

        if (!builder.clients.isEmpty()) {
            List<MultiYTsaurusClient.YTsaurusClientOptions> clientOptions =
                    builder.clients.stream()
                            .map(client -> MultiYTsaurusClient.YTsaurusClientOptions.builder(client)
                                    .setInitialPenalty(builder.preferredAllowance)
                                    .build())
                            .collect(Collectors.toList());
            addClients(clientOptions);
        }

        if (!builder.preferredClusters.isEmpty()) {
            List<YTsaurusClient> clientsFromClusters = createClientsFromClusters(builder.preferredClusters, builder);
            List<MultiYTsaurusClient.YTsaurusClientOptions> clientOptions =
                    clientsFromClusters.stream()
                            .map(client -> MultiYTsaurusClient.YTsaurusClientOptions.builder(client).build())
                            .collect(Collectors.toList());
            addClients(clientOptions);
        }

        if (!builder.clusters.isEmpty()) {
            List<YTsaurusClient> clientsFromClusters = createClientsFromClusters(builder.clusters, builder);
            List<MultiYTsaurusClient.YTsaurusClientOptions> clientOptions = clientsFromClusters.stream()
                    .map(client -> MultiYTsaurusClient.YTsaurusClientOptions.builder(client)
                            .setInitialPenalty(builder.preferredAllowance)
                            .build())
                    .collect(Collectors.toList());
            addClients(clientOptions);
        }

        if (this.clients.size() < 2) {
            throw new IllegalArgumentException("Count of clients is less than 2");
        }

        HashSet<String> clusters = new HashSet<>();
        for (YTsaurusClientEntry client : this.clients) {
            String clusterName = client.getClusterName();
            if (clusters.contains(clusterName)) {
                throw new IllegalArgumentException("Got more than one clients for cluster: '" + clusterName + "'");
            }
            clusters.add(clusterName);
        }
    }

    private void addClients(List<MultiYTsaurusClient.YTsaurusClientOptions> clientOptions) {
        for (MultiYTsaurusClient.YTsaurusClientOptions options : clientOptions) {
            this.clients.add(new YTsaurusClientEntry(options));
        }
    }

    private static List<YTsaurusClient> createClientsFromClusters(
            List<String> clusters, MultiYTsaurusClient.Builder builder) {
        List<YTsaurusClient> result = new ArrayList<>();
        for (String cluster : clusters) {
            var clientBuilder = builder.clientBuilderSupplier.get()
                    .setClusters(cluster)
                    .setConfig(builder.config)
                    .setRpcCompression(builder.compression);
            if (builder.auth != null) {
                clientBuilder.setAuth(builder.auth);
            }
            result.add(clientBuilder.build());
        }
        return result;
    }

    private List<YTsaurusClientEntry> updateAdaptivePenalty(Instant now) {
        List<YTsaurusClientEntry> currentClients = new ArrayList<>(clients.size());

        synchronized (this) {
            for (YTsaurusClientEntry client : clients) {
                if (client.banUntil != null && client.banUntil.compareTo(now) < 0) {
                    client.adaptivePenalty = Duration.ZERO;
                }
                currentClients.add(new YTsaurusClientEntry(client));
            }

            return currentClients;
        }
    }

    private Duration getMinPenalty(List<YTsaurusClientEntry> clients) {
        Duration minPenalty = MAX_DURATION;
        for (YTsaurusClientEntry client : clients) {
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
        YTsaurusClientEntry client = clients.get(clientId);
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

    <R> CompletableFuture<R> execute(Function<BaseYTsaurusClient, CompletableFuture<R>> callback) {
        Instant now = Instant.now();
        List<YTsaurusClientEntry> currentClients = updateAdaptivePenalty(now);
        Duration minPenalty = getMinPenalty(currentClients);
        List<DelayedTask<R>> delayedTasks = new ArrayList<>();
        CompletableFuture<R> result = new CompletableFuture<>();

        AtomicInteger finishedClientsCount = new AtomicInteger(0);

        for (int clientId = 0; clientId < currentClients.size(); ++clientId) {
            YTsaurusClientEntry client = currentClients.get(clientId);
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
        final YTsaurusClientEntry client;

        DelayedTask(ScheduledFuture<?> task, CompletableFuture<R> result,
                    int clientId, Duration effectivePenalty, YTsaurusClientEntry client, Instant start) {
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
    static class YTsaurusClientEntry {
        BaseYTsaurusClient client;
        Duration initialPenalty;
        Duration adaptivePenalty = Duration.ZERO;
        Duration externalPenalty = Duration.ZERO;
        @Nullable
        Instant banUntil = null;

        YTsaurusClientEntry(
                MultiYTsaurusClient.YTsaurusClientOptions clientOptions
        ) {
            if (clientOptions.client.getClusters().size() != 1) {
                throw new IllegalArgumentException("Got YTsaurusClient with more than 1 cluster");
            }
            this.client = clientOptions.client;
            this.initialPenalty = clientOptions.initialPenalty;
        }

        YTsaurusClientEntry(YTsaurusClientEntry other) {
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
