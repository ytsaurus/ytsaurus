package tech.ytsaurus.client;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
    private final List<YTsaurusClientOptions> clients = new ArrayList<>();
    private final MultiExecutor executor;

    private MultiYTsaurusClient(Builder builder) {

        clients.addAll(builder.clientsOptions);

        builder.clients.stream()
                .map(client -> YTsaurusClientOptions.builder(client)
                        .setInitialPenalty(builder.preferredAllowance)
                        .build())
                .forEach(clients::add);

        builder.preferredClusters.stream().map((cluster) -> builder.clientBuilderSupplier.get()
                        .setClusters(cluster)
                        .setConfig(builder.config)
                        .setRpcCompression(builder.compression)
                        .setAuth(builder.auth)
                        .build()
                )
                .map(client -> YTsaurusClientOptions.builder(client).build())
                .forEach(clients::add);

        builder.clusters.stream().map((cluster) -> builder.clientBuilderSupplier.get()
                        .setClusters(cluster)
                        .setConfig(builder.config)
                        .setRpcCompression(builder.compression)
                        .setAuth(builder.auth)
                        .build())
                .map(client -> YTsaurusClientOptions.builder(client)
                        .setInitialPenalty(builder.preferredAllowance)
                        .build())
                .forEach(clients::add);

        if (this.clients.size() < 2) {
            throw new IllegalArgumentException("Count of clients is less than 2");
        }

        List<String> clusterNames = this.clients.stream()
                .map(YTsaurusClientOptions::getClusterName)
                .collect(Collectors.toUnmodifiableList());
        if (clusterNames.stream().distinct().count() != clusterNames.size()) {
            var duplicatesHint = clusterNames.stream()
                    .filter(clusterName -> Collections.frequency(clusterNames, clusterName) > 1)
                    .collect(Collectors.joining(", "));
            throw new IllegalArgumentException("Duplicate clusters are not permitted: " + duplicatesHint);
        }

        executor = new MultiExecutor(
                clients,
                builder.banPenalty,
                builder.banDuration,
                builder.penaltyProvider,
                builder.executorMonitoring
        );

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
    public void close() {
        try {
            for (YTsaurusClientOptions entry : clients) {
                entry.client.close();
            }
            executor.close();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
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

        public String getClusterName() {
            return client.getClusters().get(0).getName();
        }

        public String getShortClusterName() {
            return getClusterName().split("\\.")[0];
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
        MultiExecutorMonitoring executorMonitoring = new NoopMultiExecutorMonitoring();
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

        /**
         * Set executor callback to monitor hedging request execution.
         *
         * @return self
         */
        public Builder setExecutorMonitoring(MultiExecutorMonitoring executorMonitoring) {
            this.executorMonitoring = executorMonitoring;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}

class MultiExecutor implements Closeable {

    private final List<ClientEntry> clients;
    private final Duration banPenalty;
    private final Duration banDuration;
    private final PenaltyProvider penaltyProvider;
    private final MultiExecutorMonitoring executorMonitoring;

    MultiExecutor(
            List<MultiYTsaurusClient.YTsaurusClientOptions> clientOptions,
            Duration banPenalty,
            Duration banDuration,
            PenaltyProvider penaltyProvider,
            MultiExecutorMonitoring executorMonitoring
    ) {
        this.clients = clientOptions.stream().map(ClientEntry::new).collect(Collectors.toUnmodifiableList());
        this.banPenalty = banPenalty;
        this.banDuration = banDuration;
        this.penaltyProvider = penaltyProvider;
        this.executorMonitoring = executorMonitoring;
    }

    <R> CompletableFuture<R> execute(Function<BaseYTsaurusClient, CompletableFuture<R>> callback) {
        return new MultiExecutorRequestTask<>(
                getEffectiveClientOptions(Instant.now()),
                callback,
                executorMonitoring
        ).getFuture();
    }

    private synchronized List<MultiExecutorRequestTask.ClientEntry> getEffectiveClientOptions(Instant now) {
        // Update penalties.
        for (ClientEntry client : clients) {
            if (client.banUntil != null && client.banUntil.compareTo(now) < 0) {
                client.adaptivePenalty = Duration.ZERO;
            }
            client.externalPenalty = penaltyProvider.getPenalty(client.clientOptions.getShortClusterName());
        }

        // Calculate min penalty to subtract.
        final Duration minPenalty = Collections.min(
                clients.stream().map(ClientEntry::getPenalty).collect(Collectors.toUnmodifiableList())
        );

        // Create immutable client entries with right effective penalties.
        return clients.stream().map((client) -> new MultiExecutorRequestTask.ClientEntry(
                client.clientOptions,
                client.getPenalty().minus(minPenalty),
                client::onFinishRequest
        )).collect(Collectors.toUnmodifiableList());
    }

    @Override
    public void close() throws IOException {
        penaltyProvider.close();
    }

    public class ClientEntry {

        private final MultiYTsaurusClient.YTsaurusClientOptions clientOptions;
        private Duration adaptivePenalty;
        private Duration externalPenalty;
        @Nullable
        private Instant banUntil;

        ClientEntry(
                MultiYTsaurusClient.YTsaurusClientOptions clientOptions
        ) {
            this(clientOptions, Duration.ZERO, Duration.ZERO, null);
        }

        ClientEntry(
                MultiYTsaurusClient.YTsaurusClientOptions clientOptions,
                Duration adaptivePenalty,
                Duration externalPenalty,
                @Nullable Instant banUntil
        ) {
            if (clientOptions.client.getClusters().size() != 1) {
                throw new IllegalArgumentException("Got YTsaurusClient with more than 1 cluster");
            }
            this.clientOptions = clientOptions;
            this.adaptivePenalty = adaptivePenalty;
            this.externalPenalty = externalPenalty;
            this.banUntil = banUntil;
        }

        Duration getPenalty() {
            return clientOptions.initialPenalty.plus(adaptivePenalty).plus(externalPenalty);
        }

        public synchronized void onFinishRequest(Boolean success) {
            if (success) {
                if (this.adaptivePenalty.compareTo(Duration.ZERO) > 0) {
                    this.banUntil = null;
                    this.adaptivePenalty = Duration.ZERO;
                }
            } else {
                this.banUntil = Instant.now().plus(banDuration);
                this.adaptivePenalty = this.adaptivePenalty.plus(banPenalty);
            }
        }

    }

}
