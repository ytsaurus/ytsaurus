package tech.ytsaurus.client;

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.request.ColumnFilter;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.client.request.GetTabletInfos;
import tech.ytsaurus.client.request.MasterReadKind;
import tech.ytsaurus.client.request.MasterReadOptions;
import tech.ytsaurus.client.request.TableReplicaMode;
import tech.ytsaurus.client.request.TabletInfo;
import tech.ytsaurus.client.request.TabletInfoReplica;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.YtTimestamp;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeNode;


@NonNullApi
@NonNullFields
public abstract class PenaltyProvider implements Closeable {
    /**
     * Allow getting penalty for current cluster
     *
     * @param clusterName: current cluster
     * @return penalty duration
     */
    abstract Duration getPenalty(String clusterName);

    /**
     * @return dummy penalty provider, always return zero penalty.
     */
    public static DummyPenaltyProvider.Builder dummyPenaltyProviderBuilder() {
        return new DummyPenaltyProvider.Builder();
    }

    /**
     * @return penalty provider which gives penalties depended on replication lag of replicas.
     */
    public static LagPenaltyProvider.Builder lagPenaltyProviderBuilder() {
        return new LagPenaltyProvider.Builder();
    }

    @NonNullApi
    @NonNullFields
    public static class DummyPenaltyProvider extends PenaltyProvider {
        DummyPenaltyProvider(Builder builder) {
        }

        @Override
        Duration getPenalty(String clusterName) {
            return Duration.ZERO;
        }

        @Override
        public void close() {
        }

        public static class Builder {
            public PenaltyProvider build() {
                return new DummyPenaltyProvider(this);
            }
        }
    }

    @NonNullApi
    @NonNullFields
    public static class LagPenaltyProvider extends PenaltyProvider {
        private static final Logger logger = LoggerFactory.getLogger(LagPenaltyProvider.class);

        private final Map<String, ReplicaInfo> replicaClusters;
        private final YPath tablePath;
        private final BaseYtClient client;
        private final Duration maxTabletLag;
        private final Duration lagPenalty;
        private final double maxTabletsWithLagFraction;
        private final boolean clearPenaltiesOnErrors;
        private final Duration checkPeriod;

        private final CompletableFuture<Void> closed = new CompletableFuture<>();

        LagPenaltyProvider(Builder builder) {
            Objects.requireNonNull(builder.replicaClusters);
            Objects.requireNonNull(builder.tablePath);
            Objects.requireNonNull(builder.client);

            this.replicaClusters = new HashMap<>();
            builder.replicaClusters.forEach(cluster -> replicaClusters.put(cluster, null));
            this.tablePath = builder.tablePath;
            this.client = builder.client;
            this.maxTabletLag = builder.maxTabletLag;
            this.lagPenalty = builder.lagPenalty;
            this.maxTabletsWithLagFraction = builder.maxTabletsWithLagFraction;
            this.clearPenaltiesOnErrors = builder.clearPenaltiesOnErrors;
            this.checkPeriod = builder.checkPeriod;

            this.client.getExecutor().schedule(
                    this::updateCurrentLagPenalty,
                    0,
                    TimeUnit.MILLISECONDS
            );
        }

        private void updateCurrentLagPenalty() {
            updateReplicaIdsIfNeeded()
                    .thenCompose(unused -> getTabletsInfo())
                    .thenApply(tabletsInfo -> {
                        long totalTabletsCount = tabletsInfo.getTotalNumberOfTablets();
                        for (Map.Entry<String, ReplicaInfo> entry : replicaClusters.entrySet()) {
                            String cluster = entry.getKey();
                            GUID replicaId = entry.getValue().getReplicaId();
                            long tabletsWithLagCount = tabletsInfo.getNumbersOfTabletsWithLag()
                                    .getOrDefault(replicaId, 0L);
                            Duration newLagPenalty = calculateLagPenalty(totalTabletsCount, tabletsWithLagCount);
                            entry.getValue().setCurrentLagPenalty(newLagPenalty);

                            logger.info(
                                    "Finish penalty updater check ({}: {}/{} tablets lagging => penalty {} ms) for: {}",
                                    cluster,
                                    tabletsWithLagCount,
                                    totalTabletsCount,
                                    newLagPenalty,
                                    tablePath
                            );
                        }
                        return null;
                    }).handle((unused, ex) -> {
                if (ex != null) {
                    logger.info("Lag penalty updater for {} failed: {}", tablePath, ex);

                    if (clearPenaltiesOnErrors) {
                        for (Map.Entry<String, ReplicaInfo> entry : replicaClusters.entrySet()) {
                            logger.info(
                                    "Clearing penalty for cluster {} and table {}",
                                    entry.getValue(),
                                    tablePath
                            );
                            entry.getValue().setCurrentLagPenalty(Duration.ZERO);
                        }
                    }
                }
                if (!closed.isDone()) {
                    client.getExecutor().schedule(
                            this::updateCurrentLagPenalty,
                            checkPeriod.toMillis(),
                            TimeUnit.MILLISECONDS);
                }
                return null;
            });
        }

        private Duration calculateLagPenalty(long totalTabletsCount, long tabletsWithLagCount) {
            if (tabletsWithLagCount >= totalTabletsCount * maxTabletsWithLagFraction) {
                return lagPenalty;
            }
            return Duration.ZERO;
        }

        private CompletableFuture<Void> updateReplicaIdsIfNeeded() {
            try {
                checkAllReplicaIdsPresent();
                return CompletableFuture.completedFuture(null);
            } catch (UnknownReplicaIdException ex) {
                return client.getNode(GetNode.builder()
                        .setPath(tablePath)
                        .setAttributes(ColumnFilter.of("replicas"))
                        .setTimeout(Duration.ofSeconds(5))
                        .setMasterReadOptions(new MasterReadOptions().setReadFrom(MasterReadKind.Cache))
                        .build()
                ).thenApply(node -> {
                    for (Map.Entry<String, YTreeNode> row : node.getAttributeOrThrow("replicas").asMap().entrySet()) {
                        String cluster = row.getValue().asMap().get("cluster_name").stringValue();
                        if (replicaClusters.containsKey(cluster)) {
                            replicaClusters.put(cluster, new ReplicaInfo(GUID.valueOf(row.getKey())));
                        }
                    }
                    checkAllReplicaIdsPresent();
                    return null;
                });
            }
        }

        private void checkAllReplicaIdsPresent() {
            for (Map.Entry<String, ReplicaInfo> replica : replicaClusters.entrySet()) {
                if (replica.getValue() == null) {
                    throw new UnknownReplicaIdException("Replica id wasn't found for " + replica.getKey());
                }
            }
        }

        private CompletableFuture<TabletsInfo> getTabletsInfo() {
            return client.getNode(GetNode.builder()
                    .setPath(tablePath)
                    .setAttributes(ColumnFilter.of("tablet_count"))
                    .setTimeout(Duration.ofSeconds(5))
                    .setMasterReadOptions(new MasterReadOptions().setReadFrom(MasterReadKind.Cache))
                    .build()
            ).thenCompose(tabletsCountNode -> {
                int tabletsCount = tabletsCountNode.getAttributeOrThrow("tablet_count").intValue();
                return client.getTabletInfos(
                        GetTabletInfos.builder().setPath(tablePath.toString())
                                .setTabletIndexes(IntStream.range(0, tabletsCount).boxed().collect(Collectors.toList()))
                                .build()
                ).thenApply(tabletInfos -> {
                    Map<GUID, Long> tabletsWithLag = new HashMap<>();
                    Instant now = Instant.now();
                    for (TabletInfo tabletInfo : tabletInfos) {
                        for (TabletInfoReplica tabletInfoReplica : tabletInfo.getTabletInfoReplicas()) {
                            Instant lastReplicationTimestamp = YtTimestamp.valueOf(
                                    tabletInfoReplica.getLastReplicationTimestamp()).getInstant();
                            if (TableReplicaMode.Async.equals(tabletInfoReplica.getMode()) &&
                                    now.minus(maxTabletLag).compareTo(lastReplicationTimestamp) > 0) {
                                tabletsWithLag.merge(tabletInfoReplica.getReplicaId(), 1L, Long::sum);
                            }
                        }
                    }
                    return new TabletsInfo(tabletsCount, tabletsWithLag);
                });
            });
        }

        @Override
        Duration getPenalty(String clusterName) {
            if (replicaClusters.containsKey(clusterName)) {
                return replicaClusters.get(clusterName).getCurrentLagPenalty();
            }
            return Duration.ZERO;
        }

        @Override
        public void close() {
            closed.complete(null);
        }

        static class UnknownReplicaIdException extends RuntimeException {
            UnknownReplicaIdException(String message) {
                super(message);
            }
        }

        @NonNullApi
        @NonNullFields
        public static class Builder {
            @Nullable
            private List<String> replicaClusters;
            @Nullable
            private YPath tablePath;
            @Nullable
            private BaseYtClient client;
            private Duration maxTabletLag = Duration.ofSeconds(300);
            private Duration lagPenalty = Duration.ofMillis(10);
            private double maxTabletsWithLagFraction = 0.05;
            private boolean clearPenaltiesOnErrors = false;
            private Duration checkPeriod = Duration.ofSeconds(60);

            /**
             * Set clusters that need checks for replication lag.
             */
            public Builder setReplicaClusters(List<String> replicaClusters) {
                if (replicaClusters.isEmpty()) {
                    throw new IllegalArgumentException("Got empty list of clusters");
                }
                this.replicaClusters = replicaClusters;
                return this;
            }

            /**
             * Set table that needs checks for replication lag.
             */
            public Builder setTablePath(YPath tablePath) {
                this.tablePath = tablePath;
                return this;
            }

            /**
             * Set client for getting replications info.
             */
            public Builder setClient(BaseYtClient client) {
                this.client = client;
                return this;
            }

            /**
             * Set max allowed lag when tablets aren't considered as 'lagged'.
             * Tablet is considered as 'lagged'
             * if CurrentTimestamp - TabletLastReplicationTimestamp >= MaxTabletLag (in seconds).
             */
            public Builder setMaxTabletLag(Duration maxTabletLag) {
                this.maxTabletLag = maxTabletLag;
                return this;
            }

            /**
             * Set a penalty that will be assigned in case of lag.
             * Same as banPenalty in MultiYtClient.
             */
            public Builder setLagPenalty(Duration lagPenalty) {
                this.lagPenalty = lagPenalty;
                return this;
            }

            /**
             * Real value from 0.0 to 1.0.
             * Replica cluster receives LagPenalty
             * if NumberOfTabletsWithLag >= MaxTabletsWithLagFraction * TotalNumberOfTablets.
             */
            public Builder setMaxTabletsWithLagFraction(double maxTabletsWithLagFraction) {
                this.maxTabletsWithLagFraction = maxTabletsWithLagFraction;
                return this;
            }

            /**
             * If true, in case of any errors from yt client - clear all penalties.
             */
            public Builder setClearPenaltiesOnErrors(boolean clearPenaltiesOnErrors) {
                this.clearPenaltiesOnErrors = clearPenaltiesOnErrors;
                return this;
            }

            /**
             * Set replication lag check period.
             */
            public Builder setCheckPeriod(Duration checkPeriod) {
                this.checkPeriod = checkPeriod;
                return this;
            }

            public PenaltyProvider build() {
                return new LagPenaltyProvider(this);
            }
        }

        static class TabletsInfo {
            private final long totalNumberOfTablets;
            private final Map<GUID, Long> numbersOfTabletsWithLag;

            TabletsInfo(long totalNumberOfTablets, Map<GUID, Long> numbersOfTabletsWithLag) {
                this.totalNumberOfTablets = totalNumberOfTablets;
                this.numbersOfTabletsWithLag = numbersOfTabletsWithLag;
            }

            long getTotalNumberOfTablets() {
                return totalNumberOfTablets;
            }

            Map<GUID, Long> getNumbersOfTabletsWithLag() {
                return numbersOfTabletsWithLag;
            }
        }

        static class ReplicaInfo {
            private final GUID replicaId;
            private final AtomicLong currentLagPenaltyMillis = new AtomicLong();

            ReplicaInfo(GUID replicaId) {
                this.replicaId = replicaId;
            }

            void setCurrentLagPenalty(Duration currentLagPenalty) {
                this.currentLagPenaltyMillis.set(currentLagPenalty.toMillis());
            }

            public GUID getReplicaId() {
                return replicaId;
            }

            public Duration getCurrentLagPenalty() {
                return Duration.ofMillis(currentLagPenaltyMillis.get());
            }
        }
    }
}
