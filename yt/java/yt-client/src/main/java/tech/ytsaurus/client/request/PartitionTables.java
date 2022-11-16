package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.DataSize;
import tech.ytsaurus.core.cypress.YPath;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.EPartitionTablesMode;
import ru.yandex.yt.rpcproxy.TFetchChunkSpecConfig;
import ru.yandex.yt.rpcproxy.TFetcherConfig;
import ru.yandex.yt.rpcproxy.TReqPartitionTables;
import ru.yandex.yt.rpcproxy.TTransactionalOptions;

@NonNullApi
@NonNullFields
public class PartitionTables
        extends RequestBase<PartitionTables.Builder, PartitionTables>
        implements HighLevelRequest<TReqPartitionTables.Builder> {
    private final List<YPath> paths;
    @Nullable
    private final FetchChunkSpecConfig fetchChunkSpecConfig;
    @Nullable
    private final FetcherConfig fetcherConfig;
    @Nullable
    private final ChunkSliceFetcherConfig chunkSliceFetcherConfig;
    private final PartitionTablesMode partitionMode;
    private final DataSize dataWeightPerPartition;
    @Nullable
    private final Integer maxPartitionCount;
    @Nullable
    private final Boolean enableKeyGuarantee;
    @Nullable
    private final TransactionalOptions transactionalOptions;

    PartitionTables(Builder builder) {
        super(builder);
        this.paths = new ArrayList<>(Objects.requireNonNull(builder.paths));
        this.chunkSliceFetcherConfig = builder.chunkSliceFetcherConfig;
        this.fetchChunkSpecConfig = builder.fetchChunkSpecConfig;
        this.fetcherConfig = builder.fetcherConfig;
        this.partitionMode = Objects.requireNonNull(builder.partitionMode);
        this.dataWeightPerPartition = Objects.requireNonNull(builder.dataWeightPerPartition);
        this.maxPartitionCount = builder.maxPartitionCount;
        this.enableKeyGuarantee = builder.enableKeyGuarantee;
        if (builder.transactionalOptions != null) {
            this.transactionalOptions = new TransactionalOptions(builder.transactionalOptions);
        } else {
            this.transactionalOptions = null;
        }
    }

    public PartitionTables(List<YPath> paths, PartitionTablesMode partitionMode, DataSize dataWeightPerPartition) {
        this(builder()
                .setPaths(paths)
                .setPartitionMode(partitionMode)
                .setDataWeightPerPartition(dataWeightPerPartition));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqPartitionTables.Builder, ?> requestBuilder) {
        TReqPartitionTables.Builder builder = requestBuilder.body();
        for (YPath path : paths) {
            builder.addPaths(path.toString());
        }
        if (chunkSliceFetcherConfig != null) {
            builder.setChunkSliceFetcherConfig(chunkSliceFetcherConfig.writeTo(
                    TReqPartitionTables.TChunkSliceFetcherConfig.newBuilder()));
        }
        if (fetchChunkSpecConfig != null) {
            builder.setFetchChunkSpecConfig(fetchChunkSpecConfig.writeTo(TFetchChunkSpecConfig.newBuilder()));
        }
        if (fetcherConfig != null) {
            builder.setFetcherConfig(fetcherConfig.writeTo(TFetcherConfig.newBuilder()));
        }
        builder.setPartitionMode(EPartitionTablesMode.forNumber(partitionMode.getProtoValue()));
        builder.setDataWeightPerPartition(dataWeightPerPartition.toBytes());
        if (maxPartitionCount != null) {
            builder.setMaxPartitionCount(maxPartitionCount);
        }
        if (enableKeyGuarantee != null) {
            builder.setEnableKeyGuarantee(enableKeyGuarantee);
        }
        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("Paths: ").append(Arrays.toString(paths.toArray()));
        sb.append("; PartitionMode: ").append(partitionMode.getName());
        sb.append("; DataWeightPerPartition: ").append(dataWeightPerPartition);
        if (maxPartitionCount != null) {
            sb.append("; MaxPartitionCount: ").append(maxPartitionCount);
        }
        sb.append(";");
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setPaths(paths)
                .setChunkSliceFetcherConfig(chunkSliceFetcherConfig)
                .setFetchChunkSpecConfig(fetchChunkSpecConfig)
                .setFetcherConfig(fetcherConfig)
                .setPartitionMode(partitionMode)
                .setDataWeightPerPartition(dataWeightPerPartition)
                .setMaxPartitionCount(maxPartitionCount)
                .setEnableKeyGuarantee(enableKeyGuarantee)
                .setTransactionalOptions(transactionalOptions)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends RequestBase.Builder<Builder, PartitionTables> {
        @Nullable
        private List<YPath> paths;
        @Nullable
        private FetchChunkSpecConfig fetchChunkSpecConfig;
        @Nullable
        private FetcherConfig fetcherConfig;
        @Nullable
        private ChunkSliceFetcherConfig chunkSliceFetcherConfig;
        @Nullable
        private PartitionTablesMode partitionMode;
        @Nullable
        private DataSize dataWeightPerPartition;
        @Nullable
        private Integer maxPartitionCount;
        @Nullable
        private Boolean enableKeyGuarantee;
        @Nullable
        private TransactionalOptions transactionalOptions;

        public Builder setPaths(List<YPath> paths) {
            this.paths = paths;
            return self();
        }

        public Builder setPaths(YPath... paths) {
            setPaths(Arrays.asList(paths));
            return self();
        }

        public Builder setFetchChunkSpecConfig(@Nullable FetchChunkSpecConfig fetchChunkSpecConfig) {
            this.fetchChunkSpecConfig = fetchChunkSpecConfig;
            return self();
        }

        public Builder setFetcherConfig(@Nullable FetcherConfig fetcherConfig) {
            this.fetcherConfig = fetcherConfig;
            return self();
        }

        public Builder setChunkSliceFetcherConfig(@Nullable ChunkSliceFetcherConfig chunkSliceFetcherConfig) {
            this.chunkSliceFetcherConfig = chunkSliceFetcherConfig;
            return self();
        }

        public Builder setPartitionMode(PartitionTablesMode mode) {
            this.partitionMode = mode;
            return self();
        }

        public Builder setDataWeightPerPartition(DataSize dataWeightPerPartition) {
            this.dataWeightPerPartition = dataWeightPerPartition;
            return self();
        }

        public Builder setMaxPartitionCount(@Nullable Integer maxPartitionCount) {
            this.maxPartitionCount = maxPartitionCount;
            return self();
        }

        public Builder setEnableKeyGuarantee(@Nullable Boolean enableKeyGuarantee) {
            this.enableKeyGuarantee = enableKeyGuarantee;
            return self();
        }

        public Builder setTransactionalOptions(@Nullable TransactionalOptions transactionalOptions) {
            this.transactionalOptions = transactionalOptions;
            return self();
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public PartitionTables build() {
            return new PartitionTables(this);
        }
    }
}
