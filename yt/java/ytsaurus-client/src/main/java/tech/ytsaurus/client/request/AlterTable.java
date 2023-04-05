package tech.ytsaurus.client.request;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.rpcproxy.TReqAlterTable;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Immutable alter table request.
 * <p>
 *
 * @see tech.ytsaurus.client.ApiServiceClient#alterTable(AlterTable)
 * @see <a href="https://ytsaurus.tech/docs/en/api/commands#alter_table">
 * alter_table documentation
 * </a>
 */
public class AlterTable
        extends TableReq<AlterTable.Builder, AlterTable>
        implements HighLevelRequest<TReqAlterTable.Builder> {
    @Nullable
    private final YTreeNode schemaNode;
    @Nullable
    private final Boolean dynamic;
    @Nullable
    private final GUID upstreamReplicaId;
    @Nullable
    private final TransactionalOptions transactionalOptions;

    AlterTable(BuilderBase<?> builder) {
        super(builder);
        this.schemaNode = builder.schemaNode;
        this.dynamic = builder.dynamic;
        this.upstreamReplicaId = builder.upstreamReplicaId;
        this.transactionalOptions = builder.transactionalOptions;
    }

    /**
     * Constructs alter table request from path with other options set to default.
     */
    public AlterTable(YPath path) {
        this(builder().setPath(path));
    }

    /**
     * Construct empty builder for alter table request.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Internal method: prepare request to send over network.
     */
    @Override
    public void writeTo(RpcClientRequestBuilder<TReqAlterTable.Builder, ?> requestBuilder) {
        TReqAlterTable.Builder builder = requestBuilder.body();

        super.writeTo(builder);

        if (schemaNode != null) {
            builder.setSchema(ByteString.copyFrom(schemaNode.toBinary()));
        }

        if (dynamic != null) {
            builder.setDynamic(dynamic);
        }

        if (upstreamReplicaId != null) {
            builder.setUpstreamReplicaId(RpcUtil.toProto(upstreamReplicaId));
        }

        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.toProto());
        }
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        if (schemaNode != null) {
            sb.append("Schema: ").append(schemaNode).append("; ");
        }
        if (dynamic != null) {
            sb.append("Dynamic: ").append(dynamic).append("; ");
        }
    }

    public YTreeBuilder toTree(YTreeBuilder builder) {
        return builder.key("path").apply(YPath.simple(getPath())::toTree)
                .when(dynamic != null, x -> x.key("dynamic").value(dynamic))
                .when(schemaNode != null, x -> x.key("schema").value(schemaNode))
                .when(
                        upstreamReplicaId != null,
                        x -> x.key("upstream_replica_id").value(Objects.requireNonNull(upstreamReplicaId).toString())
                );
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setSchema(schemaNode)
                .setDynamic(dynamic)
                .setTransactionalOptions(transactionalOptions)
                .setUpstreamReplicaId(upstreamReplicaId)
                .setMutatingOptions(mutatingOptions)
                .setPath(path)
                .setTabletRangeOptions(tabletRangeOptions)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    /**
     * Builder for {@link AlterTable}
     */
    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    public abstract static class BuilderBase<
            TBuilder extends BuilderBase<TBuilder>>
            extends TableReq.Builder<TBuilder, AlterTable> {
        @Nullable
        private YTreeNode schemaNode;
        @Nullable
        private Boolean dynamic;
        @Nullable
        private GUID upstreamReplicaId;
        @Nullable
        private TransactionalOptions transactionalOptions;

        public BuilderBase() {
        }

        public BuilderBase(BuilderBase<?> builder) {
            super(builder);
            if (builder.schemaNode != null) {
                schemaNode = YTree.deepCopy(builder.schemaNode);
            }
            dynamic = builder.dynamic;
            upstreamReplicaId = builder.upstreamReplicaId;
            if (builder.transactionalOptions != null) {
                transactionalOptions = new TransactionalOptions(builder.transactionalOptions);
            }
        }

        /**
         * If specified, it sets a new schema for the table
         * @return self
         */
        public TBuilder setSchema(@Nullable TableSchema schema) {
            if (schema != null) {
                this.schemaNode = schema.toYTree();
            }
            return self();
        }

        /**
         * If specified, it sets a new schema for the table
         * @return self
         */
        public TBuilder setSchema(@Nullable YTreeNode schema) {
            if (schema != null) {
                this.schemaNode = YTree.deepCopy(schema);
            }
            return self();
        }

        /**
         * If specified, it changes a static table to a dynamic table.
         * This setting can only be changed outside a transaction.
         * @return self
         */
        public TBuilder setDynamic(@Nullable Boolean dynamic) {
            this.dynamic = dynamic;
            return self();
        }

        /**
         * If specified, it changes the ID of the replica object on the metacluster.
         * For more information, @see <a href="https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/replicated-dynamic-tables">
         * Replicated dynamic tables
         * </a>
         * @return self
         */
        public TBuilder setUpstreamReplicaId(@Nullable GUID upstreamReplicaId) {
            this.upstreamReplicaId = upstreamReplicaId;
            return self();
        }

        /**
         * Set transactional options of the request.
         * @return self
         */
        public TBuilder setTransactionalOptions(@Nullable TransactionalOptions transactionalOptions) {
            this.transactionalOptions = transactionalOptions;
            return self();
        }

        @Override
        protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
            super.writeArgumentsLogString(sb);
            if (schemaNode != null) {
                sb.append("Schema: ").append(schemaNode).append("; ");
            }
            if (dynamic != null) {
                sb.append("Dynamic: ").append(dynamic).append("; ");
            }
        }

        public YTreeBuilder toTree(YTreeBuilder builder) {
            return builder.key("path").apply(YPath.simple(getPath())::toTree)
                    .when(dynamic != null, x -> x.key("dynamic").value(dynamic))
                    .when(schemaNode != null, x -> x.key("schema").value(schemaNode))
                    .when(
                            upstreamReplicaId != null,
                            x -> x.key("upstream_replica_id").value(upstreamReplicaId.toString())
                    );
        }

        /**
         * Construct {@link AlterTable} instance.
         */
        @Override
        public AlterTable build() {
            return new AlterTable(this);
        }
    }
}
