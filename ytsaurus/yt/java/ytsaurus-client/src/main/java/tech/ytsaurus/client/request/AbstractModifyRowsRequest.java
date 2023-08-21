package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.client.SerializationResolver;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.rpcproxy.ERowModificationType;
import tech.ytsaurus.rpcproxy.TReqModifyRows;

/**
 * Base class for all kinds of modify row requests.
 * <p>
 * Users should create and use one of inheritors.
 */
public abstract class AbstractModifyRowsRequest<
        TBuilder extends AbstractModifyRowsRequest.Builder<TBuilder, TRequest>,
        TRequest extends AbstractModifyRowsRequest<TBuilder, TRequest>> extends RequestBase<TBuilder, TRequest> {
    protected final String path;
    protected final TableSchema schema;
    @Nullable
    protected Boolean requireSyncReplica;
    protected final List<ERowModificationType> rowModificationTypes;

    protected AbstractModifyRowsRequest(Builder<?, ?> builder) {
        super(builder);
        this.path = Objects.requireNonNull(builder.path);
        this.schema = Objects.requireNonNull(builder.schema);
        if (!schema.isWriteSchema()) {
            throw new IllegalArgumentException("ModifyRowsRequest requires a write schema");
        }
        this.requireSyncReplica = builder.requireSyncReplica;
        this.rowModificationTypes = new ArrayList<>(builder.rowModificationTypes);
    }

    public String getPath() {
        return path;
    }

    public TableSchema getSchema() {
        return schema;
    }

    public List<ERowModificationType> getRowModificationTypes() {
        return Collections.unmodifiableList(rowModificationTypes);
    }

    public Optional<Boolean> getRequireSyncReplica() {
        return Optional.ofNullable(requireSyncReplica);
    }

    public abstract void convertValues(SerializationResolver serializationResolver);

    public abstract void serializeRowsetTo(RpcClientRequestBuilder<TReqModifyRows.Builder, ?> builder);

    public abstract static class Builder<
            TBuilder extends Builder<TBuilder, TRequest>,
            TRequest extends RequestBase<?, TRequest>> extends RequestBase.Builder<TBuilder, TRequest> {
        @Nullable
        protected String path;
        @Nullable
        protected TableSchema schema;
        @Nullable
        protected Boolean requireSyncReplica = null;
        protected final List<ERowModificationType> rowModificationTypes = new ArrayList<>();

        public TBuilder setPath(String path) {
            this.path = path;
            return self();
        }

        public TBuilder setSchema(TableSchema schema) {
            this.schema = schema;
            return self();
        }

        public TBuilder setRowModificationTypes(List<ERowModificationType> rowModificationTypes) {
            this.rowModificationTypes.addAll(rowModificationTypes);
            return self();
        }

        public TBuilder addRowModificationType(ERowModificationType rowModificationType) {
            this.rowModificationTypes.add(rowModificationType);
            return self();
        }

        public TBuilder setRequireSyncReplica(@Nullable Boolean requireSyncReplica) {
            this.requireSyncReplica = requireSyncReplica;
            return self();
        }

        public List<ERowModificationType> getRowModificationTypes() {
            return rowModificationTypes;
        }

        public String getPath() {
            return Objects.requireNonNull(path);
        }

        public TableSchema getSchema() {
            return Objects.requireNonNull(schema);
        }
    }
}

