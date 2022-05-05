package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import ru.yandex.yt.rpcproxy.ERowModificationType;
import ru.yandex.yt.rpcproxy.TReqModifyRows;
import ru.yandex.yt.ytclient.proxy.request.RequestBase;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.tables.TableSchema;

/**
 * Base class for all kinds of modify row requests.
 *
 * Users should create and use one of inheritors.
 *
 * @see MappedModifyRowsRequest
 * @see ModifyRowsRequest
 * @see PreparedModifyRowRequest
 */
public abstract class AbstractModifyRowsRequest<R extends AbstractModifyRowsRequest<R>> extends RequestBase<R> {
    protected final String path;
    protected final TableSchema schema;
    protected Boolean requireSyncReplica = null;
    protected final ArrayList<ERowModificationType> rowModificationTypes = new ArrayList<>();

    AbstractModifyRowsRequest(String path, TableSchema schema) {
        if (!schema.isWriteSchema()) {
            throw new IllegalArgumentException("ModifyRowsRequest requires a write schema");
        }
        this.path = Objects.requireNonNull(path);
        this.schema = Objects.requireNonNull(schema);
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

    public R setRequireSyncReplica(boolean requireSyncReplica) {
        this.requireSyncReplica = requireSyncReplica;
        return self();
    }

    public Optional<Boolean> getRequireSyncReplica() {
        return Optional.ofNullable(requireSyncReplica);
    }

    abstract void serializeRowsetTo(RpcClientRequestBuilder<TReqModifyRows.Builder, ?> builder);
}

