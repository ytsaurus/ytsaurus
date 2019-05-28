package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import ru.yandex.yt.rpcproxy.ERowModificationType;
import ru.yandex.yt.ytclient.tables.TableSchema;

public abstract class AbstractModifyRowsRequest {
    protected final String path;
    protected final TableSchema schema;
    protected Boolean requireSyncReplica = null;
    protected final List<ERowModificationType> rowModificationTypes = new ArrayList<>();

    public AbstractModifyRowsRequest(String path, TableSchema schema) {
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

    public AbstractModifyRowsRequest setRequireSyncReplica(boolean requireSyncReplica) {
        this.requireSyncReplica = requireSyncReplica;
        return this;
    }

    public Optional<Boolean> getRequireSyncReplica() {
        return Optional.ofNullable(requireSyncReplica);
    }

    //

    public abstract void serializeRowsetTo(List<byte[]> attachments);
}
