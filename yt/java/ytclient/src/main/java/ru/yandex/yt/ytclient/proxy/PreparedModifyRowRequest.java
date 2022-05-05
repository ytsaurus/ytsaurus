package ru.yandex.yt.ytclient.proxy;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import ru.yandex.yt.rpcproxy.ERowModificationType;
import ru.yandex.yt.rpcproxy.TReqModifyRows;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;
import ru.yandex.yt.ytclient.rpc.RpcCompression;
import ru.yandex.yt.ytclient.rpc.RpcUtil;
import ru.yandex.yt.ytclient.rpc.internal.Compression;
import ru.yandex.yt.ytclient.tables.TableSchema;

/**
 * Immutable row modification request that contains serialized and compressed rowset
 *
 * It is useful when same request is performed multiple times (e.g. to different clusters)
 * and saves CPU resources used on serialization and compressing.
 *
 * It should be built using {@link PreparableModifyRowsRequest#prepare(RpcCompression)} method
 *
 * Compression used in this request have to match with compression of the client otherwise exception will be thrown
 * when trying to execute this request.
 */
public class PreparedModifyRowRequest extends AbstractModifyRowsRequest<PreparedModifyRowRequest> {
    private final Compression codecId;
    private final List<byte[]> compressedAttachments;

    PreparedModifyRowRequest(
            String path,
            TableSchema schema,
            List<ERowModificationType> rowModificationTypes,
            Compression codecId,
            List<byte[]> compressedAttachments
    ) {
        super(path, schema);
        this.rowModificationTypes.addAll(rowModificationTypes);
        this.codecId = codecId;
        this.compressedAttachments = compressedAttachments;
    }

    @Override
    void serializeRowsetTo(RpcClientRequestBuilder<TReqModifyRows.Builder, ?> builder) {
        builder.setCompressedAttachments(codecId, compressedAttachments);
    }

    @Nonnull
    @Override
    protected PreparedModifyRowRequest self() {
        return this;
    }
}

abstract class PreparableModifyRowsRequest<R extends PreparableModifyRowsRequest<R>>
        extends AbstractModifyRowsRequest<R> {
    PreparableModifyRowsRequest(String path, TableSchema schema) {
        super(path, schema);
    }

    @Override
    void serializeRowsetTo(RpcClientRequestBuilder<TReqModifyRows.Builder, ?> builder) {
        serializeRowsetTo(builder.attachments());
    }

    abstract void serializeRowsetTo(List<byte[]> attachments);

    /**
     * Serialize and compress rowset.
     */
    public PreparedModifyRowRequest prepare(RpcCompression rpcCompression) {
        List<byte[]> attachments = new ArrayList<>();
        serializeRowsetTo(attachments);
        Compression codecId = rpcCompression.getRequestCodecId().orElse(Compression.fromValue(0));
        List<byte[]> preparedMessage = RpcUtil.createCompressedAttachments(attachments, codecId);

        return new PreparedModifyRowRequest(path, schema, rowModificationTypes, codecId, preparedMessage);
    }
}
