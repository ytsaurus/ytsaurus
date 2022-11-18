package ru.yandex.yt.ytclient.proxy;

import java.util.List;

import tech.ytsaurus.client.rpc.Compression;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.rpcproxy.ERowModificationType;
import ru.yandex.yt.ytclient.tables.TableSchema;

/**
 * Immutable row modification request that contains serialized and compressed rowset
 * <p>
 * It is useful when same request is performed multiple times (e.g. to different clusters)
 * and saves CPU resources used on serialization and compressing.
 * <p>
 * It should be built using {@link tech.ytsaurus.client.request.PreparableModifyRowsRequest} method
 * <p>
 * Compression used in this request have to match with compression of the client otherwise exception will be thrown
 * when trying to execute this request.
 */
@NonNullApi
@NonNullFields
public class PreparedModifyRowRequest
        extends tech.ytsaurus.client.request.PreparedModifyRowRequest.BuilderBase<PreparedModifyRowRequest> {
    PreparedModifyRowRequest(
            String path,
            TableSchema schema,
            List<ERowModificationType> rowModificationTypes,
            Compression codecId,
            List<byte[]> compressedAttachments
    ) {
        setPath(path).setSchema(schema).setRowModificationTypes(rowModificationTypes)
                .setCodecId(codecId).setCompressedAttachments(compressedAttachments);
    }

    @Override
    protected PreparedModifyRowRequest self() {
        return this;
    }

    @Override
    public tech.ytsaurus.client.request.PreparedModifyRowRequest build() {
        return new tech.ytsaurus.client.request.PreparedModifyRowRequest(this);
    }
}
