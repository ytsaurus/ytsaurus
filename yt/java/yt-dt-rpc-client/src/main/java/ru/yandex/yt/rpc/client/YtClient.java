package ru.yandex.yt.rpc.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;

import ru.yandex.yt.rpc.TResponseHeader;
import ru.yandex.yt.rpc.client.responses.VersionedLookupRow;
import ru.yandex.yt.rpc.client.schema.ColumnSchema;
import ru.yandex.yt.rpc.client.schema.TableSchema;
import ru.yandex.yt.rpc.protocol.bus.BusPackage;
import ru.yandex.yt.rpc.protocol.proto.ProtobufHelpers;
import ru.yandex.yt.rpc.protocol.rpc.lookup.RpcRspLookupRows;
import ru.yandex.yt.rpc.protocol.rpc.lookup.RpcRspVersionedLookupRows;
import ru.yandex.yt.rpcproxy.TRowsetDescriptor;
import ru.yandex.yt.rpcproxy.TRspGetNode;
import ru.yandex.yt.rpcproxy.TRspLookupRows;
import ru.yandex.yt.rpcproxy.TRspSelectRows;
import ru.yandex.yt.rpcproxy.TRspVersionedLookupRows;

/**
 * @author valri
 */
abstract class YtClient {
    public abstract void close() throws InterruptedException;

    protected abstract Logger getLogger();

    String getGetNode(BusPackage pack) {
        TResponseHeader responseHeader = ProtobufHelpers.parseResponseHeader(pack);
        if (ProtobufHelpers.validateHeader(pack, responseHeader)) {
            TRspGetNode response = ProtobufHelpers.getProtoFromRsp(
                    pack, TRspGetNode.parser(), TRspGetNode.class);
            if (response != null) {
                getLogger().debug("Successfully received answer for request requestId={}.",
                        ProtobufHelpers.getUuidFromHeader(responseHeader));
                return new String(response.getValue().toByteArray());
            }
        }
        return null;
    }

    List<Map<String, Object>> getLookupRows(BusPackage pack, TableSchema schema) {
        TResponseHeader responseHeader = ProtobufHelpers.parseResponseHeader(pack);
        if (ProtobufHelpers.validateHeader(pack, responseHeader)) {
            TRspLookupRows tRspLookupRows = ProtobufHelpers.getProtoFromRsp(
                    pack, TRspLookupRows.parser(), TRspLookupRows.class);
            if (tRspLookupRows != null) {
                getLogger().debug("Successfully received answer for request requestId={}.",
                        ProtobufHelpers.getUuidFromHeader(responseHeader));
                RpcRspLookupRows resp = new RpcRspLookupRows(schema);
                return resp.parseRpcResponse(pack.getBlobPart());
            }
        }
        return null;
    }

    // TODO: remove copy-paste
    List<Map<String, Object>> getSelectRows(BusPackage pack) {
        TResponseHeader responseHeader = ProtobufHelpers.parseResponseHeader(pack);
        if (ProtobufHelpers.validateHeader(pack, responseHeader)) {
            TRspSelectRows tRspLookupRows = ProtobufHelpers.getProtoFromRsp(
                    pack, TRspSelectRows.parser(), TRspSelectRows.class);
            if (tRspLookupRows != null) {
                getLogger().debug("Successfully received answer for request requestId={}.",
                        ProtobufHelpers.getUuidFromHeader(responseHeader));

                List<ColumnSchema> columnSchema = new ArrayList<>(tRspLookupRows.getRowsetDescriptor().getColumnsList().size());
                int id = 0;
                for (TRowsetDescriptor.TColumnDescriptor column : tRspLookupRows.getRowsetDescriptor().getColumnsList()) {
                    columnSchema.add(new ColumnSchema((short)id, column.getName(), ValueType.fromType((short)column.getType())));
                    id ++;
                }
                TableSchema schema = new TableSchema("unused", columnSchema);

                RpcRspLookupRows resp = new RpcRspLookupRows(schema);
                return resp.parseRpcResponse(pack.getBlobPart());
            }
        }
        return null;
    }

    List<VersionedLookupRow> getVersionedLookupRows(BusPackage pack, TableSchema schema) {
        TResponseHeader responseHeader = ProtobufHelpers.parseResponseHeader(pack);
        if (ProtobufHelpers.validateHeader(pack, responseHeader)) {
            TRspVersionedLookupRows tRspLookupRows = ProtobufHelpers.getProtoFromRsp(
                    pack, TRspVersionedLookupRows.parser(),
                    TRspVersionedLookupRows.class);
            if (tRspLookupRows != null) {
                getLogger().debug("Successfully received answer for request requestId={}.",
                        ProtobufHelpers.getUuidFromHeader(responseHeader));
                RpcRspVersionedLookupRows resp = new RpcRspVersionedLookupRows(tRspLookupRows.getRowsetDescriptor(),
                                                                               schema);
                return resp.parseRpcResponse(pack.getBlobPart());
            }
        }
        return null;
    }
}
