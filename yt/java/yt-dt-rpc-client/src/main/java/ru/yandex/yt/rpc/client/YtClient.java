package ru.yandex.yt.rpc.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;

import ru.yandex.yt.rpc.client.responses.VersionedLookupRow;
import ru.yandex.yt.rpc.client.schema.ColumnSchema;
import ru.yandex.yt.rpc.client.schema.TableSchema;
import ru.yandex.yt.rpc.protocol.bus.BusPackage;
import ru.yandex.yt.rpc.protocol.proto.ProtobufHelpers;
import ru.yandex.yt.rpc.protocol.rpc.lookup.RpcRspLookupRows;
import ru.yandex.yt.rpc.protocol.rpc.lookup.RpcRspVersionedLookupRows;

/**
 * @author valri
 */
abstract class YtClient {
    public abstract void close() throws InterruptedException;

    protected abstract Logger getLogger();

    String getGetNode(BusPackage pack) {
        protocol.Rpc.TResponseHeader responseHeader = ProtobufHelpers.parseResponseHeader(pack);
        if (ProtobufHelpers.validateHeader(pack, responseHeader)) {
            protocol.ApiService.TRspGetNode response = ProtobufHelpers.getProtoFromRsp(
                    pack, protocol.ApiService.TRspGetNode.parser(), protocol.ApiService.TRspGetNode.class);
            if (response != null) {
                getLogger().debug("Successfully received answer for request requestId={}.",
                        ProtobufHelpers.getUuidFromHeader(responseHeader));
                return new String(response.getData().toByteArray());
            }
        }
        return null;
    }

    List<Map<String, Object>> getLookupRows(BusPackage pack, TableSchema schema) {
        protocol.Rpc.TResponseHeader responseHeader = ProtobufHelpers.parseResponseHeader(pack);
        if (ProtobufHelpers.validateHeader(pack, responseHeader)) {
            protocol.ApiService.TRspLookupRows tRspLookupRows = ProtobufHelpers.getProtoFromRsp(
                    pack, protocol.ApiService.TRspLookupRows.parser(), protocol.ApiService.TRspLookupRows.class);
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
        protocol.Rpc.TResponseHeader responseHeader = ProtobufHelpers.parseResponseHeader(pack);
        if (ProtobufHelpers.validateHeader(pack, responseHeader)) {
            protocol.ApiService.TRspSelectRows tRspLookupRows = ProtobufHelpers.getProtoFromRsp(
                    pack, protocol.ApiService.TRspSelectRows.parser(), protocol.ApiService.TRspSelectRows.class);
            if (tRspLookupRows != null) {
                getLogger().debug("Successfully received answer for request requestId={}.",
                        ProtobufHelpers.getUuidFromHeader(responseHeader));

                List<ColumnSchema> columnSchema = new ArrayList<>(tRspLookupRows.getRowsetDescriptor().getColumnsList().size());
                int id = 0;
                for (protocol.ApiService.TRowsetDescriptor.TColumnDescriptor column : tRspLookupRows.getRowsetDescriptor().getColumnsList()) {
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
        protocol.Rpc.TResponseHeader responseHeader = ProtobufHelpers.parseResponseHeader(pack);
        if (ProtobufHelpers.validateHeader(pack, responseHeader)) {
            protocol.ApiService.TRspVersionedLookupRows tRspLookupRows = ProtobufHelpers.getProtoFromRsp(
                    pack, protocol.ApiService.TRspVersionedLookupRows.parser(),
                    protocol.ApiService.TRspVersionedLookupRows.class);
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
