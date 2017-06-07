package ru.yandex.yt.rpc.protocol.rpc.lookup;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.ApiService;

import ru.yandex.yt.rpc.client.ValueType;
import ru.yandex.yt.rpc.client.requests.LookupReqInfo;
import ru.yandex.yt.rpc.protocol.rpc.RpcReqHeader;

/**
 * @author valri
 */
public class RpcReqLookupRows extends RpcReqLookupMessage {
    private static Logger logger = LogManager.getLogger(RpcReqLookupRows.class);

    protected ApiService.TReqLookupRows request;
    private ApiService.ERowsetKind rowSetKind = ApiService.ERowsetKind.UNVERSIONED;

    static {
        serviceName = "ApiService";
        methodName = "LookupRows";
    }

    public RpcReqLookupRows(RpcReqHeader.Builder header, LookupReqInfo info) {
        ApiService.TReqLookupRows.Builder reqBuilder = ApiService.TReqLookupRows
                .newBuilder().setPath(info.tableSchema.path);
        ApiService.TRowsetDescriptor.Builder descriptor = ApiService.TRowsetDescriptor
                .newBuilder()
                .setWireFormatVersion(info.wireFormat)
                .setRowsetKind(rowSetKind);
        for (Map.Entry<Short, String> row : info.tableSchema.idToName.entrySet()) {
            ValueType tt = info.tableSchema.nameToType.getOrDefault(row.getValue(), ValueType.ANY);
            descriptor.addColumns(ApiService.TRowsetDescriptor.TColumnDescriptor
                    .newBuilder()
                    .setName(row.getValue())
                    .setType(tt.getValue())
                    .build());
        }
        info.tableSchema.columnsToLookup.forEach(reqBuilder::addColumns);

        if (info.timestamp != null) {
            reqBuilder.setTimestamp(info.timestamp);
        }
        if (info.keepMissingRows != null) {
            reqBuilder.setKeepMissingRows(info.keepMissingRows);
        }
        this.request = reqBuilder.setRowsetDescriptor(descriptor.build()).build();
        this.info = info;
        this.wholePackSize = getPackageSize();
        this.header = header
                .setService(serviceName)
                .setMethod(methodName)
                .setUuid(info.uuid)
                .build();
        this.requestId = this.header.getUuid();
    }

    @Override
    public byte[] getRequestBytes() {
        return request.toByteArray();
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
