package ru.yandex.yt.rpc.protocol.rpc.lookup;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.rpc.client.ValueType;
import ru.yandex.yt.rpc.client.requests.LookupReqInfo;
import ru.yandex.yt.rpc.protocol.rpc.RpcReqHeader;
import ru.yandex.yt.rpcproxy.ERowsetKind;
import ru.yandex.yt.rpcproxy.TReqLookupRows;
import ru.yandex.yt.rpcproxy.TRowsetDescriptor;

/**
 * @author valri
 */
public class RpcReqLookupRows extends RpcReqLookupMessage {
    private static Logger logger = LoggerFactory.getLogger(RpcReqLookupRows.class);

    protected TReqLookupRows request;
    private ERowsetKind rowSetKind = ERowsetKind.RK_UNVERSIONED;

    static {
        serviceName = "ApiService";
        methodName = "LookupRows";
    }

    public RpcReqLookupRows(RpcReqHeader.Builder header, LookupReqInfo info) {
        TReqLookupRows.Builder reqBuilder = TReqLookupRows
                .newBuilder().setPath(info.tableSchema.path);
        TRowsetDescriptor.Builder descriptor = TRowsetDescriptor
                .newBuilder()
                .setWireFormatVersion(info.wireFormat)
                .setRowsetKind(rowSetKind);
        for (Map.Entry<Short, String> row : info.tableSchema.idToName.entrySet()) {
            ValueType tt = info.tableSchema.nameToType.getOrDefault(row.getValue(), ValueType.ANY);
            descriptor.addColumns(TRowsetDescriptor.TColumnDescriptor
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
