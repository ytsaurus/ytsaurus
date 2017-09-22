package ru.yandex.yt.rpc.protocol.rpc.lookup;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.rpc.client.ValueType;
import ru.yandex.yt.rpc.client.requests.LookupReqInfo;
import ru.yandex.yt.rpc.protocol.rpc.RpcReqHeader;
import ru.yandex.yt.rpcproxy.ERowsetKind;
import ru.yandex.yt.rpcproxy.TReqVersionedLookupRows;
import ru.yandex.yt.rpcproxy.TRowsetDescriptor;

/**
 * @author valri
 */
public class RpcReqVersionedLookupRows extends RpcReqLookupMessage {
    private static Logger logger = LoggerFactory.getLogger(RpcReqVersionedLookupRows.class);

    protected TReqVersionedLookupRows request;
    private static ERowsetKind rowSetKind = ERowsetKind.UNVERSIONED;

    static {
        serviceName = "ApiService";
        methodName = "VersionedLookupRows";
    }

    public RpcReqVersionedLookupRows(RpcReqHeader.Builder header, LookupReqInfo reqInfo) {
        TReqVersionedLookupRows.Builder reqBuilder = TReqVersionedLookupRows
                .newBuilder().setPath(reqInfo.tableSchema.path);
        TRowsetDescriptor.Builder descriptor = TRowsetDescriptor
                .newBuilder()
                .setWireFormatVersion(reqInfo.wireFormat)
                .setRowsetKind(rowSetKind);
        for (Map.Entry<Short, String> row : reqInfo.tableSchema.idToName.entrySet()) {
            ValueType tt = reqInfo.tableSchema.nameToType.getOrDefault(row.getValue(), ValueType.ANY);
            descriptor.addColumns(TRowsetDescriptor.TColumnDescriptor
                    .newBuilder()
                    .setName(row.getValue())
                    .setType(tt.getValue())
                    .build());
        }
        reqInfo.tableSchema.columnsToLookup.forEach(reqBuilder::addColumns);
        if (reqInfo.timestamp != null) {
            reqBuilder.setTimestamp(reqInfo.timestamp);
        }
        if (reqInfo.keepMissingRows != null) {
            reqBuilder.setKeepMissingRows(reqInfo.keepMissingRows);
        }
        this.request = reqBuilder.setRowsetDescriptor(descriptor.build()).build();
        this.info = reqInfo;
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
