package ru.yandex.yt.rpc.protocol.rpc.getnode;

import java.util.UUID;

import ru.yandex.yt.rpc.protocol.RpcRequestMessage;
import ru.yandex.yt.rpc.protocol.rpc.RpcReqHeader;
import ru.yandex.yt.rpcproxy.TReqGetNode;

/**
 * @author valri
 */
public class RpcReqGetNode extends RpcRequestMessage {
    static {
        serviceName = "ApiService";
        methodName = "GetNode";
    }

    private final String path;
    private final TReqGetNode request;

    public RpcReqGetNode(final RpcReqHeader.Builder header, final String path, UUID uuid) {
        this.path = path;
        this.request = TReqGetNode.newBuilder().setPath(this.path).build();
        this.header = header.setMethod(methodName).setService(serviceName).setUuid(uuid).build();
        this.requestId = this.header.getUuid();
    }

    @Override
    protected byte[] getRequestBytes() {
        return request.toByteArray();
    }
}
