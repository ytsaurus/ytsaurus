package ru.yandex.yt.rpc.protocol.rpc;

import java.util.UUID;

import ru.yandex.yt.rpc.client.requests.LookupReqInfo;
import ru.yandex.yt.rpc.protocol.RpcRequestMessage;
import ru.yandex.yt.rpc.protocol.rpc.getnode.RpcReqGetNode;
import ru.yandex.yt.rpc.protocol.rpc.lookup.RpcReqLookupRows;
import ru.yandex.yt.rpc.protocol.rpc.lookup.RpcReqVersionedLookupRows;
import ru.yandex.yt.rpcproxy.TReqSelectRows;

/**
 * @author valri
 */
public class RpcReqFactory {
    private RpcReqHeader.Builder preparedBuilder = RpcReqHeader.newBuilder();

    public RpcReqFactory(String token, String user, String domain, int protocolVersion, String localIpAddress) {
        this.preparedBuilder.setAuthToken(token)
                .setDomainName(domain)
                .setProtocolVersion(protocolVersion)
                .setUserIp(localIpAddress)
                .setUsername(user)
                .setType(RpcMessageType.REQUEST);
    }

    public RpcRequestMessage createLookupRows(LookupReqInfo info) {
        return new RpcReqLookupRows(this.preparedBuilder, info);
    }

    public RpcRequestMessage createVersionedLookupRows(LookupReqInfo info) {
        return new RpcReqVersionedLookupRows(this.preparedBuilder, info);
    }

    public RpcRequestMessage createGetNode(String path, UUID requestId) {
        return new RpcReqGetNode(this.preparedBuilder, path, requestId);
    }

    // TReqSelectRows
    class RcpSelectRows extends RpcRequestMessage {
        private final TReqSelectRows request;

        private RcpSelectRows(final RpcReqHeader.Builder header, final String query, UUID uuid) {
            String serviceName = "ApiService";
            String methodName = "SelectRows";

            this.request = TReqSelectRows.newBuilder()
                .setQuery(query)
                .build();

            this.header = header
                .setService(serviceName)
                .setMethod(methodName)
                .setUuid(uuid)
                .build();

            this.requestId = this.header.getUuid();
        }

        @Override
        protected byte[] getRequestBytes() {
            return request.toByteArray();
        }
    };

    public RpcRequestMessage createSelectRows(String query, UUID requestId) {
        return new RcpSelectRows(this.preparedBuilder, query, requestId);
    }
}
