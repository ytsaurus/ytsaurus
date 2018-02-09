package ru.yandex.yt.rpc.protocol.rpc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.common.primitives.Bytes;

import ru.yandex.yt.TGuid;
import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.rpc.protocol.BUSPartable;
import ru.yandex.yt.rpcproxy.TCredentialsExt;

/**
 * @author valri
 */
public final class RpcReqHeader implements BUSPartable {
    private UUID uuid;
    private RpcMessageType type;
    private TRequestHeader requestHeader;

    private RpcReqHeader(String service, String method, RpcMessageType type, int protocolVersion,
                        String username, String userIp, String authToken, String domainName, UUID uuid)
    {
        this.type = type;
        this.uuid = uuid;
        final TRequestHeader.Builder headerBuilder = TRequestHeader.newBuilder();

        final TGuid.Builder uid = TGuid.newBuilder();
        uid.setFirst(this.uuid.getMostSignificantBits());
        uid.setSecond(this.uuid.getLeastSignificantBits());

        final TCredentialsExt.Builder credentialsBuilder = TCredentialsExt.newBuilder();
        credentialsBuilder.setUserIp(userIp);
        credentialsBuilder.setToken(authToken);
        credentialsBuilder.setDomain(domainName);

        headerBuilder.setExtension(TCredentialsExt.credentialsExt, credentialsBuilder.build());
        headerBuilder.setRequestId(uid.build());
        headerBuilder.setService(service);
        headerBuilder.setMethod(method);
        headerBuilder.setProtocolVersion(protocolVersion);
        headerBuilder.setUser(username);

        this.requestHeader = headerBuilder.build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public UUID getUuid() {
        return this.uuid;
    }

    @Override
    public List<Byte> getBusPart() {
        final List<Byte> header = new ArrayList<>();
        header.addAll(Bytes.asList(ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).putInt(
                this.type.getValue()).array()));
        header.addAll(Bytes.asList(requestHeader.toByteArray()));
        return header;
    }

    public static class Builder {
        private String service;
        private String method;
        private RpcMessageType type;
        private int protocolVersion;
        private String username;
        private String userIp;
        private String authToken;
        private String domainName;
        private UUID uuid;

        public Builder setService(String service) {
            this.service = service;
            return this;
        }

        public Builder setMethod(String method) {
            this.method = method;
            return this;
        }

        public Builder setType(RpcMessageType type) {
            this.type = type;
            return this;
        }

        public Builder setProtocolVersion(int protocolVersion) {
            this.protocolVersion = protocolVersion;
            return this;
        }

        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder setUserIp(String userIp) {
            this.userIp = userIp;
            return this;
        }

        public Builder setAuthToken(String authToken) {
            this.authToken = authToken;
            return this;
        }

        public Builder setDomainName(String domainName) {
            this.domainName = domainName;
            return this;
        }

        public Builder setUuid(UUID uid) {
            this.uuid = uid;
            return this;
        }

        public RpcReqHeader build() {
            return new RpcReqHeader(service, method, type, protocolVersion,
                    username, userIp, authToken, domainName, uuid);
        }
    }
}
