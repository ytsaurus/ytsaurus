package tech.ytsaurus.client.rpc;

import java.util.Objects;

import tech.ytsaurus.rpc.TCredentialsExt;
import tech.ytsaurus.rpc.TRequestHeader;

/**
 * Decorator for RpcClient that adds authentication
 */
public class AuthenticationWrapper extends RpcClientWrapper {
    private final YTsaurusClientAuth clientAuth;

    public AuthenticationWrapper(RpcClient client, YTsaurusClientAuth auth) {
        super(client);
        this.clientAuth = Objects.requireNonNull(auth);
    }

    @Override
    public RpcClient withAuthentication(YTsaurusClientAuth auth) {
        return new AuthenticationWrapper(this.innerClient, auth);
    }

    private void patchHeader(TRequestHeader.Builder header) {
        if (!header.hasUser()) {
            clientAuth.getUser().ifPresent(header::setUser);
        }
        if (header.hasExtension(TCredentialsExt.credentialsExt)) {
            return;
        }
        TCredentialsExt.Builder credentialsExtBuilder = TCredentialsExt.newBuilder();

        var serviceTicketAuth = clientAuth.getServiceTicketAuth();
        var userTicketAuth = clientAuth.getUserTicketAuth();
        if (serviceTicketAuth.isPresent()) {
            credentialsExtBuilder.setServiceTicket(serviceTicketAuth.get().issueServiceTicket());
        } else if (userTicketAuth.isPresent()) {
            credentialsExtBuilder.setUserTicket(userTicketAuth.get().issueUserTicket());
        } else {
            clientAuth.getToken().ifPresent(credentialsExtBuilder::setToken);
        }
        header.setExtension(TCredentialsExt.credentialsExt, credentialsExtBuilder.build());
    }

    @Override
    public RpcClientRequestControl send(
            RpcClient sender,
            RpcRequest<?> request,
            RpcClientResponseHandler handler,
            RpcOptions options
    ) {
        TRequestHeader.Builder header = request.header.toBuilder();
        patchHeader(header);
        return super.send(
                sender,
                request.copy(header.build()),
                handler,
                options
        );
    }

    @Override
    public RpcClientStreamControl startStream(
            RpcClient sender,
            RpcRequest<?> request,
            RpcStreamConsumer consumer,
            RpcOptions options
    ) {
        TRequestHeader.Builder header = request.header.toBuilder();
        patchHeader(header);
        return super.startStream(
                sender,
                request.copy(header.build()),
                consumer,
                options
        );
    }

    @Override
    public String toString() {
        return clientAuth.getUser() + "@" + super.toString();
    }
}
