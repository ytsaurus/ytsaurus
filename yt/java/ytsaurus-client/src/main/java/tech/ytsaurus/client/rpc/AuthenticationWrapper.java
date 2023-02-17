package tech.ytsaurus.client.rpc;

import java.util.Objects;

import tech.ytsaurus.rpc.TCredentialsExt;
import tech.ytsaurus.rpc.TRequestHeader;

/**
 * Decorator for RpcClient that adds authentication
 */
public class AuthenticationWrapper extends RpcClientWrapper {
    private final RpcCredentials credentials;

    public AuthenticationWrapper(RpcClient client, RpcCredentials credentials) {
        super(client);
        this.credentials = Objects.requireNonNull(credentials);
    }

    @Override
    public RpcClient withAuthentication(RpcCredentials credentials) {
        return new AuthenticationWrapper(this.innerClient, credentials);
    }

    private void patchHeader(TRequestHeader.Builder header) {
        if (!header.hasUser() && credentials.getUser() != null) {
            header.setUser(credentials.getUser());
        }
        if (header.hasExtension(TCredentialsExt.credentialsExt)) {
            return;
        }
        TCredentialsExt.Builder credentialsExtBuilder = TCredentialsExt.newBuilder();
        credentials.getServiceTicketAuth().ifPresent(serviceTicketAuth ->
                credentialsExtBuilder.setServiceTicket(serviceTicketAuth.issueServiceTicket()));
        if (credentials.getToken() != null) {
            credentialsExtBuilder.setToken(credentials.getToken());
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
        return credentials.getUser() + "@" + super.toString();
    }
}
