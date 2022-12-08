package tech.ytsaurus.client.rpc;

import java.util.Objects;

import ru.yandex.yt.rpc.TCredentialsExt;
import ru.yandex.yt.rpc.TRequestHeader;

/**
 * Декоратор для RpcClient, добавляющий аутентификацию по токену
 */
public class TokenAuthentication extends RpcClientWrapper {
    private final RpcCredentials credentials;

    public TokenAuthentication(RpcClient client, RpcCredentials credentials) {
        super(client);
        this.credentials = Objects.requireNonNull(credentials);
    }

    @Override
    public RpcClient withTokenAuthentication(RpcCredentials credentials) {
        return new TokenAuthentication(this.innerClient, credentials);
    }

    private void patchHeader(TRequestHeader.Builder header) {
        if (!header.hasUser() && credentials.getUser() != null) {
            header.setUser(credentials.getUser());
        }
        if (!header.hasExtension(TCredentialsExt.credentialsExt)) {
            header.setExtension(TCredentialsExt.credentialsExt, TCredentialsExt.newBuilder()
                    .setToken(credentials.getToken())
                    .build());
        }
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
