package ru.yandex.yt.ytclient.rpc.internal;

import java.util.Objects;

import ru.yandex.yt.rpc.TCredentialsExt;
import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcClientWrapper;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.RpcRequest;
import ru.yandex.yt.ytclient.rpc.RpcStreamConsumer;

/**
 * Декоратор для RpcClient, добавляющий аутентификацию по токену
 */
public class TokenAuthentication extends RpcClientWrapper {
    private final RpcCredentials credentials;

    public TokenAuthentication(RpcClient client, RpcCredentials credentials) {
        super(client);
        this.credentials = Objects.requireNonNull(credentials);
    }

    @Deprecated
    public TokenAuthentication(RpcClient client, String user, String token) {
        this(client, new RpcCredentials(user, token));
    }

    @Override
    public RpcClient withTokenAuthentication(RpcCredentials credentials) {
        return new TokenAuthentication(this.innerClient, credentials);
    }

    private void patchHeader(TRequestHeader.Builder header) {
        if (!header.hasUser()) {
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
                new RpcRequest<>(header.build(), request.body, request.attachments),
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
                new RpcRequest<>(header.build(), request.body, request.attachments),
                consumer,
                options
        );
    }

    @Override
    public String toString() {
        return credentials.getUser() + "@" + super.toString();
    }
}
