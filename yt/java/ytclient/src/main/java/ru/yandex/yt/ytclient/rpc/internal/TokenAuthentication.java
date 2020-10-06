package ru.yandex.yt.ytclient.rpc.internal;

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;

import ru.yandex.yt.rpc.TCredentialsExt;
import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequest;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;
import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;

/**
 * Декоратор для RpcClient, добавляющий аутентификацию по токену
 */
public class TokenAuthentication implements RpcClient {
    private final RpcClient client;
    private final RpcCredentials credentials;

    public TokenAuthentication(RpcClient client, RpcCredentials credentials) {
        this.client = Objects.requireNonNull(client);
        this.credentials = Objects.requireNonNull(credentials);
    }

    @Deprecated
    public TokenAuthentication(RpcClient client, String user, String token) {
        this(client, new RpcCredentials(user, token));
    }

    @Override
    public RpcClient withTokenAuthentication(RpcCredentials credentials) {
        return new TokenAuthentication(this.client, credentials);
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public void setDefaultHeader(TRequestHeader.Builder header) {
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
    public RpcClientRequestControl send(RpcClient sender, RpcClientRequest request, RpcClientResponseHandler handler) {
        return client.send(sender, request, handler);
    }

    @Override
    public RpcClientStreamControl startStream(RpcClient sender, RpcClientRequest request) {
        return client.startStream(sender, request);
    }

    @Override
    public String toString() {
        return credentials.getUser() + "@" + client.toString();
    }

    @Override
    public String destinationName() {
        return client.destinationName();
    }

    @Override
    public ScheduledExecutorService executor() {
        return client.executor();
    }
}
