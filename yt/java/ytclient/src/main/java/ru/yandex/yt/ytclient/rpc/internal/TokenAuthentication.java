package ru.yandex.yt.ytclient.rpc.internal;

import java.util.Objects;

import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.rpcproxy.TCredentialsExt;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClientRequest;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestControl;
import ru.yandex.yt.ytclient.rpc.RpcClientResponseHandler;

/**
 * Декоратор для RpcClient, добавляющий аутентификацию по токену
 */
public class TokenAuthentication implements RpcClient {
    private final RpcClient client;
    private final String user;
    private final String token;

    public TokenAuthentication(RpcClient client, String user, String token) {
        this.client = Objects.requireNonNull(client);
        this.user = user;
        this.token = token;
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public RpcClientRequestControl send(RpcClientRequest request, RpcClientResponseHandler handler) {
        TRequestHeader.Builder header = request.header();
        if (!header.hasUser()) {
            header.setUser(user);
        }
        if (!header.hasExtension(TCredentialsExt.credentialsExt)) {
            header.setExtension(TCredentialsExt.credentialsExt, TCredentialsExt.newBuilder()
                    .setToken(token)
                    .setUserIp(getLocalAddress())
                    .build());
        }
        return client.send(request, handler);
    }

    private static String getLocalAddress() {
        // Пока используем 127.0.0.1
        return "127.0.0.1";
    }

    @Override
    public String toString() {
        return "TokenAuthentication@" + client.toString();
    }

    public String destinationName() {
        return client.destinationName();
    }
}
