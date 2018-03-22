package ru.yandex.yt.ytclient.rpc;

public class RpcCredentials {
    private String user;
    private String token;

    public RpcCredentials() {
        this(null, null);
    }

    public RpcCredentials(String user, String token) {
        this.user = user;
        this.token = token;
    }

    public String getUser() {
        return user;
    }

    public String getToken() {
        return token;
    }

    public RpcCredentials setUser(String user) {
        this.user = user;
        return this;
    }

    public RpcCredentials setToken(String token) {
        this.token = token;
        return this;
    }

    public boolean isEmpty() {
        return user == null || token == null;
    }
}
