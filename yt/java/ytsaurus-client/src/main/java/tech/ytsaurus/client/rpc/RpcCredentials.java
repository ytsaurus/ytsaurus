package tech.ytsaurus.client.rpc;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class RpcCredentials {
    private String user;
    private String token;
    private ServiceTicketAuth serviceTicketAuth;

    public RpcCredentials() {
        this(null, null);
    }

    public RpcCredentials(String user, String token) {
        this.user = user;
        this.token = token;
    }

    public RpcCredentials(ServiceTicketAuth serviceTicketAuth) {
        this.serviceTicketAuth = serviceTicketAuth;
    }

    public String getUser() {
        return user;
    }

    public String getToken() {
        return token;
    }

    public Optional<ServiceTicketAuth> getServiceTicketAuth() {
        return Optional.ofNullable(serviceTicketAuth);
    }

    public RpcCredentials setUser(String user) {
        this.user = user;
        return this;
    }

    public RpcCredentials setToken(String token) {
        this.token = token;
        return this;
    }

    public void setServiceTicketAuth(ServiceTicketAuth serviceTicketAuth) {
        this.serviceTicketAuth = serviceTicketAuth;
    }

    public boolean isEmpty() {
        return token == null;
    }

    /**
     * Load authentication info from environment.
     *
     * <p>
     * Username is searched in following places (in that order):
     * 1. YT_USER environment variable
     * 2. Current username got from system
     *
     * <p>
     * User token is searched in following places (in that order):
     * 1. YT_TOKEN environment variable
     * 2. ~/.yt/token file
     *
     * @throws RuntimeException if username or user token cannot be obtained.
     */
    public static RpcCredentials loadFromEnvironment() {
        String userName = System.getenv("YT_USER");
        if (userName == null) {
            userName = System.getProperty("user.name");
        }

        String token = System.getenv("YT_TOKEN");
        if (token == null || token.isEmpty()) {
            Path tokenPath = Paths.get(System.getProperty("user.home"), ".yt", "token");
            try (BufferedReader reader = Files.newBufferedReader(tokenPath)) {
                token = reader.readLine();
                if (token.isEmpty()) {
                    throw new RuntimeException("~/.yt/token is missing YT token (first line of file is empty)");
                }
            } catch (IOException exception) {
                throw new RuntimeException("Cannot load token from ~/.yt/token", exception);
            }
        }
        return new RpcCredentials(userName, token);
    }
}
