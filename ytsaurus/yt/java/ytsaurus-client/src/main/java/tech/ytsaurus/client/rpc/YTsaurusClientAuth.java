package tech.ytsaurus.client.rpc;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class YTsaurusClientAuth {
    private final String user;
    private final String token;
    private final ServiceTicketAuth serviceTicketAuth;
    private final UserTicketAuth userTicketAuth;

    private YTsaurusClientAuth(Builder builder) {
        this(builder.user, builder.token, builder.serviceTicketAuth, builder.userTicketAuth);
    }

    private YTsaurusClientAuth(
            String user,
            String token,
            ServiceTicketAuth serviceTicketAuth,
            UserTicketAuth userTicketAuth
    ) {
        this.user = user;
        this.token = token;
        this.serviceTicketAuth = serviceTicketAuth;
        this.userTicketAuth = userTicketAuth;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static YTsaurusClientAuth empty() {
        return new YTsaurusClientAuth(null, null, null, null);
    }

    public Optional<String> getUser() {
        return Optional.ofNullable(user);
    }

    public Optional<String> getToken() {
        return Optional.ofNullable(token);
    }

    public Optional<ServiceTicketAuth> getServiceTicketAuth() {
        return Optional.ofNullable(serviceTicketAuth);
    }

    public Optional<UserTicketAuth> getUserTicketAuth() {
        return Optional.ofNullable(userTicketAuth);
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
    public static YTsaurusClientAuth loadUserAndTokenFromEnvironment() {
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
        return builder()
                .setUser(userName)
                .setToken(token)
                .build();
    }

    public static class Builder {
        private String user;
        private String token;
        private ServiceTicketAuth serviceTicketAuth;
        private UserTicketAuth userTicketAuth;

        public YTsaurusClientAuth build() {
            return new YTsaurusClientAuth(this);
        }

        public Builder setUser(String user) {
            this.user = user;
            return this;
        }

        public Builder setToken(String token) {
            this.token = token;
            return this;
        }

        public Builder setServiceTicketAuth(ServiceTicketAuth serviceTicketAuth) {
            this.serviceTicketAuth = serviceTicketAuth;
            return this;
        }

        public Builder setUserTicketAuth(UserTicketAuth userTicketAuth) {
            this.userTicketAuth = userTicketAuth;
            return this;
        }
    }
}
