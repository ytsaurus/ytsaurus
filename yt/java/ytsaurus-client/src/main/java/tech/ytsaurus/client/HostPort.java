package tech.ytsaurus.client;

import java.net.InetSocketAddress;
import java.util.Objects;

class HostPort {
    final String host;
    final int port;

    private HostPort(String host, int port) {
        this.host = Objects.requireNonNull(host);
        this.port = port;
    }

    public InetSocketAddress toInetSocketAddress() {
        return new InetSocketAddress(host, port);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public static HostPort parse(String hostPortString) {
        String host;
        String portString = null;

        if (hostPortString.startsWith("[")) {
            String[] hostAndPort = getHostAndPortFromBracketedHost(hostPortString);
            host = hostAndPort[0];
            portString = hostAndPort[1];
        } else {
            int colonPos = hostPortString.indexOf(':');
            if (colonPos >= 0 && hostPortString.indexOf(':', colonPos + 1) == -1) {
                // Exactly 1 colon. Split into host:port.
                host = hostPortString.substring(0, colonPos);
                portString = hostPortString.substring(colonPos + 1);
            } else {
                // 0 or 2+ colons. Bare hostname or IPv6 literal.
                host = hostPortString;
            }
        }

        int port = 9013;
        if (portString != null && !portString.isEmpty()) {
            // Try to parse the whole port string as a number.
            // JDK7 accepts leading plus signs. We don't want to.
            checkArgument(!portString.startsWith("+"), "Unparseable port number: %s", hostPortString);
            try {
                port = Integer.parseInt(portString);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Unparseable port number: " + hostPortString);
            }
            checkArgument(isValidPort(port), "Port number out of range: %s", hostPortString);
        }

        return new HostPort(host, port);
    }

    private static String[] getHostAndPortFromBracketedHost(String hostPortString) {
        checkArgument(
                hostPortString.charAt(0) == '[',
                "Bracketed host-port string must start with a bracket: %s",
                hostPortString
        );
        int colonIndex = hostPortString.indexOf(':');
        int closeBracketIndex = hostPortString.lastIndexOf(']');
        checkArgument(
                colonIndex > -1 && closeBracketIndex > colonIndex,
                "Invalid bracketed host/port: %s",
                hostPortString
        );

        String host = hostPortString.substring(1, closeBracketIndex);
        if (closeBracketIndex + 1 == hostPortString.length()) {
            return new String[]{host, ""};
        } else {
            checkArgument(
                    hostPortString.charAt(closeBracketIndex + 1) == ':',
                    "Only a colon may follow a close bracket: %s",
                    hostPortString
            );
            for (int i = closeBracketIndex + 2; i < hostPortString.length(); ++i) {
                checkArgument(
                        Character.isDigit(hostPortString.charAt(i)),
                        "Port must be numeric: %s",
                        hostPortString
                );
            }
            return new String[]{host, hostPortString.substring(closeBracketIndex + 2)};
        }
    }

    private static void checkArgument(
            boolean expression,
            String errorMessageTemplate,
            Object... errorMessageArgs) {
        if (!expression) {
            throw new IllegalArgumentException(String.format(errorMessageTemplate, errorMessageArgs));
        }
    }

    private static boolean isValidPort(int port) {
        return port >= 0 && port <= 65535;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HostPort hostPort = (HostPort) o;

        return host.equals(hostPort.getHost()) && port == hostPort.getPort();
    }

    @Override
    public int hashCode() {
        return 31 * host.hashCode() + port;
    }

    @Override
    public String toString() {
        // "[]:12345" requires 8 extra bytes.
        StringBuilder builder = new StringBuilder(host.length() + 8);
        if (host.indexOf(':') >= 0) {
            builder.append('[').append(host).append(']');
        } else {
            builder.append(host);
        }

        builder.append(':').append(port);

        return builder.toString();
    }
}
