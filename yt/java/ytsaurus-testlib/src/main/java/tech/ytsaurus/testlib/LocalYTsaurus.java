package tech.ytsaurus.testlib;

import tech.ytsaurus.lang.NonNullApi;

/**
 * Class helps to create YTsaurus clients to local YTsaurus instance.
 */
@NonNullApi
public class LocalYTsaurus {
    private static volatile Address address;

    private LocalYTsaurus() {
    }

    /**
     * Get full address of local YTsaurus instance from YT_PROXY environment variable.
     */
    public static String getAddress() {
        return getAddressInstance().host + ":" + getAddressInstance().port;
    }

    /**
     * Get host part of local YTsaurus instance address.
     */
    public static String getHost() {
        return getAddressInstance().host;
    }

    /**
     * Get port part of local YTsaurus instance address.
     */
    public static int getPort() {
        return getAddressInstance().port;
    }

    private static Address getAddressInstance() {
        Address tmpAddress = address;
        if (tmpAddress == null) {
            synchronized (LocalYTsaurus.class) {
                tmpAddress = address;
                if (tmpAddress == null) {
                    tmpAddress = new Address();
                    address = tmpAddress;
                }
            }
        }
        return tmpAddress;
    }

    static class Address {
        private final String host;
        private final int port;

        private Address() {
            final String ytProxy = System.getenv("YT_PROXY");
            if (ytProxy == null) {
                throw new RuntimeException(
                        "Cannot get address of local YTsaurus instance: environment variable $YT_PROXY is not set."
                );
            }
            String[] parts = ytProxy.split(":");
            host = parts[0];
            if (parts.length == 1) {
                port = 80;
            } else if (parts.length == 2) {
                try {
                    port = Integer.parseInt(parts[1]);
                } catch (NumberFormatException error) {
                    throw new RuntimeException("Environment variable $YT_PROXY contains bad address: " + ytProxy,
                            error);
                }
            } else {
                throw new RuntimeException("Environment variable $YT_PROXY contains bad address: " + ytProxy);
            }

            // Check that we don't try to use production instance.
            if (host.contains(".yt.yandex.net")
                    || !host.contains(".") && !host.equals("localhost")) {
                String confirmation = System.getenv("YT_DANGEROUS_CONFIRMATION");
                if (confirmation == null || !confirmation.equals("hereby I confirm that I understand consequences of " +
                        "my actions")) {
                    // NB. It's dangerous to run tests on the production instance.
                    // (Especially if you are developer of YT).
                    // We cannot stop you if you really want, but you have to understand that YOU MIGHT BREAK
                    // EVERYTHING.
                    throw new RuntimeException(
                            "Looks like you are trying to use production YTsaurus instance in your tests: "
                                    + ytProxy + "\n" +
                                    "Please investigate source code and fill confirmation if you really want to do so."
                    );
                }
            }
        }
    }
}
