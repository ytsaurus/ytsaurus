package ru.yandex.yt.testlib;

import javax.annotation.Nullable;

import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * Class helps to create YT clients to local YT instance.
 */
@NonNullFields
@NonNullApi
public class LocalYt {
    private static volatile @Nullable Address address = null;

    /**
     * Get full address of local YT instance from YT_PROXY environment variable.
     */
    public static String getAddress() {
        return getAddressInstance().host + ":" + getAddressInstance().port;
    }

    /**
     * Get host part of local YT instance address.
     */
    @SuppressWarnings("unused")
    public static String getHost() {
        return getAddressInstance().host;
    }

    /**
     * Get port part of local YT instance address.
     */
    @SuppressWarnings("unused")
    public static int getPort() {
        return getAddressInstance().port;
    }

    private static Address getAddressInstance() {
        Address tmpAddress = address;
        if (tmpAddress == null) {
            synchronized (LocalYt.class) {
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
                        "Cannot get address of local YT instance: environment variable $YT_PROXY is not set."
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
                    throw new RuntimeException("Environment variable $YT_PROXY contains bad address: " + ytProxy, error);
                }
            } else {
                throw new RuntimeException("Environment variable $YT_PROXY contains bad address: " + ytProxy);
            }

            // Check that we don't try to use production instance.
            if (host.contains(".yt.yandex.net")
                    || !host.contains(".") && !host.equals("localhost"))
            {
                String confirmation = System.getenv("YT_DANGEROUS_CONFIRMATION");
                if (confirmation == null || !confirmation.equals("hereby I confirm that I understand consequences of my actions")) {
                    // NB. It's dangerous to run tests on the production instance.
                    // (Especially if you are developer of YT).
                    // We cannot stop you if you really want, but you have to understand that YOU MIGHT BREAK EVERYTHING.
                    throw new RuntimeException(
                            "Looks like you are trying to use production YT instance in your tests: " + ytProxy + "\n" +
                                    "Please investigate source code and fill confirmation if you really want to do so."
                    );
                }
            }
        }
    }
}
