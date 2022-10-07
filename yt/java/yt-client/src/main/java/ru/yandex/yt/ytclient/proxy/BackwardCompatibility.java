package ru.yandex.yt.ytclient.proxy;

import java.util.List;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.rpc.RpcClient;

/**
 * Miscellaneous stuff to help with backward compatibility.
 * Should not be used in new code.
 * @deprecated
 */
@Deprecated
@NonNullFields
@NonNullApi
public class BackwardCompatibility {
    private BackwardCompatibility() {
    }

    /**
     * Get list of rpcclients that can be used to send messages to YtClient.
     */
    public static List<RpcClient> selectDestinations(YtClient client) {
        return client.selectDestinations();
    }
}
