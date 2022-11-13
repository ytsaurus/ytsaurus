package ru.yandex.yt.ytclient.proxy;

import java.util.List;

import tech.ytsaurus.client.rpc.RpcClient;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

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
    public static List<RpcClient> selectDestinations(YtClientOpensource client) {
        return client.selectDestinations();
    }
}
