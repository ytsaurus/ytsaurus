package tech.ytsaurus.client;

import java.util.List;

import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

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
