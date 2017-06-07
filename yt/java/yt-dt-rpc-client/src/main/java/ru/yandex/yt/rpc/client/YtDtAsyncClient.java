package ru.yandex.yt.rpc.client;

import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ru.yandex.yt.rpc.channel.ClientChannelPool;
import ru.yandex.yt.rpc.client.requests.LookupReqInfo;
import ru.yandex.yt.rpc.protocol.RpcRequestMessage;
import ru.yandex.yt.rpc.protocol.bus.BusMessage;
import ru.yandex.yt.rpc.protocol.bus.BusPackage;
import ru.yandex.yt.rpc.protocol.rpc.RpcReqFactory;

/**
 * Asynchronous RPC-client without retries logic.
 *
 * @author valri
 */
public class YtDtAsyncClient extends YtClient {
    private static Logger logger = LogManager.getLogger(YtDtAsyncClient.class);
    private static final int CHANNEL_READ_TIMEOUT_SECONDS = 5;

    private RpcReqFactory rpcReqFactory;
    private ClientChannelPool channelPool;
    private int channelReadTimeoutSeconds;

    /**
     * Returns an instance of YT client.
     *
     * @param inetSocketAddress remote address of YT DT server
     * @param token             token fot user authentication in YT system
     * @param user              username for authentication in YT
     * @param domain            target domain of YT server
     * @param protocolVersion   RPC-protocol version
     */
    public YtDtAsyncClient(InetSocketAddress inetSocketAddress, String token, String user,
                           String domain, int protocolVersion)
    {
        this(inetSocketAddress, token, user, domain, protocolVersion, CHANNEL_READ_TIMEOUT_SECONDS);
    }

    /**
     * Returns an instance of YT client.
     * Class constructor specifying request and TCP channel idle timeouts.
     *
     * @param inetSocketAddress  remote address of YT DT server
     * @param token              token fot user authentication in YT system
     * @param user               username for authentication in YT
     * @param domain             target domain of YT server
     * @param protocolVersion    RPC-protocol version
     * @param channelReadTimeout TCP channel read timeout
     */
    public YtDtAsyncClient(InetSocketAddress inetSocketAddress, String token, String user, String domain,
                           int protocolVersion, int channelReadTimeout)
    {
        this.channelPool = new ClientChannelPool(inetSocketAddress, channelReadTimeoutSeconds);
        String localIpAddress;
        try {
            localIpAddress = Inet4Address.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            localIpAddress = channelPool.retrieveLocalIp();
        }
        this.rpcReqFactory = new RpcReqFactory(token, user, domain, protocolVersion, localIpAddress);
        this.channelReadTimeoutSeconds = channelReadTimeout;
    }

    /**
     * Call GetNode YT method.
     *
     * @param path path to the node
     * @return info about the node with specified path
     */
    public CompletableFuture<String> getNode(String path, UUID uuid) {
        return this.getNode(path, uuid, false, false);
    }

    /**
     * Call GetNode YT method with ACKenabled and checkSum parameter.
     *
     * @param path            path to the node in YT cluster
     * @param checkSumEnabled enable checkSum computation for bus packages
     * @param ackEnabled      enable/disable ACK package receive
     * @return                info about the node with specified path
     */
    public CompletableFuture<String> getNode(String path, UUID uuid, boolean ackEnabled, boolean checkSumEnabled) {
        RpcRequestMessage request = rpcReqFactory.createGetNode(path, uuid);
        BusPackage outcomeBusPackage = new BusMessage(ackEnabled, request.getRequestId(),
                checkSumEnabled, request.getBusEnvelope());
        CompletableFuture<BusPackage> future = channelPool.sendRequest(outcomeBusPackage);
        return future.thenApply((pack) -> {
            String parsedResponse = getGetNode(pack);
            if (parsedResponse != null) {
                return parsedResponse;
            } else {
                logger.error("Failed to receive answer for ({}). Reason:\n", pack);
                return "";
            }
        });
    }

    /**
     * Call LookupRows YT method for multiple rows with ACK parameter.
     *
     * @param ackEnabled      enable/disable ACK package receive
     * @param checkSumEnabled enable checkSum computation for bus packages
     * @return                list of maps with full response row
     */
    public CompletableFuture<List<Map<String, Object>>> lookupRows(LookupReqInfo info, boolean ackEnabled,
                                                                   boolean checkSumEnabled)
    {
        RpcRequestMessage request = rpcReqFactory.createLookupRows(info);
        BusPackage outcomeBusPackage = new BusMessage(ackEnabled, request.getRequestId(),
                checkSumEnabled, request.getBusEnvelope());
        CompletableFuture<BusPackage> future = channelPool.sendRequest(outcomeBusPackage);
        return future.thenApply((pack) -> {
            List<Map<String, Object>> parsedResponse = getLookupRows(pack, info.tableSchema);
            if (parsedResponse != null) {
                return parsedResponse;
            } else {
                logger.error("Failed to receive answer for ({}). Reason:\n", pack);
                return Collections.emptyList();
            }
        });
    }

    public CompletableFuture<List<Map<String, Object>>> selectRows(
        String query,
        UUID requestId,
        boolean ackEnabled,
        boolean checkSumEnabled)
    {
        RpcRequestMessage request = rpcReqFactory.createSelectRows(query, requestId);
        BusPackage outcomeBusPackage = new BusMessage(ackEnabled, request.getRequestId(),
                checkSumEnabled, request.getBusEnvelope());
        CompletableFuture<BusPackage> future = channelPool.sendRequest(outcomeBusPackage);

        return future.thenApply((pack) -> {
            List<Map<String, Object>> parsedResponse = getSelectRows(pack);
            if (parsedResponse == null) {
                logger.error("Failed to receive answer for ({}). Reason:\n", pack);
                return Collections.emptyList();
            } else {
                return parsedResponse;
            }
        });
    }

    /**
     * Close connection pool to RPC server.
     *
     * @throws InterruptedException
     */
    public void close() throws InterruptedException {
        channelPool.close();
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
