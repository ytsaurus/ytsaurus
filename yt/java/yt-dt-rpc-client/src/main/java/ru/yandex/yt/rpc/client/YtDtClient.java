package ru.yandex.yt.rpc.client;

import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.rpc.channel.ClientChannelPool;
import ru.yandex.yt.rpc.client.requests.LookupReqInfo;
import ru.yandex.yt.rpc.client.responses.VersionedLookupRow;
import ru.yandex.yt.rpc.protocol.RpcRequestMessage;
import ru.yandex.yt.rpc.protocol.bus.BusMessage;
import ru.yandex.yt.rpc.protocol.bus.BusPackage;
import ru.yandex.yt.rpc.protocol.rpc.RpcReqFactory;

/**
 * Blocking RPC-client with retries and cancellation logic.
 *
 * @author valri
 */
public class YtDtClient extends YtClient {
    private static Logger logger = LoggerFactory.getLogger(YtDtClient.class);

    private static final int NUMBER_OF_TRIES = 1;
    private static final int CHANNEL_READ_TIMEOUT_SECONDS = 5;
    private static final int REQUEST_TIMEOUT = 500;
    private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

    private RpcReqFactory rpcReqFactory;
    private ClientChannelPool channelPool;

    private int numberOfTries;
    private int requestTimeout;
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
    public YtDtClient(InetSocketAddress inetSocketAddress, String token, String user,
                      String domain, int protocolVersion)
    {
        this(inetSocketAddress, token, user, domain, protocolVersion,
                NUMBER_OF_TRIES, REQUEST_TIMEOUT, CHANNEL_READ_TIMEOUT_SECONDS);
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
     * @param requestTimeout     timeout for requests to RPC server default is {@value REQUEST_TIMEOUT}
     * @param channelReadTimeout TCP channel read timeout
     */
    public YtDtClient(InetSocketAddress inetSocketAddress, String token, String user, String domain,
                      int protocolVersion, int numberOfTries, int requestTimeout, int channelReadTimeout)
    {
        this.channelPool = new ClientChannelPool(inetSocketAddress, channelReadTimeoutSeconds);
        String localIpAddress;
        try {
            localIpAddress = Inet4Address.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            localIpAddress = channelPool.retrieveLocalIp();
        }
        this.rpcReqFactory = new RpcReqFactory(token, user, domain, protocolVersion, localIpAddress);
        this.numberOfTries = numberOfTries;
        this.requestTimeout = requestTimeout;
        this.channelReadTimeoutSeconds = channelReadTimeout;
    }

    /**
     * Makes a request to remote RPC server with {@link #numberOfTries}.
     *
     * @param bus   package whick is ment to be send
     * @param pack  actual message packet with bus
     * @return bus  package response from RPC server
     */
    private BusPackage retryRequest(BusPackage bus, RpcRequestMessage pack) {
        for (int i = numberOfTries; i > 0; --i) {
            final CompletableFuture<BusPackage> rsp = channelPool.sendRequest(bus);
            try {
                logger.info("Request requestId={} send. Timeout: {} {}. Retries: {}", pack.getRequestId(),
                        requestTimeout, TIME_UNIT, i - 1);
                long startTime  = System.nanoTime();
                BusPackage busPackage = rsp.get(requestTimeout, TIME_UNIT);
                logger.info("Request with requestId={} execution took: {}ms",
                        pack.getRequestId(), (System.nanoTime() - startTime) / 1000000);
                return busPackage;
            } catch (CancellationException | InterruptedException | ExecutionException e) {
                logger.error("Request was cancelled ({}), Retries left: {}", bus, i - 1, e);
            } catch (TimeoutException e) {
                logger.error("Request timed out ({})", bus);
                if (!rsp.isDone()) {
                    channelPool.sendCancellation(new BusPackage(BusPackage.PacketType.MESSAGE,
                            BusPackage.PacketFlags.NONE, pack.getRequestId(), false,
                            pack.getCancellationBusEnvelope()));
                    rsp.cancel(false);
                }
                break;
            }
        }
        logger.error("Failed to send bus package ({})", bus.toString());
        return null;
    }

    /**
     * Call GetNode YT method.
     *
     * @param path  path to the node
     * @return      info about the node with specified path
     */
    public String getNode(String path, UUID uuid) {
        return this.getNode(path, uuid, false, false);
    }

    /**
     * Call GetNode YT method with ACK enabled and checkSum parameter.
     *
     * @param path            path to the node in YT cluster
     * @param checkSumEnabled enable checkSum computation for bus packages
     * @param ackEnabled      enable/disable ACK package receive
     * @return                info about the node with specified path
     */
    public String getNode(String path, UUID uuid, boolean ackEnabled, boolean checkSumEnabled) {
        RpcRequestMessage request = rpcReqFactory.createGetNode(path, uuid);
        BusPackage outcomeBusPackage = new BusMessage(ackEnabled, request.getRequestId(),
                checkSumEnabled, request.getBusEnvelope());
        BusPackage pack = retryRequest(outcomeBusPackage, request);
        if (pack != null) {
            String parsedResponse = getGetNode(pack);
            if (parsedResponse != null) {
                return parsedResponse;
            }
        }
        return "";
    }

    /**
     * Call LookupRows YT method for multiple rows with ACK parameter.
     *
     * @param info            all the info about database structure
     * @param ackEnabled      enable/disable ACK package receive
     * @param checkSumEnabled enable checkSum computation for bus packages
     * @return                list of maps with full response row
     */
    public List<Map<String, Object>> lookupRows(LookupReqInfo info, boolean ackEnabled, boolean checkSumEnabled) {
        if (info.filters == null || info.tableSchema == null || info.wireFormat == null) {
            logger.error("LookupReqInfo is not filled properly. Request won't be executed");
            return Collections.emptyList();
        }
        RpcRequestMessage request = rpcReqFactory.createLookupRows(info);
        BusPackage outcomeBusPackage = new BusMessage(ackEnabled, request.getRequestId(),
                                                      checkSumEnabled, request.getBusEnvelope());
        BusPackage pack = retryRequest(outcomeBusPackage, request);
        if (pack != null) {
            List<Map<String, Object>> parsedResponse = getLookupRows(pack, info.tableSchema);
            if (parsedResponse != null) {
                return parsedResponse;
            }
        }
        return Collections.emptyList();
    }

    /**
     * VersionedLookupRows method for multiple rows and their write and delete timestamps.
     *
     * @param info            all the info about database structure
     * @param ackEnabled      enable/disable ACK package receive
     * @param checkSumEnabled enable checkSum computation for bus packages
     * @return                list of versioned rows where keys are single objects and values are lists
     */
    public List<VersionedLookupRow> versionedLookupRows(LookupReqInfo info, boolean ackEnabled,
                                                        boolean checkSumEnabled)
    {
        if (info.filters == null || info.tableSchema == null || info.wireFormat == null) {
            logger.error("LookupReqInfo is not filled properly. Request won't be executed");
            return Collections.emptyList();
        }
        RpcRequestMessage request = rpcReqFactory.createVersionedLookupRows(info);
        BusPackage outcomeBusPackage = new BusMessage(ackEnabled, request.getRequestId(),
                checkSumEnabled, request.getBusEnvelope());
        BusPackage pack = retryRequest(outcomeBusPackage, request);
        if (pack != null) {
            List<VersionedLookupRow> parsedResponse = getVersionedLookupRows(pack, info.tableSchema);
            if (parsedResponse != null) {
                return parsedResponse;
            }
        }
        return Collections.emptyList();
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
