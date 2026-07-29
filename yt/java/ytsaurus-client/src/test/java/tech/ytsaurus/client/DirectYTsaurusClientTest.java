package tech.ytsaurus.client;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tech.ytsaurus.client.bus.DefaultBusConnector;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.junit.Assert.assertEquals;
import static tech.ytsaurus.testlib.FutureUtils.waitFuture;

/**
 * Requests are sent to a server that accepts a connection and never answers, so every attempt ends with an
 * acknowledgement timeout. Attempts are counted by the ids of the requests that reached the wire: an attempt
 * is written to the connection before it times out, and a retry is written with a fresh request id.
 */
public class DirectYTsaurusClientTest {
    private static final long WAIT_TIMEOUT_MS = 30_000;
    private static final int ATTEMPT_LIMIT = 3;

    private final Set<GUID> requestIds = ConcurrentHashMap.newKeySet();
    private final ConcurrentLinkedQueue<Socket> connections = new ConcurrentLinkedQueue<>();

    private ServerSocket server;
    private Thread acceptor;
    private DefaultBusConnector busConnector;

    @Before
    public void startServer() throws IOException {
        server = new ServerSocket(0, 0, InetAddress.getLoopbackAddress());
        acceptor = new Thread(() -> {
            while (!server.isClosed()) {
                try {
                    connections.add(server.accept());
                } catch (IOException e) {
                    return;
                }
            }
        });
        acceptor.setDaemon(true);
        acceptor.start();
        busConnector = new DefaultBusConnector();
    }

    @After
    public void stopServer() throws IOException, InterruptedException {
        // the acceptor is stopped first, so that no connection is opened past the loop over them below
        server.close();
        acceptor.join(WAIT_TIMEOUT_MS);
        busConnector.close();
        for (Socket connection : connections) {
            connection.close();
        }
    }

    @Test
    public void sendsRequestOnceByDefault() {
        listNode(DirectYTsaurusClient.builder());

        assertEquals(1, requestIds.size());
    }

    @Test
    public void retriesRequestWhenAsked() {
        listNode(DirectYTsaurusClient.builder().setRetryRequests(true));

        assertEquals(ATTEMPT_LIMIT, requestIds.size());
    }

    private void listNode(DirectYTsaurusClient.Builder builder) {
        RpcOptions options = new RpcOptions()
                .setGlobalTimeout(Duration.ofSeconds(2))
                .setAcknowledgementTimeout(Duration.ofMillis(100))
                .setRetryPolicyFactory(() -> RetryPolicy.retryAll(ATTEMPT_LIMIT))
                .setRpcClientListener((context, bytes) -> requestIds.add(context.getRequestId()));

        DirectYTsaurusClient client = builder
                .setSharedBusConnector(busConnector)
                .setAddress(new InetSocketAddress(server.getInetAddress(), server.getLocalPort()))
                .setAuth(YTsaurusClientAuth.builder().setUser("test").setToken("test").build())
                .setConfig(YTsaurusClientConfig.builder().setRpcOptions(options).build())
                .build();
        try {
            CompletableFuture<YTreeNode> result = client.listNode("/");
            waitFuture(result, WAIT_TIMEOUT_MS);
        } finally {
            client.close();
        }
    }
}
