package tech.ytsaurus.client.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.ApiServiceClientImpl;
import tech.ytsaurus.client.DefaultSerializationResolver;
import tech.ytsaurus.client.YTsaurusClientConfig;
import tech.ytsaurus.client.bus.DefaultBusConnector;

import static org.hamcrest.MatcherAssert.assertThat;
import static tech.ytsaurus.testlib.FutureUtils.getError;
import static tech.ytsaurus.testlib.FutureUtils.waitFuture;
import static tech.ytsaurus.testlib.Matchers.isCausedBy;

public class DefaultRpcBusClientTest {
    @Test(timeout = 5000)
    public void testRequestAcknowledgementTimeout() throws Exception {
        try (var server = new DevNullServer();
             var connector = new DefaultBusConnector()) {
            var options = new RpcOptions()
                    .setAcknowledgementTimeout(Duration.ofMillis(500));

            var rpcClient = new DefaultRpcBusClient(
                    connector,
                    new InetSocketAddress("localhost", server.getPort()));

            try (rpcClient) {
                var api = new ApiServiceClientImpl(
                        rpcClient,
                        YTsaurusClientConfig.builder().setRpcOptions(options).build(),
                        ForkJoinPool.commonPool(),
                        connector.executorService(),
                        DefaultSerializationResolver.getInstance());
                var listNodeFuture = api.listNode("/");

                waitFuture(listNodeFuture, 5000);
                assertThat(getError(listNodeFuture), isCausedBy(AcknowledgementTimeoutException.class));
            }
        }
    }
}

// Server that accepts all connections it reads all data and not sending any byte in response.
class DevNullServer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(DevNullServer.class);

    final ServerSocket serverSocket;
    final Thread listenThread;
    final ConcurrentHashMap<Integer, Item> activeConnections;

    DevNullServer() throws IOException {
        serverSocket = new ServerSocket(0);
        listenThread = new Thread(this::listenProc);
        activeConnections = new ConcurrentHashMap<>();
    }

    public int getPort() {
        return serverSocket.getLocalPort();
    }

    @Override
    public void close() throws Exception {
        serverSocket.close();
        listenThread.join(100);
        for (var item : activeConnections.values()) {
            try {
                item.socket.close();
            } catch (IOException e) {
                logger.error("Unexpected error while closing connection", e);

            }
            item.readThread.join(100);
        }
    }

    void listenProc() {
        while (true) {
            try {
                Socket socket = serverSocket.accept();
                socket.getLocalPort();
                Thread readThread = new Thread(() -> readProc(socket));
                activeConnections.put(socket.getLocalPort(), new Item(socket, readThread));
                readThread.start();
            } catch (IOException error) {
                logger.warn("Error accepting connection", error);
                break;
            }
        }
    }

    void readProc(Socket socket) {
        try {
            var stream = socket.getInputStream();
            byte[] buffer = new byte[1024];

            while (true) {
                int read = stream.read(buffer);
                if (read < 0) {
                    break;
                }
            }
        } catch (IOException error) {
            logger.warn("Error reading socket", error);
        } finally {
            activeConnections.remove(socket.getLocalPort());
        }
    }

    static class Item {
        Socket socket;
        Thread readThread;

        Item(Socket socket, Thread readThread) {
            this.socket = socket;
            this.readThread = readThread;
        }
    }
}
