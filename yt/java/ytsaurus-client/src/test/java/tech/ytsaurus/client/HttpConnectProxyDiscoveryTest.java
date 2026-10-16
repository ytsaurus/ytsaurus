package tech.ytsaurus.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Test;
import tech.ytsaurus.client.rpc.RpcOptions;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class HttpConnectProxyDiscoveryTest {
    // Does not resolve locally, so the request can only reach the balancer through the proxy.
    private static final String BALANCER_FQDN = "yt-balancer.invalid";

    @Test
    public void proxyDiscoveryGoesThroughProxy() throws Exception {
        NioEventLoopGroup eventLoop = new NioEventLoopGroup(1);
        try (RecordingProxy proxy = new RecordingProxy("{proxies=[]}")) {
            ClientPoolService clientPoolService = ClientPoolService.httpBuilder()
                    .setBalancerFqdn(BALANCER_FQDN)
                    .setHttpConnectProxy(new InetSocketAddress("127.0.0.1", proxy.port()))
                    .setDataCenterName("test")
                    .setOptions(new RpcOptions())
                    .setClientFactory((hostPort, name) -> {
                        throw new UnsupportedOperationException();
                    })
                    .setEventLoop(eventLoop)
                    .setRandom(new Random())
                    .build();
            try {
                clientPoolService.start();

                String requestLine = proxy.awaitRequestLine();
                assertNotNull("the discovery request must reach the proxy", requestLine);
                assertTrue("the discovery request must be proxied in absolute-uri form: " + requestLine,
                        requestLine.startsWith("GET http://" + BALANCER_FQDN)
                                && requestLine.contains("/api/v4/discover_proxies"));
            } finally {
                clientPoolService.close();
            }
        } finally {
            eventLoop.shutdownGracefully(0, 500, TimeUnit.MILLISECONDS).syncUninterruptibly();
        }
    }

    /**
     * Minimal in-process forward proxy that records request lines and always answers with
     * {@code responseBody}.
     */
    private static final class RecordingProxy implements AutoCloseable {
        private final ServerSocket serverSocket;
        private final BlockingQueue<String> requestLines = new ArrayBlockingQueue<>(4);

        RecordingProxy(String responseBody) throws IOException {
            serverSocket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress());
            Thread thread = new Thread(() -> serve(responseBody));
            thread.setDaemon(true);
            thread.start();
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        String awaitRequestLine() throws InterruptedException {
            return requestLines.poll(10, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws IOException {
            serverSocket.close();
        }

        private void serve(String responseBody) {
            while (!serverSocket.isClosed()) {
                try (Socket socket = serverSocket.accept()) {
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
                    String requestLine = in.readLine();
                    if (requestLine == null) {
                        continue;
                    }
                    requestLines.offer(requestLine);
                    while (true) {
                        String header = in.readLine();
                        if (header == null || header.isEmpty()) {
                            break;
                        }
                    }
                    byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);
                    OutputStream out = socket.getOutputStream();
                    out.write(("HTTP/1.1 200 OK\r\nContent-Length: " + body.length + "\r\n\r\n")
                            .getBytes(StandardCharsets.US_ASCII));
                    out.write(body);
                    out.flush();
                } catch (IOException stopped) {
                    return;
                }
            }
        }
    }
}
