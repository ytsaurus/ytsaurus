package tech.ytsaurus.client.bus;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BusTest {
    private BusConnector busConnector;

    @Before
    public void createConnector() {
        busConnector = new DefaultBusConnector()
                .setReadTimeout(Duration.ofSeconds(1))
                .setWriteTimeout(Duration.ofSeconds(1));
    }

    @After
    public void stopConnector() {
        if (busConnector != null) {
            try {
                busConnector.close();
            } finally {
                busConnector = null;
            }
        }
    }

    @Test
    public void roundtrip() throws InterruptedException {
        BlockingQueue<Object> serverQueue = new ArrayBlockingQueue<>(1);
        BusServer server = busConnector.listen(new InetSocketAddress("127.0.0.1", 0), new BusListenerAdapter() {
            @Override
            public void onMessage(Bus bus, List<byte[]> message) {
                try {
                    // Проверяем, что нам прислали правильное сообщение
                    assertEquals(new String(message.get(0)), "Hello, world!");

                    // Отправляем ответное сообщение и закрываем bus, оно всё-равно должно дойти
                    bus.send(Collections.singletonList("Message received".getBytes()), BusDeliveryTracking.NONE);
                    bus.close();
                } catch (Throwable e) {
                    e.printStackTrace();
                    serverQueue.add(e);
                    throw e;
                }
                serverQueue.add("OK");
            }
        });
        server.bound().sync();

        BlockingQueue<Object> clientQueue = new ArrayBlockingQueue<>(1);
        Bus client = busConnector.connect(server.localAddress(), new BusListenerAdapter() {
            @Override
            public void onMessage(Bus bus, List<byte[]> message) {
                try {
                    // Проверяем, что нам пристали правильное сообщение
                    assertEquals(new String(message.get(0)), "Message received");

                    // Просто закрываем канал
                    bus.close();
                } catch (Throwable e) {
                    e.printStackTrace();
                    clientQueue.add(e);
                    throw e;
                }
                clientQueue.add("OK");
            }
        });
        client.connected().sync();

        // Отправляем сообщение и дожидаемся ACK о доставке
        client.send(Collections.singletonList("Hello, world!".getBytes()), BusDeliveryTracking.FULL).join();

        // Проверяем, что обе стороны получили правильные сообщения
        assertEquals(serverQueue.poll(100, TimeUnit.MILLISECONDS), "OK");
        assertEquals(clientQueue.poll(100, TimeUnit.MILLISECONDS), "OK");
    }
}
