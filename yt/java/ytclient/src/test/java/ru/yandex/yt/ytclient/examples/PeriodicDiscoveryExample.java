package ru.yandex.yt.ytclient.examples;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.proxy.PeriodicDiscovery;

public class PeriodicDiscoveryExample {
    public static void main(String[] args) {
        try {
            BusConnector connector = ExamplesUtil.createConnector();
            List<String> initialHosts = new ArrayList<>();
            Collections.addAll(initialHosts, ExamplesUtil.getHosts());

            Duration interval = Duration.ofSeconds(15);
            PeriodicDiscovery pd = new PeriodicDiscovery(initialHosts, connector, interval);
            while (true) {
                System.out.printf("current list: %s\n", pd.getAddresses().toString());
                Thread.sleep(interval.toMillis());
            }
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }
    }
}
