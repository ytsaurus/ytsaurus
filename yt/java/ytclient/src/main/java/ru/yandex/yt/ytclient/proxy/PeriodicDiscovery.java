package ru.yandex.yt.ytclient.proxy;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.bolts.collection.Tuple2;
import ru.yandex.misc.ExceptionUtils;
import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.bus.DefaultBusFactory;
import ru.yandex.yt.ytclient.rpc.DefaultRpcBusClient;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

public class PeriodicDiscovery {
    private static final Logger logger = LoggerFactory.getLogger(PeriodicDiscovery.class);

    private final BusConnector connector;
    private final Map<String, DiscoveryServiceClient> proxies;
    private final Duration updatePeriod;
    private final Random rnd;

    public PeriodicDiscovery(List<String> initialAddresses, BusConnector connector, Duration updatePeriod) {
        this.connector = connector;
        this.updatePeriod = updatePeriod;
        this.rnd = new Random();

        proxies = initialAddresses.stream().map(x -> {
            try {
                return new Tuple2<>(x, createDiscoveryServiceClient(x));
            } catch (Exception e) {
                throw ExceptionUtils.translate(e);
            }
        }).collect(Collectors.toMap(Tuple2::get1, Tuple2::get2));

        updateProxies();
    }

    public Set<String> getAddresses() {
        return proxies.keySet();
    }

    private DiscoveryServiceClient createDiscoveryServiceClient(String addr) throws URISyntaxException {
        final URI uri = new URI("my://" + addr);
        final String host = uri.getHost();
        final int port = uri.getPort();
        DiscoveryServiceClient client = new DiscoveryServiceClient(new DefaultRpcBusClient(
                new DefaultBusFactory(connector, () -> new InetSocketAddress(host, port < 0 ? 9013 : port))
        ), new RpcOptions().setDefaultTimeout(updatePeriod.dividedBy(2)));
        return client;
    }

    private void updateProxies() {
        List<DiscoveryServiceClient> clients = new ArrayList<>(proxies.values());
        DiscoveryServiceClient client = clients.get(rnd.nextInt(clients.size()));
        client.discoverProxies().whenComplete((result, error) -> {
            if (error != null) {
                logger.error("Error on update proxies {}", error);
            } else {
                processProxies(new HashSet<>(result));
            }

            scheduleUpdate();
        });
    }

    private void processProxies(Set<String> list) {
        Set<String> addresses = proxies.keySet();
        Set<String> removed = Sets.difference(addresses, list).immutableCopy();
        Set<String> added = Sets.difference(list, addresses).immutableCopy();

        for (String addr : removed) {
            try {
                DiscoveryServiceClient client = proxies.remove(addr);
                if (client != null) {
                    logger.info("Proxy removed: {}", addr);
                    client.getClient().close();
                } else {
                    logger.warn("Cannot remove proxy: {}", addr);
                }
            } catch (Throwable e) {
                logger.error("Error on proxy remove {}: {}", addr, e);
            }
        }

        for (String addr : added) {
            try {
                DiscoveryServiceClient client = createDiscoveryServiceClient(addr);
                logger.info("New proxy added: {}", addr);
                proxies.put(addr, client);
            } catch (Throwable e) {
                logger.error("Error on address parse {}: {}", addr, e);
            }
        }
    }

    private void scheduleUpdate() {
        connector.executorService().schedule(this::updateProxies, updatePeriod.toMillis(), TimeUnit.MILLISECONDS);
    }
}
