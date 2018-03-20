package ru.yandex.yt.ytclient.proxy;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final List<String> initialAddresses;

    public PeriodicDiscovery(List<String> initialAddresses, BusConnector connector, Duration updatePeriod) {
        this.connector = connector;
        this.updatePeriod = updatePeriod;
        this.rnd = new Random();
        this.initialAddresses = initialAddresses;
        this.proxies = new HashMap<>();

        addProxies(initialAddresses);
        updateProxies();
    }

    public Set<String> getAddresses() {
        return proxies.keySet();
    }

    private void removeProxies(Collection<String> list) {
        for (String addr : list) {
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
    }

    private void addProxies(Collection<String> list) {
        for (String addr : list) {
            try {
                DiscoveryServiceClient client = createDiscoveryServiceClient(addr);
                logger.info("New proxy added: {}", addr);
                proxies.put(addr, client);
            } catch (Throwable e) {
                logger.error("Error on address parse {}: {}", addr, e);
            }
        }
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

        removeProxies(removed);
        addProxies(added);

        if (proxies.isEmpty()) {
            logger.warn("Empty proxies list. Bootstraping from the initial list: {}", initialAddresses);
            addProxies(initialAddresses);
        }
    }

    private void scheduleUpdate() {
        connector.executorService().schedule(this::updateProxies, updatePeriod.toMillis(), TimeUnit.MILLISECONDS);
    }
}
