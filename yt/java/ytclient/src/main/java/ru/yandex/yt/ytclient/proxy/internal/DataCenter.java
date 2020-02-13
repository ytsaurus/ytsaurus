package ru.yandex.yt.ytclient.proxy.internal;

import java.io.Closeable;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.internal.metrics.DataCenterMetricsHolder;

/**
 * @author aozeritsky
 */
public final class DataCenter implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(DataCenter.class);

    private final DataCenterMetricsHolder metricsHolder;
    private final RpcOptions options;

    private final String dc;
    private final double weight;

    private boolean terminated;
    private final ArrayList<RpcClientSlot> slots;
    private LocalDateTime lastRebalance;

    private final ReentrantLock lock = new ReentrantLock();

    public DataCenter(String dc) {
        this(dc, -1.0);
    }

    public DataCenter(String dc, DataCenterMetricsHolder metricsHolder) {
        this(dc, -1.0, metricsHolder, new RpcOptions());
    }

    public DataCenter(String dc, double weight) {
        this(dc, weight, new RpcOptions());
    }

    public DataCenter(String dc, double weight, RpcOptions options) {
        this(dc, weight, options.getDataCenterMetricsHolder(), options);
    }

    public DataCenter(String dc, double weight, DataCenterMetricsHolder metricsHolder, RpcOptions options) {
        this(dc, new ArrayList<>(), weight, metricsHolder, options);
    }

    public DataCenter(
            String dc,
            List<RpcClient> clients,
            double weight,
            DataCenterMetricsHolder metricsHolder,
            RpcOptions options) {
        this.dc = dc;

        this.weight = weight;
        this.metricsHolder = metricsHolder;
        this.options = options;

        this.terminated = false;
        this.slots = new ArrayList<>();
        this.lastRebalance = LocalDateTime.now();

        for (int i = 0; i < options.getChannelPoolSize(); i++) {
            slots.add(new RpcClientSlot());
        }

        int clientsSize = Math.min(clients.size(), options.getChannelPoolSize());
        HostPort unknownAddress = HostPort.parse("unknown:1");
        for (int i = 0; i < clientsSize; i++) {
            slots.get(i).setClient(clients.get(i), unknownAddress);
        }
    }

    public double weight() {
        if (weight > 0) {
            return weight;
        } else {
            return metricsHolder.getDc99thPercentile(dc);
        }
    }

    public String getName() {
        return dc;
    }

    private void broadcastError(Exception e) {
        lock.lock();
        try {
            for (RpcClientSlot slot : slots) {
                slot.getClientFuture().completeExceptionally(e);
            }
        } finally {
            lock.unlock();
        }
    }

    private Duration randomDuration(Duration maxDuration, Random rnd) {
        long maxMillis = maxDuration.toMillis();
        long randomMillis = (long) rnd.nextFloat() * maxMillis;
        return Duration.ofMillis(randomMillis);
    }

    private List<RpcClientSlot> replacedSlots(Set<HostPort> proxies,
                                              Duration rebalanceInterval,
                                              LocalDateTime now,
                                              Random rnd) {
        ArrayList<RpcClientSlot> result = new ArrayList<>();
        for (int i = 0; i < slots.size(); ++i) {
            // Replace channels that returned rpc errors.
            if (slots.get(i).seemsBroken()) {
                slots.set(i, new RpcClientSlot());
                result.add(slots.get(i));
                continue;
            }

            CompletableFuture<RpcClient> channel = slots.get(i).getClientFuture();
            // Initialize slots created in constructor.
            if (!channel.isDone()) {
                result.add(slots.get(i));
                continue;
            }

            // Replace channels that are broken for sure.
            if (channel.isCompletedExceptionally()) {
                slots.set(i, new RpcClientSlot());
                result.add(slots.get(i));
                continue;
            }

            // Replace channels connected to unknown proxies.
            if (!proxies.contains(slots.get(i).getAddress())) {
                slots.set(i, new RpcClientSlot());
                result.add(slots.get(i));
                continue;
            }
        }

        int randomVictim = -1;
        if (result.isEmpty() && lastRebalance.plus(rebalanceInterval).isBefore(now)) {
            lastRebalance = now;
            randomVictim = rnd.nextInt(slots.size());
        }


        if (result.isEmpty() && randomVictim >= 0 && randomVictim < slots.size()) {
            slots.set(randomVictim, new RpcClientSlot());
            result.add(slots.get(randomVictim));
        }

        return result;
    }

    public void setProxies(Set<HostPort> proxies,
                           RpcClientFactory rpcClientFactory,
                           Random rnd) {
        if (proxies.isEmpty()) {
            broadcastError(new Exception("Address list is empty"));
            return;
        }

        Duration rebalanceInterval = randomDuration(options.getChannelPoolRebalanceInterval(), rnd)
                .plus(options.getChannelPoolRebalanceInterval());
        LocalDateTime now = LocalDateTime.now();
        List<RpcClientSlot> replaced;
        lock.lock();
        try {
            if (terminated) {
                return;
            }
            replaced = replacedSlots(proxies, rebalanceInterval, now, rnd);
        } finally {
            lock.unlock();
        }

        ArrayList<HostPort> proxiesList = new ArrayList<>(proxies);
        if (!replaced.isEmpty()) {
            Collections.shuffle(proxiesList, rnd);
            logger.debug("Proxy address list prepared (Addresses: `{}`)", proxiesList);
            for (int i = 0; i < replaced.size(); ++i) {
                HostPort address = proxiesList.get(i % proxiesList.size());
                RpcClient client = rpcClientFactory.create(address, dc);
                replaced.get(i).setClient(client, address);
            }
        }
    }

    private void closeClient(RpcClient client) {
        try {
            logger.info("Close client `{}`", client);
            client.close();
        } catch (Exception e) {
            logger.error(e.getMessage(), "Error while closing DataCenter");
        }
    }

    @Override
    public void close() {
        ArrayList<RpcClient> aliveClients = new ArrayList<>();
        Exception error = new Exception("DataCenter is terminated");
        lock.lock();
        try {
            if (terminated) {
                return;
            }
            terminated = true;
            for (RpcClientSlot slot : slots) {
                slot.getClientFuture().completeExceptionally(error);
                if (slot.getClientFuture().isDone()) {
                    if (!slot.getClientFuture().isCompletedExceptionally()) {
                        aliveClients.add(slot.getClient());
                    }
                }
            }
            slots.clear();
        } finally {
            lock.unlock();
        }

        for (RpcClient client : aliveClients) {
            closeClient(client);
        }
    }

    public List<RpcClient> getAliveDestinations() {
        return getAliveDestinations(RpcClientSlot::getClient);
    }

    public <T> List<T> getAliveDestinations(Function<RpcClientSlot, T> func) {
        lock.lock();
        try {
            return slots.stream().filter(RpcClientSlot::isClientReady).map(func).collect(Collectors.toList());
        } finally {
            lock.unlock();
        }
    }

    public List<RpcClient> selectDestinations(final int maxSelect, Random rnd) {
        return selectDestinations(maxSelect, rnd, RpcClientSlot::getClient);
    }

    private boolean canSkipSlots(int index, int requiredResultSize) {
        return slots.size() - index > requiredResultSize;
    }

    private boolean shouldSkipSlot(int index) {
        return slots.get(index).seemsBroken() || !slots.get(index).isClientReady();
    }

    public <T> List<T> selectDestinations(final int maxSelect, Random rnd, Function<RpcClientSlot, T> func) {
        lock.lock();
        try {
            if (terminated) {
                throw new RuntimeException("DataCenter is terminated");
            }

            if (slots.isEmpty()) {
                throw new IllegalStateException("DataCenter channel pool is empty");
            }

            int resultSize = Math.min(maxSelect, slots.size());
            final ArrayList<T> result = new ArrayList<>(resultSize);

            Collections.shuffle(slots, rnd);

            int count = resultSize;
            int index = 0;
            while (count != 0 && index < slots.size()) {
                if (canSkipSlots(index, count) && shouldSkipSlot(index)) {
                    ++index;
                    continue;
                }
                result.add(func.apply(slots.get(index)));
                ++index;
                --count;
            }

            logger.debug("Selected rpc proxies: `{}`", result);

            return result;
        } finally {
            lock.unlock();
        }
    }
}
