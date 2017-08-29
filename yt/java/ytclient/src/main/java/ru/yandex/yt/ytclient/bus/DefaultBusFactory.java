package ru.yandex.yt.ytclient.bus;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Устанавливает соединения через connector на фиксированный адрес
 */
public class DefaultBusFactory implements BusFactory {
    private final BusConnector connector;
    private final Supplier<SocketAddress> addressSupplier;

    public DefaultBusFactory(BusConnector connector, String host, int port) {
        this(connector, constSupplier(host, port));
    }

    public DefaultBusFactory(BusConnector connector, SocketAddress address) {
        this(connector, constSupplier(address));
    }

    public DefaultBusFactory(BusConnector connector, Supplier<SocketAddress> addressSupplier) {
        this.connector = Objects.requireNonNull(connector);
        this.addressSupplier = Objects.requireNonNull(addressSupplier);
    }

    @Override
    public Bus createBus(BusListener listener) {
        return connector.connect(addressSupplier.get(), listener);
    }

    private static Supplier<SocketAddress> constSupplier(SocketAddress address) {
        Objects.requireNonNull(address);
        return () -> address;
    }

    private static Supplier<SocketAddress> constSupplier(String host, int port) {
        Objects.requireNonNull(host);
        return () -> new InetSocketAddress(host, port);
    }

    public String destinationName() {
        SocketAddress addr = addressSupplier.get();
        if (addr instanceof InetSocketAddress) {
            return ((InetSocketAddress) addr).getHostName();
        } else {
            return toString();
        }
    }
}
