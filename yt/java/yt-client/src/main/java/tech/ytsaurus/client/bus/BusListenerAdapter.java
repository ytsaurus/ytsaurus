package tech.ytsaurus.client.bus;

import java.util.List;


/**
 * Абстрактный класс для упрощения реализации BusListener
 */
public abstract class BusListenerAdapter implements BusListener {
    @Override
    public void onMessage(Bus bus, List<byte[]> message) {
        // nothing by default
    }

    @Override
    public void onConnect(Bus bus) {
        // nothing by default
    }

    @Override
    public void onDisconnect(Bus bus) {
        // nothing by default
    }

    @Override
    public void onException(Bus bus, Throwable cause) {
        // nothing by default
    }
}
