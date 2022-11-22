package tech.ytsaurus.client.bus;

public interface BusLifecycle {
    /**
     * Вызывается в случае коннекта
     */
    void channelConnected();

    /**
     * Вызывается в случае дисконнекта
     */
    void channelDisconnected();

    /**
     * Вызывается на ошибках связанных с каналом
     */
    void channelFailed(Throwable cause);
}
