package tech.ytsaurus.client.bus;

/**
 * Required acknowledgment level for message delivery.
 */
public enum BusDeliveryTracking {
    /**
     * Do not follow the sending package.
     */
    NONE,

    /**
     * Notify about the status of a packet being written to a connection.
     */
    SENT,

    /**
     * Notify about the delivery status after receiving confirmation from the other side.
     */
    FULL
}
