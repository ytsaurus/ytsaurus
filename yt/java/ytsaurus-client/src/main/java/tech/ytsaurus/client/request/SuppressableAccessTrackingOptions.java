package tech.ytsaurus.client.request;

import tech.ytsaurus.rpcproxy.TSuppressableAccessTrackingOptions;

public class SuppressableAccessTrackingOptions {
    private boolean suppressAccessTracking;
    private boolean suppressModificationTracking;
    private boolean suppressExpirationTimeoutRenewal;

    public SuppressableAccessTrackingOptions() {
    }

    public SuppressableAccessTrackingOptions(SuppressableAccessTrackingOptions other) {
        suppressAccessTracking = other.suppressAccessTracking;
        suppressModificationTracking = other.suppressModificationTracking;
        suppressExpirationTimeoutRenewal = other.suppressExpirationTimeoutRenewal;
    }

    /**
     * Sets whether to omit access tracking for this request.
     */
    public SuppressableAccessTrackingOptions setSuppressAccessTracking(boolean suppressAccessTracking) {
        this.suppressAccessTracking = suppressAccessTracking;
        return this;
    }

    /**
     * Sets whether to omit modification tracking for this request.
     */
    public SuppressableAccessTrackingOptions setSuppressModificationTracking(boolean suppressModificationTracking) {
        this.suppressModificationTracking = suppressModificationTracking;
        return this;
    }

    /**
     * Sets whether to omit expiration timeout renewal for this request.
     */
    public SuppressableAccessTrackingOptions setSuppressExpirationTimeoutRenewal(
            boolean suppressExpirationTimeoutRenewal) {
        this.suppressExpirationTimeoutRenewal = suppressExpirationTimeoutRenewal;
        return this;
    }

    public boolean getSuppressAccessTracking() {
        return suppressAccessTracking;
    }

    public boolean getSuppressModificationTracking() {
        return suppressModificationTracking;
    }

    public boolean getSuppressExpirationTimeoutRenewal() {
        return suppressExpirationTimeoutRenewal;
    }

    public TSuppressableAccessTrackingOptions.Builder writeTo(TSuppressableAccessTrackingOptions.Builder builder) {
        return builder
                .setSuppressAccessTracking(suppressAccessTracking)
                .setSuppressModificationTracking(suppressModificationTracking)
                .setSuppressExpirationTimeoutRenewal(suppressExpirationTimeoutRenewal);
    }
}
