package tech.ytsaurus.client;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

abstract class Pingable {
    private final Duration pingPeriod;
    private final Duration failedPingRetryPeriod;
    private final AtomicReference<Boolean> forcedPingStop = new AtomicReference<>(false);
    private final Consumer<Exception> onPingFailed;
    private final ScheduledExecutorService executor;

    protected Pingable(
            Duration pingPeriod,
            Duration failedPingRetryPeriod,
            Consumer<Exception> onPingFailed,
            ScheduledExecutorService executor) {
        this.pingPeriod = pingPeriod;
        this.failedPingRetryPeriod = isValidPingPeriod(failedPingRetryPeriod) ? failedPingRetryPeriod : pingPeriod;
        this.onPingFailed = onPingFailed;
        this.executor = executor;

        if (isValidPingPeriod(pingPeriod)) {
            executor.schedule(this::runPeriodicPings, pingPeriod.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    public void stopPing() {
        forcedPingStop.set(true);
    }

    public abstract CompletableFuture<Void> ping();

    protected boolean ignorePingThrowable(Throwable t) {
        return false;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    protected abstract boolean isPingableState(Boolean forcedPingStop);

    private void runPeriodicPings() {
        if (!isPingableState(forcedPingStop.get())) {
            return;
        }

        ping().whenComplete((unused, ex) -> {
            long nextPingDelayMs;

            if (ex == null) {
                nextPingDelayMs = pingPeriod.toMillis();
            } else {
                nextPingDelayMs = failedPingRetryPeriod.toMillis();

                if (onPingFailed != null) {
                    onPingFailed.accept(ex instanceof Exception ? (Exception) ex : new RuntimeException(ex));
                }
                if (ignorePingThrowable(ex)) {
                    return;
                }
            }

            if (!isPingableState(forcedPingStop.get())) {
                return;
            }
            executor.schedule(this::runPeriodicPings, nextPingDelayMs, TimeUnit.MILLISECONDS);
        });
    }

    private static boolean isValidPingPeriod(Duration pingPeriod) {
        return pingPeriod != null && !pingPeriod.isZero() && !pingPeriod.isNegative();
    }
}
