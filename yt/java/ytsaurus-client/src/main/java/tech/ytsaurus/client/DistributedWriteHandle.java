package tech.ytsaurus.client;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import tech.ytsaurus.client.request.DistributedWriteCookie;
import tech.ytsaurus.client.request.DistributedWriteSession;
import tech.ytsaurus.client.request.FinishDistributedWriteSession;
import tech.ytsaurus.client.request.PingDistributedWriteSession;
import tech.ytsaurus.client.request.WriteFragmentResult;

/**
 * A pingable distributed write handle which contains all cookies that must be sent to partition writers. This handle
 * is also responsible for pinging distributed write session.
 */
public class DistributedWriteHandle extends Pingable {
    private final DistributedWriteSession session;
    private final List<DistributedWriteCookie> cookies;
    private final ApiServiceClient client;

    public DistributedWriteHandle(
            DistributedWriteSession session,
            List<DistributedWriteCookie> cookies,
            ApiServiceClient client,
            Duration pingPeriod,
            Duration failedPingRetryPeriod,
            Consumer<Exception> onPingFailed,
            ScheduledExecutorService executor) {
        super(pingPeriod, failedPingRetryPeriod, onPingFailed, executor);
        this.session = session;
        this.cookies = cookies;
        this.client = client;
    }

    public DistributedWriteSession getSession() {
        return session;
    }

    public List<DistributedWriteCookie> getCookies() {
        return cookies;
    }

    @Override
    protected boolean isPingableState(Boolean forcedPingStop) {
        return !forcedPingStop;
    }

    @Override
    public CompletableFuture<Void> ping() {
        PingDistributedWriteSession req = PingDistributedWriteSession.builder().setSession(session).build();
        return client.pingDistributedWriteSession(req);
    }

    public CompletableFuture<Void> finish(List<WriteFragmentResult> writeResults) {
        stopPing();
        FinishDistributedWriteSession req = FinishDistributedWriteSession.builder()
                .setSession(session)
                .setWriteResults(writeResults)
                .build();
        return client.finishDistributedWriteSession(req);
    }
}
