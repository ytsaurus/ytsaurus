package tech.ytsaurus.client;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import javax.annotation.Nullable;

public class MultiExecutor implements Closeable {

    private final List<ClientEntry> clients;
    private final Duration banPenalty;
    private final Duration banDuration;
    private final PenaltyProvider penaltyProvider;
    private final MultiExecutorMonitoring executorMonitoring;

    public MultiExecutor(
            List<MultiYTsaurusClient.YTsaurusClientOptions> clientOptions,
            Duration banPenalty,
            Duration banDuration,
            PenaltyProvider penaltyProvider,
            MultiExecutorMonitoring executorMonitoring
    )
    {
        this.clients = clientOptions.stream().map(ClientEntry::new).toList();
        this.banPenalty = banPenalty;
        this.banDuration = banDuration;
        this.penaltyProvider = penaltyProvider;
        this.executorMonitoring = executorMonitoring;
    }

    <R> CompletableFuture<R> execute(Function<BaseYTsaurusClient, CompletableFuture<R>> callback) {
        return new MultiExecutorRequestTask<>(
                getEffectiveClientOptions(Instant.now()),
                callback,
                executorMonitoring
        ).getFuture();
    }

    private synchronized List<MultiExecutorRequestTask.ClientEntry> getEffectiveClientOptions(Instant now) {
        // update penalties
        for (ClientEntry client : clients) {
            if (client.banUntil != null && client.banUntil.compareTo(now) < 0) {
                client.adaptivePenalty = Duration.ZERO;
            }
            client.externalPenalty = penaltyProvider.getPenalty(client.clientOptions.getShortClusterName());
        }

        // calculate min penalty to subtract
        final Duration minPenalty = Collections.min(clients.stream().map(ClientEntry::getPenalty).toList());

        // create immutable client entries with right effective penalties
        return clients.stream().map((client ) -> new MultiExecutorRequestTask.ClientEntry(
                client.clientOptions,
                client.getPenalty().minus(minPenalty),
                client::onCompleteRequest
        )).toList();
    }

    @Override
    public void close() throws IOException {
        penaltyProvider.close();
    }

    public class ClientEntry {

        private final MultiYTsaurusClient.YTsaurusClientOptions clientOptions;
        private Duration adaptivePenalty;
        private Duration externalPenalty;
        @Nullable
        private Instant banUntil;

        public ClientEntry(
                MultiYTsaurusClient.YTsaurusClientOptions clientOptions
        )
        {
            this(clientOptions, Duration.ZERO, Duration.ZERO, null);
        }

        public ClientEntry(
                MultiYTsaurusClient.YTsaurusClientOptions clientOptions,
                Duration adaptivePenalty,
                Duration externalPenalty,
                @Nullable Instant banUntil
        )
        {
            if (clientOptions.client.getClusters().size() != 1) {
                throw new IllegalArgumentException("Got YTsaurusClient with more than 1 cluster");
            }
            this.clientOptions = clientOptions;
            this.adaptivePenalty = adaptivePenalty;
            this.externalPenalty = externalPenalty;
            this.banUntil = banUntil;
        }

        MultiYTsaurusClient.YTsaurusClientOptions getClientOptions() {
            return clientOptions;
        }

        Duration getPenalty() {
            return clientOptions.initialPenalty.plus(adaptivePenalty).plus(externalPenalty);
        }

        public synchronized void onCompleteRequest(Boolean success) {
            if (success) {
                if (this.adaptivePenalty.compareTo(Duration.ZERO) > 0) {
                    this.banUntil = null;
                    this.adaptivePenalty = Duration.ZERO;
                }
            } else {
                this.banUntil = Instant.now().plus(banDuration);
                this.adaptivePenalty = this.adaptivePenalty.plus(banPenalty);
            }
        }

    }

}

