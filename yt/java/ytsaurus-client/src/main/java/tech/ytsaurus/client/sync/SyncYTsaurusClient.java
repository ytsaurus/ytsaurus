package tech.ytsaurus.client.sync;

import java.util.List;

import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.YTsaurusCluster;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * Synchronous YTsaurus client.
 */
public class SyncYTsaurusClient
        extends SyncCompoundClientImpl {
    private final YTsaurusClient client;

    protected SyncYTsaurusClient(YTsaurusClient client) {
        super(client);
        this.client = client;
    }

    public List<YTsaurusCluster> getClusters() {
        return client.getClusters();
    }

    /**
     * Create builder for SyncYTsaurusClient.
     */
    public static YTsaurusClient.ClientBuilder<? extends SyncYTsaurusClient, ?> builder() {
        return new Builder();
    }

    @Override
    public void close() {
        client.close();
    }

    @NonNullApi
    @NonNullFields
    public static class Builder extends YTsaurusClient.ClientBuilder<SyncYTsaurusClient, Builder> {
        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public SyncYTsaurusClient build() {
            return new SyncYTsaurusClient(
                    this.copyTo(YTsaurusClient.builder())
                            .build()
            );
        }
    }
}
