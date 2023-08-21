package tech.ytsaurus.client;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Client interface with common methods for {@link YTsaurusClient} and MockYTsaurusClient.
 */
public interface BaseYTsaurusClient extends ApiServiceClient, Closeable {
    /**
     * Get a list of client clusters
     */
    List<YTsaurusCluster> getClusters();

    /**
     * Get a client scheduled executor
     */
    ScheduledExecutorService getExecutor();
}
