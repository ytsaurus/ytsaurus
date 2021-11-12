package ru.yandex.yt.ytclient.proxy;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Client interface with common methods for YtClient and MockYtClient.
 */
public interface BaseYtClient extends TransactionalClient {
    /**
     * Get a list of client clusters
     */
    List<YtCluster> getClusters();

    /**
     * Get a client scheduled executor
     */
    ScheduledExecutorService getExecutor();
}
