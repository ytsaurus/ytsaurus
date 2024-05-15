package tech.ytsaurus.client.discovery;

import java.util.List;

public interface Discoverer {
    /**
     * Return current discovery servers addresses at the moment.
     */
    List<String> listDiscoveryServers();

    /**
     * Stop any discovery activity.
     */
    void stop();
}
