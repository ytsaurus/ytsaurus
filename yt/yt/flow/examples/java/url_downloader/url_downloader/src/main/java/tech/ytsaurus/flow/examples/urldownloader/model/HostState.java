package tech.ytsaurus.flow.examples.urldownloader.model;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;

/**
 * Internal state for a single host tracked by the url downloader.
 * Serialized as YSON for internal state storage.
 */
// [BEGIN host_state]
@Entity
public class HostState {
    private String host;

    @Column(name = "pending_urls")
    private List<String> pendingUrls = new ArrayList<>();

    public HostState() {
    }

    public HostState(String host) {
        this.host = host;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public List<String> getPendingUrls() {
        return pendingUrls;
    }

    public void setPendingUrls(List<String> urls) {
        this.pendingUrls = urls;
    }
}
// [END host_state]
