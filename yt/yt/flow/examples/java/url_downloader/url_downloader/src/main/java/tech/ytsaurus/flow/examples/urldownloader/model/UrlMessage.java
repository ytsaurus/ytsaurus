package tech.ytsaurus.flow.examples.urldownloader.model;

import javax.persistence.Entity;

// Field order must match the "urls" stream definition in the static spec.
@Entity
public class UrlMessage {
    private String host;

    private String url;

    public UrlMessage() {
    }

    public UrlMessage(String host, String url) {
        this.host = host;
        this.url = url;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}
