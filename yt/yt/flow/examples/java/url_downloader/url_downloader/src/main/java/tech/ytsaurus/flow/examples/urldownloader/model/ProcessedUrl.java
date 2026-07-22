package tech.ytsaurus.flow.examples.urldownloader.model;

import javax.persistence.Entity;

// Field order must match the "processed_urls" stream definition in the static spec.
@Entity
public class ProcessedUrl {
    private String host;
    private String url;
    private String data;

    public ProcessedUrl() {
    }

    public ProcessedUrl(String host, String url, String data) {
        this.host = host;
        this.url = url;
        this.data = data;
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

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
