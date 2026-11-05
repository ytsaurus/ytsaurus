package tech.ytsaurus.flow.examples.reanimate.model;

import javax.persistence.Entity;

@Entity
public class SecretState {
    private long count;
    private String secret;

    public SecretState() {
    }

    public SecretState(long count, String secret) {
        this.count = count;
        this.secret = secret;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getSecret() {
        return secret;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }
}
