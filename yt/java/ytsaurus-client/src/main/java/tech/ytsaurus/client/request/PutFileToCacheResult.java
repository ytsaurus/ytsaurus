package tech.ytsaurus.client.request;

import tech.ytsaurus.core.cypress.YPath;

public class PutFileToCacheResult {
    private final YPath path;

    public PutFileToCacheResult(YPath path) {
        this.path = path;
    }

    public YPath getPath() {
        return path;
    }
}
