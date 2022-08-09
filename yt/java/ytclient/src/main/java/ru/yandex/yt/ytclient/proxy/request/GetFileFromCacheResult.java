package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.inside.yt.kosher.cypress.YPath;

public class GetFileFromCacheResult {
    private final YPath path;

    public GetFileFromCacheResult(YPath path) {
        this.path = path;
    }

    public YPath getPath() {
        return path;
    }
}
