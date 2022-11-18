package tech.ytsaurus.client.request;

import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.core.cypress.YPath;


public class GetFileFromCacheResult {
    @Nullable
    private final YPath path;

    public GetFileFromCacheResult(@Nullable YPath path) {
        this.path = path;
    }

    public Optional<YPath> getPath() {
        return Optional.ofNullable(path);
    }
}
