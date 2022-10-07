package ru.yandex.yt.ytclient.request;

import java.util.Optional;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
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
