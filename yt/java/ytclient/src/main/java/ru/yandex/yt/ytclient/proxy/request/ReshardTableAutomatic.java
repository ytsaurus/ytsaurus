package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

import ru.yandex.inside.yt.kosher.cypress.YPath;

public class ReshardTableAutomatic extends ru.yandex.yt.ytclient.request.ReshardTableAutomatic.BuilderBase<
        ReshardTableAutomatic> {
    public ReshardTableAutomatic(YPath path, boolean keepActions) {
        setPath(path.justPath()).setKeepActions(keepActions);
    }

    /**
     * @deprecated Use {@link #ReshardTableAutomatic(YPath path,  boolean keepActions)} instead.
     */
    @Deprecated
    public ReshardTableAutomatic(String path, boolean keepActions) {
        setPath(path).setKeepActions(keepActions);
    }

    @Nonnull
    @Override
    protected ReshardTableAutomatic self() {
        return this;
    }
}
