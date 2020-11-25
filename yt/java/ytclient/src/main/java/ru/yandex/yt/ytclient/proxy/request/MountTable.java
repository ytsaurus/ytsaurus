package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

public class MountTable extends TableReq<MountTable> {
    public MountTable(String path) {
        super(path);
    }

    @Nonnull
    @Override
    protected MountTable self() {
        return this;
    }
}
