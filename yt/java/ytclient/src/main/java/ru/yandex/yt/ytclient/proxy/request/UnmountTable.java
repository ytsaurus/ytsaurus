package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

public class UnmountTable extends TableReq<UnmountTable> {
    public UnmountTable(String path) {
        super(path);
    }

    @Nonnull
    @Override
    protected UnmountTable self() {
        return this;
    }
}
