package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

public class FreezeTable extends TableReq<FreezeTable> {
    public FreezeTable(String path) {
        super(path);
    }

    @Nonnull
    @Override
    protected FreezeTable self() {
        return this;
    }
}
