package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nonnull;

public class RemountTable extends TableReq<RemountTable> {
    public RemountTable(String path) {
        super(path);
    }

    @Nonnull
    @Override
    protected RemountTable self() {
        return this;
    }
}
