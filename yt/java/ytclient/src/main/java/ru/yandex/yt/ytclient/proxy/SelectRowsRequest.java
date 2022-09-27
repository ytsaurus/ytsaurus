package ru.yandex.yt.ytclient.proxy;

public class SelectRowsRequest extends ru.yandex.yt.ytclient.request.SelectRowsRequest.BuilderBase<
        SelectRowsRequest, ru.yandex.yt.ytclient.request.SelectRowsRequest> {
    public static SelectRowsRequest of(String query) {
        return new SelectRowsRequest().setQuery(query);
    }

    @Override
    protected SelectRowsRequest self() {
        return this;
    }

    @Override
    public ru.yandex.yt.ytclient.request.SelectRowsRequest build() {
        return new ru.yandex.yt.ytclient.request.SelectRowsRequest(this);
    }
}
