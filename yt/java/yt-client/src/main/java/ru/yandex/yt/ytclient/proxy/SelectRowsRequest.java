package ru.yandex.yt.ytclient.proxy;

public class SelectRowsRequest extends ru.yandex.yt.ytclient.request.SelectRowsRequest.BuilderBase<SelectRowsRequest> {
    public static SelectRowsRequest of(String query) {
        return new SelectRowsRequest().setQuery(query);
    }

    @Override
    protected SelectRowsRequest self() {
        return this;
    }
}
