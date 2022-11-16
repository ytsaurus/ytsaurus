package ru.yandex.yt.ytclient.proxy;

import ru.yandex.lang.NonNullApi;
import ru.yandex.yt.ytclient.tables.TableSchema;

@NonNullApi
public class LookupRowsRequest extends tech.ytsaurus.client.request.LookupRowsRequest.BuilderBase<LookupRowsRequest> {
    public LookupRowsRequest(String path, TableSchema schema) {
        setPath(path).setSchema(schema);
    }

    @Override
    protected LookupRowsRequest self() {
        return this;
    }

    @Override
    public tech.ytsaurus.client.request.LookupRowsRequest build() {
        return new tech.ytsaurus.client.request.LookupRowsRequest(this);
    }
}
