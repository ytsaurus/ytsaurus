package tech.ytsaurus.client;

import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.lang.NonNullApi;

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
