package tech.ytsaurus.client;

public class SelectRowsRequest extends tech.ytsaurus.client.request.SelectRowsRequest.BuilderBase<SelectRowsRequest> {
    public static SelectRowsRequest of(String query) {
        return new SelectRowsRequest().setQuery(query);
    }

    @Override
    protected SelectRowsRequest self() {
        return this;
    }
}
