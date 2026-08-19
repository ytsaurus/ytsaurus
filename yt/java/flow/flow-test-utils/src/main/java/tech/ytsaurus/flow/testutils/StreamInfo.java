package tech.ytsaurus.flow.testutils;

import tech.ytsaurus.core.tables.TableSchema;

public class StreamInfo {
    String streamId;
    TableSchema schema;
    Long streamSpecId;

    public StreamInfo(String streamId, TableSchema schema, Long streamSpecId) {
        this.streamId = streamId;
        this.schema = schema;
        this.streamSpecId = streamSpecId;
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public Long getStreamSpecId() {
        return streamSpecId;
    }

    public void setStreamSpecId(Long streamSpecId) {
        this.streamSpecId = streamSpecId;
    }

    public TableSchema getSchema() {
        return schema;
    }

    public void setSchema(TableSchema schema) {
        this.schema = schema;
    }
}
