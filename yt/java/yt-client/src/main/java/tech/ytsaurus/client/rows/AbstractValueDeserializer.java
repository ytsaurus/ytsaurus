package tech.ytsaurus.client.rows;

import tech.ytsaurus.core.tables.ColumnValueType;

abstract class AbstractValueDeserializer<T> implements WireValueDeserializer<T> {
    protected int id;
    protected ColumnValueType type;
    protected boolean aggregate;
    protected long timestamp;
    protected Object value;

    @Override
    public void setId(int id) {
        this.id = id;
    }

    @Override
    public void setType(ColumnValueType type) {
        this.type = type;
    }

    @Override
    public void setAggregate(boolean aggregate) {
        this.aggregate = aggregate;
    }

    @Override
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public void onEntity() {
        this.value = null;
    }

    @Override
    public void onBytes(byte[] value) {
        this.value = value;
    }

    @Override
    public void onInteger(long value) {
        this.value = value;
    }

    @Override
    public void onDouble(double value) {
        this.value = value;
    }

    @Override
    public void onBoolean(boolean value) {
        this.value = value;
    }
}
