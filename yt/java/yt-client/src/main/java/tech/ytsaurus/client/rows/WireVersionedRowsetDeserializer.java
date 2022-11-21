package tech.ytsaurus.client.rows;

public interface WireVersionedRowsetDeserializer<T> extends WireVersionedRowDeserializer<T> {

    void setRowCount(int rowCount);
}
