package tech.ytsaurus.client.rows;

public interface WireRowsetDeserializer<T> extends WireRowDeserializer<T> {
    void setRowCount(int rowCount);
}
