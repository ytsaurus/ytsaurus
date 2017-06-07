package ru.yandex.yt.rpc.client.requests;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import ru.yandex.yt.rpc.client.schema.TableSchema;

/**
 * @author valri
 */
public class LookupReqInfo {
    public UUID uuid;
    public TableSchema tableSchema;
    public List<Map<String, Object>> filters;
    public Integer wireFormat;
    public Long timestamp;
    public Boolean keepMissingRows;

    /**
     *
     * @param tableSchema   dynamic table schema with all essential fields
     * @param filters       filters like list of maps { columnId --> value }
     * @param wireFormat    version of wire format
     */
    public LookupReqInfo(TableSchema tableSchema, List<Map<String, Object>> filters, int wireFormat, UUID uuid) {
        this(tableSchema, filters, wireFormat, uuid, null, null);
    }

    /**
     *
     * @param tableSchema       dynamic table schema with all essential fields
     * @param filters           filters like list of maps { columnId --> value }
     * @param wireFormat        version of wire format
     * @param timestamp         read events starting from specified timestamp
     * @param keepMissingRows   retain deleted rows
     */
    public LookupReqInfo(TableSchema tableSchema, List<Map<String, Object>> filters, int wireFormat, UUID uuid,
                         Long timestamp, Boolean keepMissingRows)
    {
        this.tableSchema = tableSchema;
        this.filters = filters;
        this.wireFormat = wireFormat;
        this.uuid = uuid;
        this.timestamp = timestamp;
        this.keepMissingRows = keepMissingRows;
    }
}
