package ru.yandex.yt.rpc.client.responses;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * All the info received from YT versioned row, where link values put into map and
 * all `keys`(aka key columns) are presented as simple Objects and `values`(aka non-key columns)
 * are lists of VersionedValues.
 *
 * @author valri
 */
public class VersionedLookupRow {
    private Map<String, Object> values;
    private List<Long> writeTimestamps;
    private List<Long> deleteTimestamps;

    /**
     * Instance of versionedRow with delete and write timestamps for row itself and for all `values`.
     */
    public VersionedLookupRow() {
        this.values = new HashMap<>();
        this.writeTimestamps = new ArrayList<>();
        this.deleteTimestamps = new ArrayList<>();
    }

    public void addValue(String column, Object value) {
        // for sure - if repeats - it's a value
        if (values.containsKey(column)) {
            ((List) values.get(column)).add(value);
        } else if (value instanceof VersionedValue) {
            List<VersionedValue> multipleValues = new ArrayList<>();
            multipleValues.add((VersionedValue) value);
            values.put(column, multipleValues);
        } else {
            values.put(column, value);
        }
    }

    public Map<String, Object> getValues() {
        return values;
    }

    public List<Long> getWriteTimestamps() {
        return writeTimestamps;
    }

    public void setWriteTimestamps(List<Long> writeTimestamps) {
        this.writeTimestamps = writeTimestamps;
    }

    public List<Long> getDeleteTimestamps() {
        return deleteTimestamps;
    }

    public void setDeleteTimestamps(List<Long> deleteTimestamps) {
        this.deleteTimestamps = deleteTimestamps;
    }

    @Override
    public String toString() {
        return MessageFormat.format("Values:\n{0}\nWrite timestamps:\n{1}\nDelete timestamps:\n{2}",
                this.values, this.writeTimestamps, this.deleteTimestamps);
    }

    public static class VersionedValue {
        private Object value;
        private Long timestamp;

        public Object getValue() {
            return value;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public VersionedValue(Object obj, long timestamp) {
            this.value = obj;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return MessageFormat.format("Value: {0} with timestamp: {1}", this.value, this.timestamp);
        }
    }
}
