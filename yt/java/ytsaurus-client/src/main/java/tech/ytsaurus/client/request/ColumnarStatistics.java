package tech.ytsaurus.client.request;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rows.CompactUnversionedRowReader;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.rpcproxy.TColumnarStatistics;

/**
 * Statistics for one requested path. Per-column lists follow {@link tech.ytsaurus.core.cypress.YPath#getColumns()}.
 */
public class ColumnarStatistics {
    private final List<Long> columnDataWeights;
    @Nullable
    private final Long timestampTotalWeight;
    private final long legacyChunkDataWeight;
    private final List<UnversionedValue> columnMinValues;
    private final List<UnversionedValue> columnMaxValues;
    private final List<Long> columnNonNullValueCounts;
    private final List<HyperLogLogDigest> columnHyperLogLogDigests;
    @Nullable
    private final Long chunkRowCount;
    @Nullable
    private final Long legacyChunkRowCount;
    @Nullable
    private final Long readSizeEstimate;

    private ColumnarStatistics(TColumnarStatistics statistics) {
        this.columnDataWeights = List.copyOf(statistics.getColumnDataWeightsList());
        this.timestampTotalWeight = statistics.hasTimestampTotalWeight()
                ? statistics.getTimestampTotalWeight()
                : null;
        this.legacyChunkDataWeight = statistics.getLegacyChunkDataWeight();
        this.columnMinValues = readValues(statistics.getColumnMinValues());
        this.columnMaxValues = readValues(statistics.getColumnMaxValues());
        this.columnNonNullValueCounts = List.copyOf(statistics.getColumnNonNullValueCountsList());
        this.columnHyperLogLogDigests = statistics.getColumnHyperloglogDigestsList().stream()
                .map(HyperLogLogDigest::new)
                .collect(Collectors.toUnmodifiableList());
        this.chunkRowCount = statistics.hasChunkRowCount() ? statistics.getChunkRowCount() : null;
        this.legacyChunkRowCount = statistics.hasLegacyChunkRowCount() ? statistics.getLegacyChunkRowCount() : null;
        this.readSizeEstimate = statistics.hasReadSizeEstimate() ? statistics.getReadSizeEstimate() : null;
    }

    public static ColumnarStatistics fromProto(TColumnarStatistics statistics) {
        return new ColumnarStatistics(statistics);
    }

    private static List<UnversionedValue> readValues(ByteString data) {
        UnversionedRow row = new CompactUnversionedRowReader(data).readUnversionedRow();
        return row == null ? List.of() : row.getValues();
    }

    /**
     * Per-column total data weight of the chunks whose meta contains columnar statistics.
     */
    public List<Long> getColumnDataWeights() {
        return columnDataWeights;
    }

    /**
     * Total data weight of the legacy chunks whose meta misses columnar statistics.
     */
    public long getLegacyChunkDataWeight() {
        return legacyChunkDataWeight;
    }

    /**
     * Total weight of all write and delete timestamps.
     */
    public Optional<Long> getTimestampTotalWeight() {
        return Optional.ofNullable(timestampTotalWeight);
    }

    /**
     * Per-column approximate minimum non-null values.
     * <p>
     * A value is less than or equal to any value of the column but may not be a real value of it
     * (e.g. a string may be truncated).
     * {@link tech.ytsaurus.core.tables.ColumnValueType#MIN}: the minimum is unknown.
     * {@link tech.ytsaurus.core.tables.ColumnValueType#NULL}: the column is empty or contains only nulls.
     * The list is empty if value statistics are unavailable, see {@link #getColumnNonNullValueCounts()}.
     */
    public List<UnversionedValue> getColumnMinValues() {
        return columnMinValues;
    }

    /**
     * Per-column approximate maximum non-null values.
     * <p>
     * A value is greater than or equal to any value of the column but may not be a real value of it
     * (e.g. a string may be truncated).
     * {@link tech.ytsaurus.core.tables.ColumnValueType#MAX}: the maximum is unknown.
     * {@link tech.ytsaurus.core.tables.ColumnValueType#NULL}: the column is empty or contains only nulls.
     * The list is empty if value statistics are unavailable, see {@link #getColumnNonNullValueCounts()}.
     */
    public List<UnversionedValue> getColumnMaxValues() {
        return columnMaxValues;
    }

    /**
     * Per-column number of non-null values.
     * <p>
     * Like min/max values, the list is empty if value statistics are unavailable: for data written before
     * they were introduced or if the statistics were taken from master.
     */
    public List<Long> getColumnNonNullValueCounts() {
        return columnNonNullValueCounts;
    }

    /**
     * Per-column HyperLogLog digests in requested column order.
     * The list is empty when the server does not provide these statistics.
     */
    public List<HyperLogLogDigest> getColumnHyperLogLogDigests() {
        return columnHyperLogLogDigests;
    }

    /**
     * Total number of rows in the chunks whose meta contains columnar statistics.
     */
    public Optional<Long> getChunkRowCount() {
        return Optional.ofNullable(chunkRowCount);
    }

    /**
     * Total number of rows in the legacy chunks whose meta misses columnar statistics.
     */
    public Optional<Long> getLegacyChunkRowCount() {
        return Optional.ofNullable(legacyChunkRowCount);
    }

    /**
     * Estimate of the data size in bytes to be read from disk for the requested columns.
     * Present only when read size estimation is enabled on the server request.
     */
    public Optional<Long> getReadSizeEstimate() {
        return Optional.ofNullable(readSizeEstimate);
    }
}
