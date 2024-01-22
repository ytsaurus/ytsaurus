package tech.ytsaurus.client.rows;

public class QueueRowset extends UnversionedRowset {
    private final long startOffset;

    public QueueRowset(UnversionedRowset rowset, long startOffset) {
        super(rowset.getSchema(), rowset.getRows());
        this.startOffset = startOffset;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getFinishOffset() {
        return getStartOffset() + getRows().size();
    }
}
