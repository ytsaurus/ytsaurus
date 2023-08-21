package tech.ytsaurus.core;

import java.time.Instant;

import javax.annotation.Nonnull;

/**
 * Timestamp is a cluster-wide unique monotonically increasing number used to implement the MVCC paradigm.
 * <p>
 * Timestamp is a 64-bit unsigned integer of the following structure:
 * bits  0-29:  auto-incrementing counter (allowing up to ~10^9 timestamps per second)
 * bits 30-61:  Unix time in seconds (from 1 Jan 1970)
 * bits 62-63:  reserved
 */
public class YtTimestamp {
    public static final int COUNTER_BITS = 30;
    public static final long COUNTER_MASK = (1L << COUNTER_BITS) - 1;

    /**
     * Minimum valid (non-sentinel) timestamp.
     */
    public static final YtTimestamp MIN = new YtTimestamp(0x0000000000000001L);

    /**
     * Maximum valid (non-sentinel) timestamp.
     */
    public static final YtTimestamp MAX = new YtTimestamp(0x3fffffffffffff00L);

    /**
     * Uninitialized/invalid timestamp.
     */
    public static final YtTimestamp NULL = new YtTimestamp(0x0000000000000000L);

    /**
     * Truly (serializable) latest committed version.
     * May cause row blocking if concurrent writes are in progress.
     */
    public static final YtTimestamp SYNC_LAST_COMMITTED = new YtTimestamp(0x3fffffffffffff01L);

    /**
     * Relaxed (non-serializable) latest committed version.
     * Never leads to row blocking but may miss some concurrent writes.
     */
    public static final YtTimestamp ASYNC_LAST_COMMITTED = new YtTimestamp(0x3fffffffffffff04L);

    private final long value;

    protected YtTimestamp(long value) {
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    public Instant getInstant() {
        return Instant.ofEpochSecond(value >>> COUNTER_BITS);
    }

    public long getCounter() {
        return value & COUNTER_MASK;
    }

    @Override
    public String toString() {
        return getInstant() + "/" + getCounter();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof YtTimestamp)) {
            return false;
        }

        YtTimestamp that = (YtTimestamp) o;

        return value == that.value;
    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }

    public static YtTimestamp valueOf(long value) {
        return new YtTimestamp(value);
    }

    public static YtTimestamp fromInstant(@Nonnull Instant instant) {
        return new YtTimestamp(instant.getEpochSecond() << COUNTER_BITS);
    }
}
