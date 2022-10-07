package ru.yandex.yt.ytclient.misc;

/**
 * @deprecated Use {@link ru.yandex.inside.yt.kosher.common.YtTimestamp}.
 */
@Deprecated
public class YtTimestamp extends ru.yandex.inside.yt.kosher.common.YtTimestamp {

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

    private YtTimestamp(long value) {
        super(value);
    }

    public static YtTimestamp valueOf(long value) {
        return new YtTimestamp(value);
    }
}
