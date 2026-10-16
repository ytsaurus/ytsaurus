package tech.ytsaurus.core.rows;

import tech.ytsaurus.typeinfo.TiType;

/**
 * Variant of {@link YTreeBytesSerializer} that exposes byte arrays as YT {@code string} columns.
 */
public class YTreeBytesToStringSerializer extends YTreeBytesSerializer {
    @Override
    public TiType getColumnValueType() {
        return TiType.string();
    }
}
