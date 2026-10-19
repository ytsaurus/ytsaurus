package tech.ytsaurus.flow.internal.row;

import static tech.ytsaurus.flow.internal.row.CompactWireFormat.rangeLength;
import static tech.ytsaurus.flow.internal.row.CompactWireFormat.rangeOffset;

/**
 * Decoded row cells: the type and wire offset of each cell, and a value slot that holds raw
 * scalar bits or the wire range of a string-like value (see
 * {@link CompactWireFormat#packRange}); sentinel cells leave the slot 0.
 * The decoder fills cells before publication; readers never modify them.
 */
abstract class DecodedCells {
    static DecodedCells create(int cellCount) {
        return cellCount == 1 ? new SingleCell() : new CellArrays(cellCount);
    }

    abstract int capacity();

    abstract int type(int columnId);

    abstract long value(int columnId);

    /**
     * Returns the wire offset of the column id that starts a cell.
     * {@code encodedCellOffset(cellCount)} is the end of the row.
     */
    abstract int encodedCellOffset(int columnId);

    abstract void set(int columnId, int type, long value, int offset);

    abstract void end(int offset);

    /**
     * Returns the wire offset of the bytes after a string-like value's length prefix.
     */
    final int valueOffset(int columnId) {
        return rangeOffset(value(columnId));
    }

    final int valueLength(int columnId) {
        return rangeLength(value(columnId));
    }

    final int encodedCellLength(int columnId) {
        return encodedCellOffset(columnId + 1) - encodedCellOffset(columnId);
    }

    private static final class SingleCell extends DecodedCells {
        private int type;
        private long value;
        private int start;
        private int end;

        @Override
        int capacity() {
            return 1;
        }

        @Override
        int type(int columnId) {
            return type;
        }

        @Override
        long value(int columnId) {
            return value;
        }

        @Override
        int encodedCellOffset(int columnId) {
            return columnId == 0 ? start : end;
        }

        @Override
        void set(int columnId, int type, long value, int offset) {
            this.type = type;
            this.value = value;
            this.start = offset;
        }

        @Override
        void end(int offset) {
            end = offset;
        }
    }

    private static final class CellArrays extends DecodedCells {
        private final byte[] types;
        private final long[] values;
        private final int[] encodedCellOffsets;

        private CellArrays(int cellCount) {
            types = new byte[cellCount];
            values = new long[cellCount];
            encodedCellOffsets = new int[cellCount + 1];
        }

        @Override
        int capacity() {
            return types.length;
        }

        @Override
        int type(int columnId) {
            return types[columnId] & 0xff;
        }

        @Override
        long value(int columnId) {
            return values[columnId];
        }

        @Override
        int encodedCellOffset(int columnId) {
            return encodedCellOffsets[columnId];
        }

        @Override
        void set(int columnId, int type, long value, int offset) {
            types[columnId] = (byte) type;
            values[columnId] = value;
            encodedCellOffsets[columnId] = offset;
        }

        @Override
        void end(int offset) {
            encodedCellOffsets[types.length] = offset;
        }
    }
}
