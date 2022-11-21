package tech.ytsaurus.client.rows;

public class Bitmap {
    private final long[] bitmap;
    private final int bitCount;

    public Bitmap(int bitCount) {
        this.bitmap = new long[computeChunkCount(bitCount)];
        this.bitCount = bitCount;
    }

    public static int computeChunkCount(int bitCount) {
        return (bitCount + 63) / 64;
    }

    public boolean getBit(int index) {
        if (index < 0 || index >= bitCount) {
            throw new IndexOutOfBoundsException();
        }
        int chunkIndex = index >> 6;
        return (bitmap[chunkIndex] & (1L << (index & 63))) != 0;
    }

    public void setBit(int index) {
        if (index < 0 || index >= bitCount) {
            throw new IndexOutOfBoundsException();
        }
        int chunkIndex = index >> 6;
        bitmap[chunkIndex] |= 1L << (index & 63);
    }

    public void clearBit(int index) {
        if (index < 0 || index >= bitCount) {
            throw new IndexOutOfBoundsException();
        }
        int chunkIndex = index >> 6;
        bitmap[chunkIndex] &= ~(1L << (index & 63));
    }

    public void setBit(int index, boolean value) {
        if (value) {
            setBit(index);
        } else {
            clearBit(index);
        }
    }

    public int getBitCount() {
        return bitCount;
    }

    public long getChunk(int index) {
        return bitmap[index];
    }

    public void setChunk(int index, long value) {
        bitmap[index] = value;
    }

    public int getChunkCount() {
        return bitmap.length;
    }
}
