package tech.ytsaurus.yson;

public class BufferReference {
    static final byte[] EMPTY_BUFFER = new byte[]{};

    private byte[] buffer = EMPTY_BUFFER;
    private int length = 0;
    private int offset = 0;

    public byte[] getBuffer() {
        return buffer;
    }

    public void setBuffer(byte[] buffer) {
        this.buffer = buffer;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }
}
