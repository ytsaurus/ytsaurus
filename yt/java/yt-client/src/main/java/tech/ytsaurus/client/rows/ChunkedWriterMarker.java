package tech.ytsaurus.client.rows;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ChunkedWriterMarker {

    private byte[] mark;
    private int markPos;

    void onArray(byte[] currentArray, byte[] newArray) {
        if (mark != currentArray) {
            return; // -- либо нет маркировки, либо массив был flush-нут
        }
        mark = newArray;
        if (markPos >= this.mark.length) {
            throw new IllegalArgumentException("Incorrect position at flush: " + markPos +
                    ", length is limited to " + this.mark.length);
        }
    }

    void mark(byte[] currentArray, int position) {
        this.mark = currentArray;
        this.markPos = position;
        if (markPos >= this.mark.length) {
            throw new IllegalArgumentException("Incorrect position: " + markPos +
                    ", length is limited to " + this.mark.length);
        }
    }

    public void writeToMark(ByteBuffer wrappedBuffer, ByteOrder order, long value) {
        if (mark == null) {
            throw new IllegalStateException("Buffer never marked");
        }
        if (wrappedBuffer != null && wrappedBuffer.hasArray() && wrappedBuffer.array() == mark) {
            // Буфер не изменился, его не требуется маппить на промаркированный массив
            wrappedBuffer.putLong(markPos, value);
        } else {
            ByteBuffer.wrap(mark, markPos, mark.length - markPos).order(order).putLong(value);
        }
    }
}
