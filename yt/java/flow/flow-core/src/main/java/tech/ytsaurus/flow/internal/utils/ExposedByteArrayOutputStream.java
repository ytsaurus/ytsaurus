package tech.ytsaurus.flow.internal.utils;

import java.io.ByteArrayOutputStream;

import com.google.protobuf.UnsafeByteOperations;

/**
 * {@link ByteArrayOutputStream} that exposes its backing buffer, letting callers wrap the written
 * bytes via {@link UnsafeByteOperations#unsafeWrap(byte[], int, int)} and skip the defensive copy
 * that {@link ByteArrayOutputStream#toByteArray()} makes.
 *
 * <p>Use {@link #buffer()} together with {@link #length()}: only the first {@code length()} bytes
 * hold data, and the array is usually larger than that. The buffer is grown by reallocation as
 * data is written, so any reference returned by {@link #buffer()} is invalidated by subsequent
 * writes; fetch it only after writing is complete and treat the wrapped bytes as read-only.
 */
public final class ExposedByteArrayOutputStream extends ByteArrayOutputStream {
    public ExposedByteArrayOutputStream(int size) {
        super(size);
    }

    /**
     * Returns the backing buffer directly, without copying. Only the first {@link #length()} bytes
     * are valid; the returned reference becomes stale after any further write grows the buffer.
     */
    public byte[] buffer() {
        return buf;
    }

    /**
     * Returns the number of valid bytes currently held in {@link #buffer()}.
     */
    public int length() {
        return count;
    }
}
