package tech.ytsaurus.client.rows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Позволяет писать бинарные данные чанками
 */
public class ChunkedWriter {
    private static final byte[] EMPTY_BUFFER = new byte[0];
    private static final int MIN_CHUNK_SIZE = 1024; // 1KB
    static final int MAX_CHUNK_SIZE = 256 * 1024 * 1024; // 256MB

    private final List<byte[]> output;
    private final int maxChunkSize;
    private final ChunkedWriterMarker marker;
    private byte[] buffer;
    private int offset;

    public ChunkedWriter() {
        this(new ArrayList<>());
    }

    public ChunkedWriter(List<byte[]> output) {
        this(output, 0);
    }

    public ChunkedWriter(List<byte[]> output, int initialReserveSize) {
        this(output, initialReserveSize, MAX_CHUNK_SIZE);
    }

    ChunkedWriter(List<byte[]> output, int initialReserveSize, int limitChunkSize) {
        this.maxChunkSize = Math.min(Math.max(limitChunkSize, MIN_CHUNK_SIZE), MAX_CHUNK_SIZE);
        this.marker = new ChunkedWriterMarker();
        initialReserveSize = minPowerOfTwo(initialReserveSize);
        if (initialReserveSize > maxChunkSize || initialReserveSize < 0) {
            initialReserveSize = maxChunkSize;
        }

        this.output = output;
        this.buffer = initialReserveSize > 0 ? new byte[initialReserveSize] : EMPTY_BUFFER;
        this.offset = 0;
    }

    public ChunkedWriterMarker getMarker() {
        return marker;
    }

    public byte[] buffer() {
        return buffer;
    }

    public int offset() {
        return offset;
    }

    public int capacity() {
        return buffer.length;
    }

    public int remaining() {
        return buffer.length - offset;
    }

    /**
     * Завершает текущий чанк и возвращает полный набор чанков
     */
    public List<byte[]> flush() {
        final byte[] bytes = Arrays.copyOfRange(buffer, 0, offset);
        marker.onArray(buffer, bytes);
        output.add(bytes);
        offset = 0;
        return output;
    }

    /**
     * Резервирует в текущем чанке место для записи не менее size байт
     */
    public void reserve(int size) {
        if (remaining() < size) {
            int newSize;
            if (offset > 0) {
                // Нам всё-равно данные копировать, лучше скопировать их в буфер побольше
                newSize = offset + size;
                if (newSize < 0 || newSize > maxChunkSize) {
                    // Совместный чанк получится слишком большой, скидываем текущие данные
                    newSize = size;
                    flush();
                }
            } else {
                newSize = size;
            }
            newSize = minPowerOfTwo(newSize);
            if (newSize < 0) {
                newSize = Integer.MAX_VALUE;
            }
            newSize = Math.max(newSize, MIN_CHUNK_SIZE);
            newSize = Math.max(newSize, buffer.length);
            if (newSize > buffer.length) {
                byte[] newBuffer = new byte[newSize];
                if (offset > 0) {
                    System.arraycopy(buffer, 0, newBuffer, 0, offset);
                }
                marker.onArray(buffer, newBuffer);
                buffer = newBuffer;
            }
        }
    }

    void mark(int position) {
        this.marker.mark(buffer, position);
    }


    /**
     * Увеличивает текущую позицию на size байт
     */
    public void advance(int size) {
        if (remaining() < size) {
            throw new IndexOutOfBoundsException("not enough space in the buffer");
        }
        this.offset += size;
    }

    /**
     * Копирует данные из src
     */
    public void write(byte[] src) {
        reserve(src.length);
        System.arraycopy(src, 0, this.buffer, this.offset, src.length);
        this.offset += src.length;
    }

    /**
     * Копирует данные из src с позиции offset и длиной length байт
     */
    public void write(byte[] src, int offset, int length) {
        checkBounds(offset, length, src.length);
        reserve(length);
        System.arraycopy(src, offset, this.buffer, this.offset, length);
        this.offset += length;
    }

    /**
     * Возвращает минимальную степень двойки, большую либо равную n
     */
    private static int minPowerOfTwo(int n) {
        --n;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        ++n;
        return n;
    }

    /**
     * Проверяет, что offset и length валидны для массива размером size
     */
    private static void checkBounds(int offset, int length, int size) {
        // Условие срабатывает, если что-либо оказывается меньше нуля
        if ((offset | length | (offset + length) | (size - (offset + length))) < 0) {
            throw new IndexOutOfBoundsException();
        }
    }
}
