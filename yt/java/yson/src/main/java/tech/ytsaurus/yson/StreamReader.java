package tech.ytsaurus.yson;

public class StreamReader {
    public static final int END_OF_STREAM = Integer.MAX_VALUE;

    private final ZeroCopyInput underlying;
    private final BufferReference tmp = new BufferReference();

    private byte[] buffer = BufferReference.EMPTY_BUFFER;
    private int bufferOffset = 0;
    private int bufferLength = 0;
    private byte[] largeStringBuffer = BufferReference.EMPTY_BUFFER;

    private int position = 0;

    StreamReader(byte[] buffer) {
        this(buffer, 0, buffer.length);
    }

    StreamReader(byte[] buffer, int offset, int length) {
        this(new ByteZeroCopyInput(buffer, offset, length));
    }

    public StreamReader(ZeroCopyInput underlying) {
        this.underlying = underlying;
    }

    public int getPosition() {
        return position;
    }

    public int tryReadByte() {
        if (bufferOffset >= bufferLength) {
            if (!nextBuffer()) {
                return END_OF_STREAM;
            }
        }
        ++position;
        return buffer[bufferOffset++];
    }

    public byte readByte() {
        if (bufferOffset >= bufferLength) {
            if (!nextBuffer()) {
                throwUnexpectedEndOfStream();
            }
        }
        ++position;
        return buffer[bufferOffset++];
    }

    public void unreadByte() {
        if (bufferOffset == 0) {
            throw new IllegalStateException();
        }
        --bufferOffset;
        --position;
    }

    public void readBytes(int length, BufferReference out) {
        if (bufferOffset + length <= bufferLength) {
            out.setBuffer(buffer);
            out.setOffset(bufferOffset);
            out.setLength(length);
            bufferOffset += length;
        } else {
            if (largeStringBuffer.length < length) {
                largeStringBuffer = new byte[length];
            }
            int pos = 0;
            while (pos < length) {
                int toCopy = Math.min(bufferLength - bufferOffset, length - pos);
                System.arraycopy(buffer, bufferOffset, largeStringBuffer, pos, toCopy);
                pos += toCopy;
                bufferOffset += toCopy;
                if (pos < length) {
                    if (!nextBuffer()) {
                        throwUnexpectedEndOfStream();
                    }
                } else {
                    break;
                }
            }

            out.setBuffer(largeStringBuffer);
            out.setOffset(0);
            out.setLength(length);
        }
        position += length;
    }

    public long readVarUint64() {
        fastPath:
        {
            int pos = bufferOffset;

            if (bufferLength == pos) {
                break fastPath;
            }

            long x;
            int y;
            if ((y = buffer[pos++]) >= 0) {
                position += (pos - bufferOffset);
                bufferOffset = pos;
                return y;
            } else if (bufferLength - pos < 9) {
                break fastPath;
            } else if ((x = y ^ (buffer[pos++] << 7)) < 0L) {
                x ^= (~0L << 7);
            } else if ((x ^= (buffer[pos++] << 14)) >= 0L) {
                x ^= (~0L << 7) ^ (~0L << 14);
            } else if ((x ^= (buffer[pos++] << 21)) < 0L) {
                x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21);
            } else if ((x ^= ((long) buffer[pos++] << 28)) >= 0L) {
                x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28);
            } else if ((x ^= ((long) buffer[pos++] << 35)) < 0L) {
                x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35);
            } else if ((x ^= ((long) buffer[pos++] << 42)) >= 0L) {
                x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35) ^ (~0L << 42);
            } else if ((x ^= ((long) buffer[pos++] << 49)) < 0L) {
                x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35) ^ (~0L << 42)
                        ^ (~0L << 49);
            } else {
                x ^= ((long) buffer[pos++] << 56);
                x ^= (~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35) ^ (~0L << 42)
                        ^ (~0L << 49) ^ (~0L << 56);
                if (x < 0L) {
                    if (buffer[pos++] < 0L) {
                        break fastPath;  // Will throw malformed varint
                    }
                }
            }
            position += (pos - bufferOffset);
            bufferOffset = pos;
            return x;
        }

        // Slow path:
        long result = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            final byte b = readByte();
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new YsonError("Malformed varint");
    }

    private void throwUnexpectedEndOfStream() {
        throw new YsonError("Unexpected end of stream");
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean nextBuffer() {
        boolean read = underlying.next(tmp);
        if (read) {
            buffer = tmp.getBuffer();
            bufferOffset = tmp.getOffset();
            bufferLength = tmp.getLength();
        }
        return read;
    }

    public long readFixed64() {
        long bits = 0;
        if (bufferOffset + 8 <= bufferLength) {
            bits |= (buffer[bufferOffset++] & 0xFF);
            bits |= (long) (buffer[bufferOffset++] & 0xFF) << 8;
            bits |= (long) (buffer[bufferOffset++] & 0xFF) << 16;
            bits |= (long) (buffer[bufferOffset++] & 0xFF) << 24;
            bits |= (long) (buffer[bufferOffset++] & 0xFF) << 32;
            bits |= (long) (buffer[bufferOffset++] & 0xFF) << 40;
            bits |= (long) (buffer[bufferOffset++] & 0xFF) << 48;
            bits |= (long) (buffer[bufferOffset++] & 0xFF) << 56;
            position += 8;
        } else {
            for (int i = 0; i < 64; i += 8) {
                bits |= (long) (readByte() & 0xFF) << i;
            }
        }
        return bits;
    }
}
