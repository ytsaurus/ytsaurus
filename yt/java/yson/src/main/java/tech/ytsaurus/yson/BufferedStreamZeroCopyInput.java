package tech.ytsaurus.yson;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

public class BufferedStreamZeroCopyInput implements ZeroCopyInput {
    final InputStream underlying;
    final byte[] buffer;

    public BufferedStreamZeroCopyInput(InputStream input, int bufferSize) {
        this(input, new byte[bufferSize]);
    }

    BufferedStreamZeroCopyInput(InputStream input, byte[] buffer) {
        if (buffer.length == 0) {
            throw new IllegalArgumentException("Buffer must be nonempty");
        }
        underlying = input;
        this.buffer = buffer;
    }

    @Override
    public boolean next(BufferReference out) {
        try {
            int totalRead = 0;
            while (totalRead < buffer.length) {
                int read = underlying.read(buffer, totalRead, buffer.length - totalRead);
                if (read == -1) {
                    break;
                }
                totalRead += read;
            }
            if (totalRead == 0) {
                return false;
            }
            out.setBuffer(buffer);
            out.setOffset(0);
            out.setLength(totalRead);
            return true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
