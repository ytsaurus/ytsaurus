package tech.ytsaurus.client.rpc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import javax.annotation.Nullable;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

class NoneCodec extends Codec {
    static Codec instance = new NoneCodec();

    private NoneCodec() {
    }

    @Override
    public byte[] compress(byte[] src) {
        return src;
    }

    @Override
    public byte[] decompress(byte[] src) {
        return src;
    }
}

abstract class AbstractLZCodec<Compressor, Decompressor> extends Codec {

    /*
struct THeader
{
    static constexpr ui32 SignatureV1 = (1 << 30) + 1;
    static constexpr ui32 SignatureV2 = (1 << 30) + 2;

    ui32 Signature = static_cast<ui32>(-1);
    ui32 Size = 0;
};
     */
    private static final int THEADER_SIZE = 4 + 4;

    /*
struct TBlockHeader
{
    ui32 CompressedSize = 0;
    ui32 UncompressedSize = 0;
};

     */
    private static final int TBLOCK_HEADER_SIZE = 4 + 4;

    private static final int HEADERS_SIZE = THEADER_SIZE + TBLOCK_HEADER_SIZE;

    private static final int SIGNATURE_V1 = (1 << 30) + 1;

    private static final int MAX_BLOCK_SIZE = 1 << 30; // 1 073 741 824

    @Override
    public byte[] compress(byte[] src) {
        final int uncompressedSize = src.length;

        /*
        При сжатии attachment-ы должны нарезаться на блоки размера `MAX_BLOCK_SIZE`
        Но размер нашего attachment-а уже ограничен размером ChunkedWriter.MAX_CHUNK_SIZE = 256 * 1024 * 1024;
        Т.е. в нашем случае заголовок блока будет один и никогда не будет повторяться.

        Тем не менее - провериим этот факт
         */
        if (uncompressedSize > MAX_BLOCK_SIZE) {
            throw new IllegalArgumentException("unsupported block size: " + uncompressedSize +
                    " must be smaller than " + MAX_BLOCK_SIZE);
        }
        final Compressor compressor = getCompressor();
        final int compressedSizeBound = this.calcCompressedSizeBound(compressor, uncompressedSize);

        final byte[] dst = new byte[HEADERS_SIZE + compressedSizeBound];

        final int compressedSize = compress(compressor, src, 0, uncompressedSize, dst, HEADERS_SIZE);

        ByteBuffer.wrap(dst, 0, HEADERS_SIZE).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(SIGNATURE_V1) // THeader.SignatureV1
                .putInt(uncompressedSize) // THeader.Size
                .putInt(compressedSize) // TBlockHeader.CompressedSize (block)
                .putInt(uncompressedSize); // TBlockHeader.UncompressedSize (block)

        return Arrays.copyOf(dst, HEADERS_SIZE + compressedSize);
    }

    @Override
    public byte[] decompress(byte[] src) {
        // Операция, обратная compress-у
        // Поддерживаем только SignatureV1 и только один блок
        final int srcLength = src.length;

        if (srcLength == 0) {
            return src; // При отсутствии данных нам придет пустой массив, без заголовков
        }

        final ByteBuffer bb = ByteBuffer.wrap(src, 0, HEADERS_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        final int signature = bb.getInt();
        final int uncompressedSize = bb.getInt();

        if (uncompressedSize > MAX_BLOCK_SIZE) {
            throw new IllegalArgumentException("unsupported block size: " + uncompressedSize +
                    " must be smaller than " + MAX_BLOCK_SIZE);
        }

        final int blockCompressedSize = bb.getInt();
        final int blockUncompressedSize = bb.getInt();
        if (signature != SIGNATURE_V1) {
            throw new IllegalArgumentException("unsupported signature: " + signature + ", supports SignatureV1 only");
        }
        if (uncompressedSize != blockUncompressedSize) {
            throw new IllegalArgumentException("unsupported compression: only single block expected");
        }
        if (blockCompressedSize != (srcLength - HEADERS_SIZE)) {
            throw new IllegalArgumentException("unsupported compression: expect block size " +
                    (srcLength - HEADERS_SIZE) + ", received " + blockCompressedSize);
        }

        final Decompressor decompressor = getDecompressor();
        final byte[] output = new byte[uncompressedSize];
        final int compressedLen = decompress(decompressor, src, HEADERS_SIZE, output, 0, uncompressedSize);
        if (compressedLen != blockCompressedSize) {
            throw new IllegalArgumentException("broken stream: expect block size " + blockCompressedSize +
                    ", decompressed " + compressedLen);
        }

        return output;
    }

    //

    abstract Compressor getCompressor();

    abstract int calcCompressedSizeBound(Compressor compressor, int uncompressedSize);

    abstract int compress(Compressor compressor, byte[] src, int srcPos, int srcLength, byte[] dst, int dstPos);

    //

    abstract Decompressor getDecompressor();

    abstract int decompress(Decompressor decompressor, byte[] src, int srcPos, byte[] dst, int dstPos, int dstLen);
}

class Lz4Codec extends AbstractLZCodec<LZ4Compressor, LZ4FastDecompressor> {
    private final boolean hc;

    private final LZ4Factory factory = LZ4Factory.fastestInstance();

    Lz4Codec(boolean hc) {
        this.hc = hc;
    }

    @Override
    LZ4Compressor getCompressor() {
        return hc
                ? factory.highCompressor()
                : factory.fastCompressor();
    }

    @Override
    int calcCompressedSizeBound(LZ4Compressor compressor, int uncompressedSize) {
        return compressor.maxCompressedLength(uncompressedSize);
    }

    @Override
    int compress(LZ4Compressor compressor, byte[] src, int srcPos, int srcLength, byte[] dst, int dstPos) {
        return compressor.compress(src, srcPos, srcLength, dst, dstPos);
    }

    @Override
    LZ4FastDecompressor getDecompressor() {
        return factory.fastDecompressor();
    }

    @Override
    int decompress(LZ4FastDecompressor decompressor, byte[] src, int srcPos, byte[] dst, int dstPos, int dstLen) {
        return decompressor.decompress(src, srcPos, dst, dstPos, dstLen);
    }
}

class ZlibCodec extends Codec {
    private final int level;

    ZlibCodec(int level) {
        this.level = level;
    }

    @Override
    public byte[] compress(byte[] src) {
        DeflaterOutputStream encoder = null;
        try {
            int uncompressedSize = src.length;
            byte[] header = new byte[8];
            ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN).putLong(uncompressedSize);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            baos.write(header);
            encoder = new DeflaterOutputStream(baos, new Deflater(level));
            copy(new ByteArrayInputStream(src), encoder);
            encoder.flush();
            encoder.close();
            return baos.toByteArray();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            if (encoder != null) {
                closeQuietly(encoder);
            }
        }
    }

    @Override
    public byte[] decompress(byte[] src) {
        InflaterInputStream decoder = null;
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(src);
            byte[] header = new byte[8];
            int ret = bais.read(header);
            if (ret != 8) {
                throw new IllegalArgumentException(String.format("broken stream (read %d bytes)", ret));
            }
            long uncompressedSize = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN).getLong();
            decoder = new InflaterInputStream(bais);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            copy(decoder, baos);
            byte[] result = baos.toByteArray();

            if (result.length != uncompressedSize) {
                throw new IllegalArgumentException("broken stream");
            }

            return result;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            closeQuietly(decoder);
        }
    }

    private static long copy(InputStream inputStream, OutputStream outputStream) {
        try {
            byte[] bytes = new byte[0x10000];
            long totalCopied = 0;
            for (; ; ) {
                int count = inputStream.read(bytes);
                if (count < 0) {
                    return totalCopied;
                }

                outputStream.write(bytes, 0, count);
                totalCopied += count;
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void closeQuietly(@Nullable Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Throwable ex) {
                if (ex instanceof VirtualMachineError) {
                    // outer instanceof for performance (to not make 3 comparisons for most cases)
                    if (ex instanceof OutOfMemoryError || ex instanceof InternalError || ex instanceof UnknownError) {
                        throw (VirtualMachineError) ex;
                    }
                }
            }
        }
    }
}

public abstract class Codec {
    private static final Map<Compression, Supplier<Codec>> CODEC_BY_COMPRESSION = getAllCodecs();

    public abstract byte[] compress(byte[] src);

    public abstract byte[] decompress(byte[] src);

    private static Map<Compression, Supplier<Codec>> getAllCodecs() {
        Map<Compression, Supplier<Codec>> ret = new HashMap<>();
        ret.put(Compression.Zlib_1, () -> new ZlibCodec(1));
        ret.put(Compression.Zlib_2, () -> new ZlibCodec(2));
        ret.put(Compression.Zlib_3, () -> new ZlibCodec(3));
        ret.put(Compression.Zlib_4, () -> new ZlibCodec(4));
        ret.put(Compression.Zlib_5, () -> new ZlibCodec(5));
        ret.put(Compression.Zlib_6, () -> new ZlibCodec(6));
        ret.put(Compression.Zlib_7, () -> new ZlibCodec(7));
        ret.put(Compression.Zlib_8, () -> new ZlibCodec(8));
        ret.put(Compression.Zlib_9, () -> new ZlibCodec(9));

        ret.put(Compression.Lz4, () -> new Lz4Codec(false));
        ret.put(Compression.Lz4HighCompression, () -> new Lz4Codec(true));

        ret.put(Compression.None, () -> NoneCodec.instance);

        return ret;
    }

    public static Codec codecFor(Compression compression) {
        Supplier<Codec> codecSupplier = CODEC_BY_COMPRESSION.get(compression);
        if (codecSupplier == null) {
            throw new NoSuchElementException(String.format("cannot find codec for %s", compression));
        }
        return codecSupplier.get();
    }
}
