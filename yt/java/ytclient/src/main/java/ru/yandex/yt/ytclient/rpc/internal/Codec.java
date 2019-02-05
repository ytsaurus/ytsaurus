package ru.yandex.yt.ytclient.rpc.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.bolts.collection.MapF;
import ru.yandex.misc.ExceptionUtils;
import ru.yandex.misc.io.IoUtils;

class NoneCodec extends Codec {
    public static Codec instance = new NoneCodec();

    private NoneCodec() { }

    @Override
    public byte[] compress(byte[] src) {
        return src;
    }

    @Override
    public byte[] decompress(byte[] src) {
        return src;
    }
}

class ZlibCodec extends Codec {
    private final int level;

    public ZlibCodec(int level) {
        this.level = level;
    }

    @Override
    public byte [] compress(byte[] src) {
        DeflaterOutputStream encoder = null;
        try {
            int uncompressedSize = src.length;
            byte [] header = new byte[8];
            ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN).putLong(uncompressedSize);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            baos.write(header);
            encoder = new DeflaterOutputStream(baos, new Deflater(level));
            IoUtils.copy(new ByteArrayInputStream(src), encoder);
            encoder.flush();
            encoder.close();
            return baos.toByteArray();
        } catch (Exception e) {
            throw  ExceptionUtils.translate(e);
        } finally {
            if (encoder != null) {
                IoUtils.closeQuietly(encoder);
            }
        }
    }

    @Override
    public byte[] decompress(byte[] src) {
        InflaterInputStream decoder = null;
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(src);
            byte [] header = new byte[8];
            int ret = bais.read(header);
            if (ret != 8) {
                throw new IllegalArgumentException(String.format("broken stream (read %d bytes)", ret));
            }
            long uncompressedSize = ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN).getLong();
            decoder = new InflaterInputStream(bais);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            IoUtils.copy(decoder, baos);
            byte[] result = baos.toByteArray();

            if (result.length != uncompressedSize) {
                throw new IllegalArgumentException("broken stream");
            }

            return result;
        } catch (Exception e) {
            throw  ExceptionUtils.translate(e);
        } finally {
            if (decoder != null) {
                IoUtils.closeQuietly(decoder);
            }
        }
    }
}

public abstract class Codec {
    abstract public byte[] compress(byte[] src);
    abstract public byte[] decompress(byte[] src);

    private static MapF<Compression, Supplier<Codec>> getAllCodecs() {

        MapF<Compression, Supplier<Codec>> ret = Cf.hashMap();
        ret.put(Compression.Zlib_1, () -> new ZlibCodec(1));
        ret.put(Compression.Zlib_2, () -> new ZlibCodec(2));
        ret.put(Compression.Zlib_3, () -> new ZlibCodec(3));
        ret.put(Compression.Zlib_4, () -> new ZlibCodec(4));
        ret.put(Compression.Zlib_5, () -> new ZlibCodec(5));
        ret.put(Compression.Zlib_6, () -> new ZlibCodec(6));
        ret.put(Compression.Zlib_7, () -> new ZlibCodec(7));
        ret.put(Compression.Zlib_8, () -> new ZlibCodec(8));
        ret.put(Compression.Zlib_9, () -> new ZlibCodec(9));

        ret.put(Compression.None, () -> NoneCodec.instance);

        return ret;
    }

    private static final MapF<Compression, Supplier<Codec>> CODEC_BY_COMPRESSION = getAllCodecs();

    public static Codec codecFor(Compression compression) {
        return CODEC_BY_COMPRESSION.getOrThrow(compression, String.format("cannot find codec for %s", compression)).get();
    }
}
