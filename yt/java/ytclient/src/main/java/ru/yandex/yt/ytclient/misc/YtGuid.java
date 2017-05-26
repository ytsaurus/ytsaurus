package ru.yandex.yt.ytclient.misc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.netty.buffer.ByteBuf;

import ru.yandex.yt.TGuid;
import ru.yandex.yt.TGuidOrBuilder;

/**
 * Примерно соответствует TGuid в исходниках yt
 * <p>
 * Основано на реализации yt/19_2/yt/core/misc/guid.cpp
 */
public final class YtGuid {
    public static final YtGuid EMPTY = new YtGuid(0, 0);

    // Regexp для парсинга текстового представления guid'ов
    private static final Pattern PATTERN =
            Pattern.compile("([0-9A-Fa-f]+)-([0-9A-Fa-f]+)-([0-9A-Fa-f]+)-([0-9A-Fa-f]+)");

    // Общий счётчик для улучшения гарантий на уникальность созданного guid'а в пределах процесса
    private static final AtomicInteger COUNTER = new AtomicInteger();

    private final long first;
    private final long second;

    public YtGuid(long first, long second) {
        this.first = first;
        this.second = second;
    }

    public long getFirst() {
        return first;
    }

    public long getSecond() {
        return second;
    }

    public boolean isEmpty() {
        return first == 0 && second == 0;
    }

    /**
     * Создаёт случайный guid
     */
    public static YtGuid create() {
        return create(ThreadLocalRandom.current());
    }

    /**
     * Создаёт случайный guid используя random
     */
    public static YtGuid create(Random random) {
        long first = random.nextLong();
        long second = ((long) COUNTER.incrementAndGet() << 32) | random.nextInt() & 0xffffffffL;
        return new YtGuid(first, second);
    }

    /**
     * Восстанавливает guid из содержимого массива data
     */
    public static YtGuid fromBytes(byte[] data) {
        if (data.length != 16) {
            throw new IllegalArgumentException("Array must be 16 bytes long, not " + data.length);
        }
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        long first = buf.getLong();
        long second = buf.getLong();
        return new YtGuid(first, second);
    }

    /**
     * Возвращает содержимое guid в виде массива байт
     */
    public byte[] getBytes() {
        ByteBuffer buf = ByteBuffer.wrap(new byte[16]).order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(first);
        buf.putLong(second);
        return buf.array();
    }

    /**
     * Читает guid из in
     */
    public static YtGuid readFrom(ByteBuf in) {
        if (in.readableBytes() < 16) {
            throw new IllegalArgumentException("At least 16 bytes must be readable");
        }
        in = in.order(ByteOrder.LITTLE_ENDIAN);
        long first = in.readLong();
        long second = in.readLong();
        return new YtGuid(first, second);
    }

    public void writeTo(ByteBuf out) {
        out = out.order(ByteOrder.LITTLE_ENDIAN);
        out.writeLong(first);
        out.writeLong(second);
    }

    public static YtGuid fromString(String s) {
        Matcher m = PATTERN.matcher(s);
        if (!m.matches()) {
            throw new IllegalArgumentException("String '" + s + "' is not a valid yt guid");
        }
        int a = Integer.parseUnsignedInt(m.group(1), 16);
        int b = Integer.parseUnsignedInt(m.group(2), 16);
        int c = Integer.parseUnsignedInt(m.group(3), 16);
        int d = Integer.parseUnsignedInt(m.group(4), 16);
        return new YtGuid(
                (long) c << 32 | d & 0xffffffffL,
                (long) a << 32 | b & 0xffffffffL);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        YtGuid ytGuid = (YtGuid) o;

        if (first != ytGuid.first) {
            return false;
        }
        return second == ytGuid.second;
    }

    @Override
    public int hashCode() {
        int result = (int) (first ^ (first >>> 32));
        result = 31 * result + (int) (second ^ (second >>> 32));
        return result;
    }

    @Override
    public String toString() {
        // Почему-то yt представляет значение как большое 128 битное число с дефисами
        return Integer.toUnsignedString((int) (second >>> 32), 16) +
                '-' +
                Integer.toUnsignedString((int) second, 16) +
                '-' +
                Integer.toUnsignedString((int) (first >>> 32), 16) +
                '-' +
                Integer.toUnsignedString((int) first, 16);
    }

    public static YtGuid fromProto(TGuidOrBuilder guid) {
        return new YtGuid(guid.getFirst(), guid.getSecond());
    }

    public TGuid toProto() {
        return TGuid.newBuilder().setFirst(first).setSecond(second).build();
    }
}
