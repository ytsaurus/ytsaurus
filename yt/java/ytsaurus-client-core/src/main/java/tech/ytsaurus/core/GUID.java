package tech.ytsaurus.core;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

/**
 * @author sankear
 *
 * см yt/19_2/yt/core/misc/guid.cpp
 */
public class GUID {
    // Regexp для парсинга текстового представления guid'ов
    private static final Pattern PATTERN =
            Pattern.compile("([0-9A-Fa-f]+)-([0-9A-Fa-f]+)-([0-9A-Fa-f]+)-([0-9A-Fa-f]+)");

    // Общий счётчик для улучшения гарантий на уникальность созданного guid'а в пределах процесса
    private static final AtomicInteger COUNTER = new AtomicInteger();

    private final long first;
    private final long second;

    public GUID(long first, long second) {
        this.first = first;
        this.second = second;
    }

    public GUID(long a, long b, long c, long d) {
        this.first = c << 32 | d & 0xffffffffL;
        this.second = a << 32 | b & 0xffffffffL;
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

    public static boolean isEmpty(@Nullable GUID id) {
        return id == null || id.isEmpty();
    }

    /**
     * Создаёт случайный guid
     */
    public static GUID create() {
        return create(ThreadLocalRandom.current());
    }

    /**
     * Создаёт случайный guid используя random
     */
    public static GUID create(Random random) {
        long first = random.nextLong();
        long second = ((long) COUNTER.incrementAndGet() << 32) | random.nextInt() & 0xffffffffL;
        return new GUID(first, second);
    }

    @Override
    public boolean equals(Object another) {
        if (this == another) {
            return true;
        }
        if (!(another instanceof GUID)) {
            return false;
        }
        GUID guid = (GUID) another;
        return first == guid.first && second == guid.second;
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

    public static boolean isValid(String s) {
        try {
            valueOf(s);
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }

    public static GUID valueOf(String s) {
        return valueOfO(s)
                .orElseThrow(() -> new IllegalArgumentException(s));
    }

    public static Optional<GUID> valueOfO(String s) {
        Matcher m = PATTERN.matcher(s);
        if (!m.matches()) {
            throw new IllegalArgumentException("String '" + s + "' is not a valid yt guid");
        }
        int a = Integer.parseUnsignedInt(m.group(1), 16);
        int b = Integer.parseUnsignedInt(m.group(2), 16);
        int c = Integer.parseUnsignedInt(m.group(3), 16);
        int d = Integer.parseUnsignedInt(m.group(4), 16);
        if (a == 0 && b == 0 && c == 0 && d == 0) {
            return Optional.empty();
        }
        return Optional.of(new GUID(a, b, c, d));
    }
}
