// Copied from arcadia //classifieds/vs/lib/arcadia-proto/src/main/java/at/orz/hash/CityHash.java

/*
 * Copyright (C) 2012 tamtam180
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.yandex.spark.yt.common.utils;

public class CityHashImpl {

    private static final long k0 = 0xc3a5c85c97cb3127L;
    private static final long k1 = 0xb492b66fbe98f273L;
    private static final long k2 = 0x9ae16a3b2f90404fL;
    private static final long k3 = 0xc949d7c7509e6557L;

    private static long toLongLE(byte[] b, int i) {
        return (((long)b[i+7] << 56) +
                ((long)(b[i+6] & 255) << 48) +
                ((long)(b[i+5] & 255) << 40) +
                ((long)(b[i+4] & 255) << 32) +
                ((long)(b[i+3] & 255) << 24) +
                ((b[i+2] & 255) << 16) +
                ((b[i+1] & 255) <<  8) +
                ((b[i+0] & 255) <<  0));
    }
    private  static int toIntLE(byte[] b, int i) {
        return (((b[i+3] & 255) << 24) + ((b[i+2] & 255) << 16) + ((b[i+1] & 255) << 8) + ((b[i+0] & 255) << 0));
    }

    private static long fetch64(byte[] s, int pos) {
        return toLongLE(s, pos);
    }

    private static int fetch32(byte[] s, int pos) {
        return toIntLE(s, pos);
    }

    private static long rotate(long val, int shift) {
        return shift == 0 ? val : (val >>> shift) | (val << (64 - shift));
    }

    private static long rotateByAtLeast1(long val, int shift) {
        return (val >>> shift) | (val << (64 - shift));
    }

    private static long shiftMix(long val) {
        return val ^ (val >>> 47);
    }

    private static final long kMul = 0x9ddfea08eb382d69L;
    private static long hash128to64(long u, long v) {
        long a = (u ^ v) * kMul;
        a ^= (a >>> 47);
        long b = (v ^ a) * kMul;
        b ^= (b >>> 47);
        b *= kMul;
        return b;
    }

    private static long hashLen16(long u, long v) {
        return hash128to64(u, v);
    }

    private static long hashLen0to16(byte[] s, int pos, int len) {
        if (len > 8) {
            long a = fetch64(s, pos + 0);
            long b = fetch64(s, pos + len - 8);
            return hashLen16(a, rotateByAtLeast1(b + len, len)) ^ b;
        }
        if (len >= 4) {
            long a = 0xffffffffL & fetch32(s, pos + 0);
            return hashLen16((a << 3) + len, 0xffffffffL & fetch32(s, pos + len - 4));
        }
        if (len > 0) {
            int a = s[pos + 0] & 0xFF;
            int b = s[pos + (len >>> 1)] & 0xFF;
            int c = s[pos + len - 1] & 0xFF;
            int y = a + (b << 8);
            int z = len + (c << 2);
            return shiftMix(y * k2 ^ z * k3) * k2;
        }
        return k2;
    }

    private static long hashLen17to32(byte[] s, int pos, int len) {
        long a = fetch64(s, pos + 0) * k1;
        long b = fetch64(s, pos + 8);
        long c = fetch64(s, pos + len - 8) * k2;
        long d = fetch64(s, pos + len - 16) * k0;
        return hashLen16(
                rotate(a - b, 43) + rotate(c, 30) + d,
                a + rotate(b ^ k3, 20) - c + len
        );
    }

    private static long[] weakHashLen32WithSeeds(
            long w, long x, long y, long z,
            long a, long b) {

        a += w;
        b = rotate(b + a + z, 21);
        long c = a;
        a += x;
        a += y;
        b += rotate(a, 44);
        return new long[]{ a + z, b + c };
    }

    private static long[] weakHashLen32WithSeeds(byte[] s, int pos, long a, long b) {
        return weakHashLen32WithSeeds(
                fetch64(s, pos + 0),
                fetch64(s, pos + 8),
                fetch64(s, pos + 16),
                fetch64(s, pos + 24),
                a,
                b
        );
    }

    private static long hashLen33to64(byte[] s, int pos, int len) {

        long z = fetch64(s, pos + 24);
        long a = fetch64(s, pos + 0) + (fetch64(s, pos + len - 16) + len) * k0;
        long b = rotate(a + z, 52);
        long c = rotate(a, 37);

        a += fetch64(s, pos + 8);
        c += rotate(a, 7);
        a += fetch64(s, pos + 16);

        long vf = a + z;
        long vs = b + rotate(a, 31) + c;

        a = fetch64(s, pos + 16) + fetch64(s, pos + len - 32);
        z = fetch64(s, pos + len - 8);
        b = rotate(a + z, 52);
        c = rotate(a, 37);
        a += fetch64(s, pos + len - 24);
        c += rotate(a, 7);
        a += fetch64(s, pos + len -16);

        long wf = a + z;
        long ws = b + rotate(a, 31) + c;
        long r = shiftMix((vf + ws) * k2 + (wf + vs) * k0);

        return shiftMix(r * k0 + vs) * k2;

    }

    // https://a.yandex-team.ru/arc/trunk/arcadia/mail/util/cityhash/main/java/at/orz/hash/CityHash.java?rev=r7630047#L208
    public static long yandexCityHash64(byte[] s, int pos, int len) {

        if (len <= 32) {
            if (len <= 16) {
                return hashLen0to16(s, pos, len);
            } else {
                return hashLen17to32(s, pos, len);
            }
        } else if (len <= 64) {
            return hashLen33to64(s, pos, len);
        }

        long x = fetch64(s, pos);
        long y = fetch64(s, pos + len - 16) ^ k1;
        long z = fetch64(s, pos + len - 56) ^ k0;

        long [] v = weakHashLen32WithSeeds(s, pos + len - 64, len, y);
        long [] w = weakHashLen32WithSeeds(s, pos + len - 32, len * k1, k0);
        z += shiftMix(v[1]) * k1;
        x = rotate(z + x, 39) * k1;
        y = rotate(y, 33) * k1;

        len = (len - 1) & ~63;
        do {
            x = rotate(x + y + v[0] + fetch64(s, pos + 16), 37) * k1;
            y = rotate(y + v[1] + fetch64(s, pos + 48), 42) * k1;
            x ^= w[1];
            y ^= v[0];
            z = rotate(z ^ w[0], 33);
            v = weakHashLen32WithSeeds(s, pos + 0, v[1] * k1, x + w[0]);
            w = weakHashLen32WithSeeds(s, pos + 32, z + w[1], y);
            { long swap = z; z = x; x = swap; }
            pos += 64;
            len -= 64;
        } while (len != 0);

        return hashLen16(
                hashLen16(v[0], w[0]) + shiftMix(y) * k1 + z,
                hashLen16(v[1], w[1]) + x
        );

    }
}