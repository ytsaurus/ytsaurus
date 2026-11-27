package tech.ytsaurus.client.request;

import java.util.Arrays;

import com.google.protobuf.ByteString;
import tech.ytsaurus.THyperLogLogDigest;

/**
 * The precision-4 HyperLogLog digest used in YTsaurus columnar statistics.
 *
 * <p>Each digest estimates distinct non-null values. Instances are immutable and can be merged
 * before estimating the cardinality of a union. Estimates are approximate and may exceed the
 * actual number of values. The register layout and bias correction match YTsaurus THyperLogLog.
 */
public final class HyperLogLogDigest {
    // TColumnarHyperLogLogDigest fixes the precision in yt/client/table_client/columnar_statistics.h:35.
    private static final int PRECISION = 4;
    // THyperLogLog::RegisterCount in yt/core/misc/hyperloglog.h:44.
    private static final int REGISTER_COUNT = 1 << PRECISION;

    // Precision-4 entry of Thresholds in yt/core/misc/hyperloglog_bias.h:79.
    private static final int LINEAR_COUNTING_THRESHOLD = 10;
    // Precision-4 row of RawEstimates in yt/core/misc/hyperloglog_bias.h:13.
    private static final double[] RAW_ESTIMATES = {
        11, 11.717, 12.207, 12.7896,
        13.2882, 13.8204, 14.3772, 14.9342,
        15.5202, 16.161, 16.7722, 17.4636,
        18.0396, 18.6766, 19.3566, 20.0454,
        20.7936, 21.4856, 22.2666, 22.9946,
        23.766, 24.4692, 25.3638, 26.0764,
        26.7864, 27.7602, 28.4814, 29.433,
        30.2926, 31.0664, 31.9996, 32.7956,
        33.5366, 34.5894, 35.5738, 36.2698,
        37.3682, 38.0544, 39.2342, 40.0108,
        40.7966, 41.9298, 42.8704, 43.6358,
        44.5194, 45.773, 46.6772, 47.6174,
        48.4888, 49.3304, 50.2506, 51.4996,
        52.3824, 53.3078, 54.3984, 55.5838,
        56.6618, 57.2174, 58.3514, 59.0802,
        60.1482, 61.0376, 62.3598, 62.8078,
        63.9744, 64.914, 65.781, 67.1806,
        68.0594, 68.8446, 69.7928, 70.8248,
        71.8324, 72.8598, 73.6246, 74.7014,
        75.393, 76.6708, 77.2394
    };
    // Precision-4 row of BiasData in yt/core/misc/hyperloglog_bias.h:46.
    private static final double[] BIAS_DATA = {
        10, 9.717, 9.207, 8.7896,
        8.2882, 7.8204, 7.3772, 6.9342,
        6.5202, 6.161, 5.7722, 5.4636,
        5.0396, 4.6766, 4.3566, 4.0454,
        3.7936, 3.4856, 3.2666, 2.9946,
        2.766, 2.4692, 2.3638, 2.0764,
        1.7864, 1.7602, 1.4814, 1.433,
        1.2926, 1.0664, 0.999600000000001, 0.7956,
        0.5366, 0.589399999999998, 0.573799999999999, 0.269799999999996,
        0.368200000000002, 0.0544000000000011, 0.234200000000001, 0.0108000000000033,
        -0.203400000000002, -0.0701999999999998, -0.129600000000003, -0.364199999999997,
        -0.480600000000003, -0.226999999999997, -0.322800000000001, -0.382599999999996,
        -0.511200000000002, -0.669600000000003, -0.749400000000001, -0.500399999999999,
        -0.617600000000003, -0.6922, -0.601599999999998, -0.416200000000003,
        -0.338200000000001, -0.782600000000002, -0.648600000000002, -0.919800000000002,
        -0.851799999999997, -0.962400000000002, -0.6402, -1.1922,
        -1.0256, -1.086, -1.21899999999999, -0.819400000000002,
        -0.940600000000003, -1.1554, -1.2072, -1.1752,
        -1.16759999999999, -1.14019999999999, -1.3754, -1.29859999999999,
        -1.607, -1.3292, -1.7606
    };

    private final ByteString data;

    HyperLogLogDigest(THyperLogLogDigest proto) {
        this(proto.getData());
        if (data.size() != REGISTER_COUNT) {
            throw new IllegalArgumentException(
                    "Expected " + REGISTER_COUNT + " HyperLogLog registers, got " + data.size());
        }
    }

    private HyperLogLogDigest(ByteString data) {
        this.data = data;
    }

    public byte[] getData() {
        return data.toByteArray();
    }

    private int registerAt(int index) {
        return Byte.toUnsignedInt(data.byteAt(index));
    }

    /**
     * Returns the union of two compatible digests without changing either input.
     * Both digests must describe values with the same YTsaurus type and hash semantics.
     */
    public HyperLogLogDigest merge(HyperLogLogDigest other) {
        byte[] result = new byte[REGISTER_COUNT];
        for (int i = 0; i < REGISTER_COUNT; i++) {
            result[i] = (byte) Math.max(registerAt(i), other.registerAt(i));
        }
        return new HyperLogLogDigest(ByteString.copyFrom(result));
    }

    /**
     * Estimates the cardinality using the same bias correction as YTsaurus.
     *
     * @throws ArithmeticException if the unsigned estimate does not fit in a Java long
     */
    public long estimateCardinality() {
        int zeroRegisters = 0;
        double sum = 0;
        for (int i = 0; i < REGISTER_COUNT; i++) {
            int register = registerAt(i);
            if (register == 0) {
                zeroRegisters++;
            } else {
                sum += Math.scalb(1.0, -register);
            }
        }
        sum += zeroRegisters;
        double m = REGISTER_COUNT;
        double alpha = 0.7213 / (1.0 + 1.079 / m);
        double raw = (1.0 / sum) * m * m * alpha;
        if (raw < 5 * m) {
            raw -= estimateBias(raw);
        }
        double smallCardinality = zeroRegisters == 0 ? raw : m * Math.log(m / zeroRegisters);
        double estimate = smallCardinality <= LINEAR_COUNTING_THRESHOLD ? smallCardinality : raw;
        // C++ returns ui64. A Java long cast would silently clamp estimates of 2^63 or more to Long.MAX_VALUE.
        if (estimate >= 0x1.0p63) {
            throw new ArithmeticException("HyperLogLog estimate exceeds signed long range");
        }
        return (long) estimate;
    }

    private static double estimateBias(double raw) {
        int index = Arrays.binarySearch(RAW_ESTIMATES, raw);
        if (index >= 0) {
            return BIAS_DATA[index];
        }
        // Arrays.binarySearch returns -(insertion point) - 1 when there is no exact match.
        index = -index - 1;
        if (index == 0) {
            return BIAS_DATA[0];
        }
        if (index == RAW_ESTIMATES.length) {
            return BIAS_DATA[BIAS_DATA.length - 1];
        }
        double w1 = raw - RAW_ESTIMATES[index - 1];
        double w2 = RAW_ESTIMATES[index] - raw;
        return (BIAS_DATA[index - 1] * w1 + BIAS_DATA[index] * w2) / (w1 + w2);
    }
}
