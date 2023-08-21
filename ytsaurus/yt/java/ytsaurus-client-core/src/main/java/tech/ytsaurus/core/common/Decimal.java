package tech.ytsaurus.core.common;

import java.math.BigInteger;
import java.util.Arrays;

import javax.annotation.Nullable;

import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class Decimal {
    private static final int MAX_PRECISION = 35;

    private static final byte[] PLUS_INF_4 = new byte[]{
            (byte) 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xfe,
    };

    private static final byte[] PLUS_INF_8 = new byte[]{
            (byte) 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff,
            (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xfe,
    };

    private static final byte[] PLUS_INF_16 = new byte[]{
            (byte) 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff,
            (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
            (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
            (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xfe,
    };

    private static final byte[] MINUS_INF_4 = new byte[]{
            (byte) 0x80, 0x00, 0x00, 0x02
    };

    private static final byte[] MINUS_INF_8 = new byte[]{
            (byte) 0x80, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x02
    };

    private static final byte[] MINUS_INF_16 = new byte[]{
            (byte) 0x80, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x02
    };

    private Decimal() {
    }

    public static byte[] textToBinary(String textDecimal, int precision, int scale) {
        validatePrecisionAndScale(precision, scale);
        int byteSize = getValueBinarySize(precision);

        byte[] binary = textToBinary(textDecimal, precision, scale, byteSize);

        if (binary.length > byteSize) {
            throw new RuntimeException(String.format(
                    "Binary size=%d, but expected no more than %d", binary.length, byteSize));
        }

        byte[] result = new byte[byteSize];
        System.arraycopy(binary, 0, result, byteSize - binary.length, binary.length);
        if (!textDecimal.isEmpty() && textDecimal.charAt(0) == '-') {
            for (int idx = 0; idx < byteSize - binary.length; ++idx) {
                result[idx] = -1;
            }
        }

        result[0] = (byte) (result[0] ^ (0x1 << 7));

        return result;
    }

    public static String binaryToText(byte[] binaryDecimal, int precision, int scale) {
        validatePrecisionAndScale(precision, scale);
        int byteSize = getValueBinarySize(precision);

        binaryDecimal[0] = (byte) (binaryDecimal[0] ^ (0x1 << 7));

        if (Arrays.equals(binaryDecimal, getPlusInf(byteSize))) {
            return "inf";
        } else if (Arrays.equals(binaryDecimal, getMinusInf(byteSize))) {
            return "-inf";
        } else if (Arrays.equals(binaryDecimal, getNan(byteSize))) {
            return "nan";
        }

        BigInteger decodedValue = new BigInteger(binaryDecimal);
        String digits = String.format("%0" + (scale + 1) + "d", decodedValue);
        StringBuilder result = new StringBuilder(digits.length() + 2);
        result.append(digits.subSequence(0, digits.length() - scale));

        if (scale > 0) {
            result.append('.');
            result.append(digits.subSequence(digits.length() - scale, digits.length()));
        }

        return result.toString();
    }

    private static byte[] textToBinary(String textDecimal, int precision, int scale, int byteSize) {
        if (textDecimal.isEmpty()) {
            throwInvalidDecimal(textDecimal, precision, scale, null);
        }

        int cur = 0;
        int end = textDecimal.length();

        boolean negative = false;
        switch (textDecimal.charAt(cur)) {
            case '-': {
                negative = true;
            }
            case '+': {
                ++cur;
                break;
            }
            default: {
                break;
            }
        }

        if (cur == end) {
            throwInvalidDecimal(textDecimal, precision, scale, null);
        }

        switch (textDecimal.charAt(cur)) {
            case 'i': {
            }
            case 'I': {
                if (cur + 3 == end) {
                    ++cur;
                    if (textDecimal.charAt(cur) == 'n' || textDecimal.charAt(cur) == 'N') {
                        ++cur;
                        if (textDecimal.charAt(cur) == 'f' || textDecimal.charAt(cur) == 'F') {
                            return negative ? getMinusInf(byteSize) : getPlusInf(byteSize);
                        }
                    }
                }
                throwInvalidDecimal(textDecimal, precision, scale, null);
            }
            case 'n': {
            }
            case 'N': {
                if (!negative && cur + 3 == end) {
                    ++cur;
                    if (textDecimal.charAt(cur) == 'a' || textDecimal.charAt(cur) == 'A') {
                        ++cur;
                        if (textDecimal.charAt(cur) == 'n' || textDecimal.charAt(cur) == 'N') {
                            return getNan(byteSize);
                        }
                    }
                }
                throwInvalidDecimal(textDecimal, precision, scale, null);
            }
            default: {
            }
        }

        BigInteger result = BigInteger.ZERO;

        int beforePoint = 0;
        int afterPoint = 0;

        for (; cur != end; ++cur) {
            if (textDecimal.charAt(cur) == '.') {
                ++cur;
                for (; cur != end; ++cur) {
                    int currentDigit = textDecimal.charAt(cur) - '0';
                    result = result.multiply(BigInteger.TEN);
                    result = result.add(BigInteger.valueOf(currentDigit));
                    ++afterPoint;
                    if (currentDigit < 0 || currentDigit > 9) {
                        throwInvalidDecimal(textDecimal, precision, scale, null);
                    }
                }
                break;
            }

            int currentDigit = textDecimal.charAt(cur) - '0';
            result = result.multiply(BigInteger.TEN);
            result = result.add(BigInteger.valueOf(currentDigit));
            ++beforePoint;
            if (currentDigit < 0 || currentDigit > 9) {
                throwInvalidDecimal(textDecimal, precision, scale, null);
            }
        }

        for (; afterPoint < scale; ++afterPoint) {
            result = result.multiply(BigInteger.TEN);
        }

        if (afterPoint > scale) {
            throwInvalidDecimal(textDecimal, precision, scale, "too many digits after decimal point");
        }

        if (beforePoint + scale > precision) {
            throwInvalidDecimal(textDecimal, precision, scale, "too many digits before decimal point");
        }

        return (negative ? result.negate() : result).toByteArray();
    }

    private static byte[] getPlusInf(int byteSize) {
        switch (byteSize) {
            case 4: {
                return PLUS_INF_4;
            }
            case 8: {
                return PLUS_INF_8;
            }
            case 16: {
                return PLUS_INF_16;
            }
            default: {
                throw new IllegalArgumentException("Incorrect byteSize in getPlusInf");
            }
        }
    }

    private static byte[] getMinusInf(int byteSize) {
        switch (byteSize) {
            case 4: {
                return MINUS_INF_4;
            }
            case 8: {
                return MINUS_INF_8;
            }
            case 16: {
                return MINUS_INF_16;
            }
            default: {
                throw new IllegalArgumentException("Incorrect byteSize in getMinusInf");
            }
        }
    }

    private static byte[] getNan(int byteSize) {
        byte[] result = getPlusInf(byteSize).clone();
        result[result.length - 1] = (byte) 0xFF;
        return result;
    }

    private static void throwInvalidDecimal(String textDecimal, int precision, int scale, @Nullable String reason) {
        if (reason == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "String %s is not valid Decimal<%d,%d> representation",
                            textDecimal, precision, scale
                    )
            );
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "String %s is not valid Decimal<%d,%d> representation: %s",
                            textDecimal, precision, scale, reason
                    )
            );
        }
    }

    private static void validatePrecisionAndScale(int precision, int scale) {
        if (precision <= 0 || precision > MAX_PRECISION) {
            throw new IllegalArgumentException(String.format(
                    "Invalid decimal precision %d, precision must be in range [1, %d]",
                    precision, MAX_PRECISION));
        }

        if (scale < 0 || scale > precision) {
            throw new IllegalArgumentException(String.format(
                    "Invalid decimal scale %d (precision: %d); decimal scale must be in range [0, PRECISION]",
                    scale, precision));
        }
    }

    private static int getValueBinarySize(int precision) {
        if (precision > 0) {
            if (precision <= 9) {
                return 4;
            } else if (precision <= 18) {
                return 8;
            } else if (precision <= 35) {
                return 16;
            }
        }

        validatePrecisionAndScale(precision, 0);

        throw new RuntimeException("Shouldn't be here");
    }
}
