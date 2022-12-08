package tech.ytsaurus.core.common;

import java.math.BigInteger;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

public class DecimalTest {
    @Test
    public void testTextBinaryConversion() {
        BigInteger v = new BigInteger(new byte[]{
                (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff,
        });

        check(3, 2, "0.00", new byte[]{(byte) 0x80, 0x00, 0x00, 0x00});
        check(3, 2, "0.02", new byte[]{(byte) 0x80, 0x00, 0x00, 0x02});
        check(3, 2, "0.10", new byte[]{(byte) 0x80, 0x00, 0x00, 0x0A});
        check(3, 2, "3.14", new byte[]{(byte) 0x80, 0x00, 0x01, 0x3A});
        check(10, 2, "3.14", new byte[]{(byte) 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x3A});
        check(35, 2, "3.14", new byte[]{(byte) 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x3A});
        check(10, 2, "13.31", new byte[]{(byte) 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x33});
        check(10, 9, "-2.718281828",  new byte[]{0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                0x5D, (byte) 0xFA, 0x4F, (byte) 0x9C});

        check(35, 9, "-2.718281828", new byte[]{0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x5D, (byte) 0xFA, 0x4F, (byte) 0x9C});

        check(3, 2, "nan", new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
        check(3, 2, "inf", new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFE});
        check(3, 2, "-inf", new byte[]{0x00, 0x00, 0x00, 0x02});

        Assert.assertArrayEquals(Decimal.textToBinary("+inf", 3, 2),
                new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFE});

        check(10, 2, "nan", new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
        });

        check(10, 2, "inf", new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFE,
        });

        check(10, 2, "-inf", new byte[]{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02});

        Assert.assertArrayEquals(Decimal.textToBinary("+inf", 10, 2),
                new byte[]{
                        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFE
                });

        check(35, 2, "nan", new byte[]{
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
        });

        check(35, 2, "inf", new byte[]{
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFE,
        });

        check(35, 2, "-inf", new byte[]{
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02
        });

        Assert.assertArrayEquals(Decimal.textToBinary("+inf", 35, 2),
                new byte[]{
                        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
                        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFE
                });

        checkErrorTextToBinary(3, 2, "-nan", "is not valid Decimal");
        checkErrorTextToBinary(3, 2, "infinity", "is not valid Decimal");
        checkErrorTextToBinary(3, 2, "-infinity", "is not valid Decimal");

        check(35, 0, "-60227680402501652580863193008687593", new byte[]{
                0x7F, (byte) 0xF4, 0x66, (byte) 0x8B, (byte) 0xCC, (byte) 0xE0, 0x02, (byte) 0xBD,
                0x68, 0x5B, 0x3A, 0x38, 0x11, (byte) 0xCE, 0x36, 0x17,
        });

        check(35, 0, "-58685702202126332296617139656872032", new byte[]{
                0x7F, (byte) 0xF4, (byte) 0xB2, (byte) 0x92, 0x4D, 0x28, 0x57, 0x2F,
                (byte) 0xF1, (byte) 0x95, 0x25, 0x51, 0x52, 0x05, (byte) 0xAF, (byte) 0xA0,
        });

        check(35, 0, "29836394225258329500167403959807652", new byte[]{
                (byte) 0x80, 0x05, (byte) 0xBF, 0x0C, 0x3D, 0x43, (byte) 0x9F, 0x6F,
                (byte) 0xFD, 0x64, (byte) 0x9D, (byte) 0x99, (byte) 0xA7, 0x04, (byte) 0xEE, (byte) 0xA4,
        });

        check(35, 0, "61449825198266175750309883089040771", new byte[]{
                (byte) 0x80, 0x0B, (byte) 0xD5, (byte) 0xB5, (byte) 0xD5, (byte) 0xF0, (byte) 0xC7, 0x3E,
                0x0C, (byte) 0x9C, (byte) 0xD4, (byte) 0x94, 0x32, (byte) 0x98, (byte) 0xB5, (byte) 0x83
        });
    }

    @Test
    public void testPrecisionScaleLimits() {
        checkErrorTextToBinary(-1, 0, "0", "Invalid decimal precision");
        checkErrorTextToBinary(0, 0, "0", "Invalid decimal precision");
        checkErrorTextToBinary(36, 0, "0", "Invalid decimal precision");

        checkErrorBinaryToText(-1, 0, new byte[]{0x00}, "Invalid decimal precision");
        checkErrorBinaryToText(0, 0, new byte[]{0x00}, "Invalid decimal precision");
        checkErrorBinaryToText(36, 0, new byte[]{0x00}, "Invalid decimal precision");

        checkRoundConvert(1, 0, "0");
        checkRoundConvert(35, 0, "0");

        checkRoundConvert(3, 2, "-3.14");

        checkErrorBinaryToText(3, 4, new byte[]{0x00}, "Invalid decimal scale");

        checkErrorTextToBinary(10, 3, "3.1415", "too many digits after decimal point");
        checkErrorTextToBinary(10, 3, "-3.1415", "too many digits after decimal point");

        checkRoundConvert(10, 3, "3.14", "3.140");
        checkRoundConvert(10, 3, "-3.14", "-3.140");

        checkErrorTextToBinary(5, 3, "314.15", "too many digits before decimal point");
        checkErrorTextToBinary(5, 3, "-314.15", "too many digits before decimal point");
    }

    private String getTextNines(int precision) {
        return String.join("", Collections.nCopies(precision, "9"));
    }

    private String getTextMinusNines(int precision) {
        return "-" + getTextNines(precision);
    }

    private String getTextZillion(int precision) {
        return "1" + String.join("", Collections.nCopies(precision, "0"));
    }

    private String getTextMinusZillion(int precision) {
        return "-" + getTextZillion(precision);
    }

    @Test
    public void testTextLimits() {
        for (int precision = 1; precision <= 35; ++precision) {
            checkRoundConvert(precision, 0, "0");
            checkRoundConvert(precision, 0, "1");
            checkRoundConvert(precision, 0, "-1");
            checkRoundConvert(precision, 0, "2");
            checkRoundConvert(precision, 0, "-2");
            checkRoundConvert(precision, 0, "3");
            checkRoundConvert(precision, 0, "-3");
            checkRoundConvert(precision, 0, "4");
            checkRoundConvert(precision, 0, "-4");
            checkRoundConvert(precision, 0, "5");
            checkRoundConvert(precision, 0, "-5");
            checkRoundConvert(precision, 0, "6");
            checkRoundConvert(precision, 0, "-6");
            checkRoundConvert(precision, 0, "7");
            checkRoundConvert(precision, 0, "-7");
            checkRoundConvert(precision, 0, "8");
            checkRoundConvert(precision, 0, "-8");
            checkRoundConvert(precision, 0, "9");
            checkRoundConvert(precision, 0, "-9");
            checkRoundConvert(precision, 0, "inf");
            checkRoundConvert(precision, 0, "-inf");
            checkRoundConvert(precision, 0, "nan");

            checkRoundConvert(precision, 0, getTextNines(precision));
            checkRoundConvert(precision, 0, getTextMinusNines(precision));

            checkErrorTextToBinary(precision, 0, getTextZillion(precision), "too many digits before decimal point");
            checkErrorTextToBinary(precision, 0, getTextMinusZillion(precision),
                    "too many digits before decimal point");
        }
    }

    private void check(int precision, int scale, String text, byte[] binary) {
        Assert.assertArrayEquals(Decimal.textToBinary(text, precision, scale), binary);
        Assert.assertEquals(Decimal.binaryToText(binary, precision, scale), text);
    }

    private void checkRoundConvert(int precision, int scale, String text) {
        checkRoundConvert(precision, scale, text, text);
    }

    private void checkRoundConvert(int precision, int scale, String text, String expectedText) {
        String roundText = Decimal.binaryToText(Decimal.textToBinary(text, precision, scale), precision, scale);
        Assert.assertEquals("Same expected", expectedText, roundText);
    }

    private void checkErrorTextToBinary(int precision, int scale, String text, String expectedMessage) {
        String gotMessage = null;
        try {
            Decimal.textToBinary(text, precision, scale);
        } catch (IllegalArgumentException ex) {
            gotMessage = ex.getMessage();
        }
        Assert.assertNotNull("Expected error", gotMessage);
        Assert.assertTrue(
                String.format("Expected error with message containing '%s', but got '%s'", expectedMessage, gotMessage),
                gotMessage.contains(expectedMessage)
        );
    }

    private void checkErrorBinaryToText(int precision, int scale, byte[] binary, String expectedMessage) {
        String gotMessage = null;
        try {
            Decimal.binaryToText(binary, precision, scale);
        } catch (IllegalArgumentException ex) {
            gotMessage = ex.getMessage();
        }
        Assert.assertNotNull("Expected error", gotMessage);
        Assert.assertTrue(
                String.format("Expected error with message containing '%s', but got '%s'", expectedMessage, gotMessage),
                gotMessage.contains(expectedMessage)
        );
    }
}
