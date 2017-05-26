package ru.yandex.yt.ytclient.yson;

public final class YsonFormatUtil {
    private static final char[] DIGITS = "0123456789abcdef".toCharArray();

    private YsonFormatUtil() {
        // static class
    }

    public static boolean isPrintable(byte b) {
        return b >= 32 && b <= 126;
    }

    public static boolean isPrintable(char c) {
        return c >= 32 && c <= 126;
    }

    public static boolean isOctDigit(byte b) {
        return b >= '0' && b <= '7';
    }

    public static boolean isHexDigit(byte b) {
        return b >= '0' && b <= '9' || b >= 'a' && b <= 'f' || b >= 'A' && b <= 'F';
    }

    public static int decodeHexDigit(byte b) {
        switch (b) {
            case '0': return 0;
            case '1': return 1;
            case '2': return 2;
            case '3': return 3;
            case '4': return 4;
            case '5': return 5;
            case '6': return 6;
            case '7': return 7;
            case '8': return 8;
            case '9': return 9;
            case 'a': return 10;
            case 'b': return 11;
            case 'c': return 12;
            case 'd': return 13;
            case 'e': return 14;
            case 'f': return 15;
            case 'A': return 10;
            case 'B': return 11;
            case 'C': return 12;
            case 'D': return 13;
            case 'E': return 14;
            case 'F': return 15;
            default: return -1;
        }
    }

    public static int decodeOctDigit(byte b) {
        switch (b) {
            case '0': return 0;
            case '1': return 1;
            case '2': return 2;
            case '3': return 3;
            case '4': return 4;
            case '5': return 5;
            case '6': return 6;
            case '7': return 7;
            default: return -1;
        }
    }

    public static int quotedByteLength(byte b, byte next) {
        switch (b) {
            case '\t':
            case '\n':
            case '\r':
            case '"':
            case '\\':
                return 2;
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                if (!isOctDigit(next)) {
                    return 2;
                }
                // fall through
            default:
                if (isPrintable(b)) {
                    return 1;
                } else {
                    return 4;
                }
        }
    }

    public static int quotedBytesLength(byte[] bytes, byte next) {
        int length = 0;
        if (bytes.length > 0) {
            for (int i = 1; i < bytes.length; ++i) {
                length += quotedByteLength(bytes[i - 1], bytes[i]);
            }
            length += quotedByteLength(bytes[bytes.length - 1], next);
        }
        return length;
    }

    public static void appendQuotedByte(StringBuilder sb, byte b, byte next) {
        switch (b) {
            case '\t':
                sb.append("\\t");
                break;
            case '\n':
                sb.append("\\n");
                break;
            case '\r':
                sb.append("\\r");
                break;
            case '"':
                sb.append("\\\"");
                break;
            case '\\':
                sb.append("\\\\");
                break;
            case 0:
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                if (!isOctDigit(next)) {
                    sb.append('\\');
                    sb.append(b);
                    break;
                }
                // fall through
            default:
                if (isPrintable(b)) {
                    sb.append((char) (b & 0xff));
                } else if (!isHexDigit(next)) {
                    sb.append("\\x");
                    sb.append(DIGITS[(b >>> 4) & 15]);
                    sb.append(DIGITS[b & 15]);
                } else {
                    sb.append('\\');
                    sb.append((b >>> 6) & 7);
                    sb.append((b >>> 3) & 7);
                    sb.append(b & 7);
                }
                break;
        }
    }

    public static void appendQuotedBytes(StringBuilder sb, byte[] bytes, byte next) {
        if (bytes.length > 0) {
            for (int i = 1; i < bytes.length; ++i) {
                appendQuotedByte(sb, bytes[i - 1], bytes[i]);
            }
            appendQuotedByte(sb, bytes[bytes.length - 1], next);
        }
    }

    public static boolean needToQuoteString(String s) {
        for (int i = 0; i < s.length(); ++i) {
            char c = s.charAt(i);
            if (c == '"' || c == '\\' || !isPrintable(c)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isUnquotedStringFirstByte(byte b) {
        return b >= 'A' && b <= 'Z' ||
                b >= 'a' && b <= 'z' ||
                b == '_';
    }

    public static boolean isUnquotedStringByte(byte b) {
        return b >= 'A' && b <= 'Z' ||
                b >= 'a' && b <= 'z' ||
                b >= '0' && b <= '9' ||
                b == '_' || b == '-' || b == '%' || b == '.';
    }

    static boolean isNumericFirstByte(byte first) {
        return first >= '0' && first <= '9' || first == '-' || first == '+';
    }
}
