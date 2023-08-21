package tech.ytsaurus.yson;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

class YsonLexer {
    static final char[] NAN_LITERAL = "%nan".toCharArray();
    static final char[] FALSE_LITERAL = "%false".toCharArray();
    static final char[] TRUE_LITERAL = "%true".toCharArray();
    static final char[] INF_LITERAL = "%inf".toCharArray();
    static final char[] PLUS_INF_LITERAL = "%+inf".toCharArray();
    static final char[] MINUS_INF_LITERAL = "%-inf".toCharArray();

    private final StreamReader reader;
    @Nullable
    private Byte lookahead = null;

    YsonLexer(byte[] buffer) {
        this.reader = new StreamReader(buffer);
    }

    public YsonToken getNextToken() {
        var ch = peekNextByte();

        while (ch != null) {
            switch (ch) {
                case YsonTags.BINARY_STRING: {
                    readNextByte();
                    return YsonToken.stringValue(readBinaryString());
                }
                case YsonTags.BINARY_INT: {
                    readNextByte();
                    var v = parseInt64();
                    return YsonToken.int64Value(v);
                }
                case YsonTags.BINARY_UINT: {
                    readNextByte();
                    return YsonToken.uint64Value(parseUint64());
                }
                case YsonTags.BINARY_DOUBLE: {
                    readNextByte();
                    return YsonToken.doubleValue(parseDouble());
                }
                case YsonTags.BINARY_FALSE: {
                    readNextByte();
                    return YsonToken.booleanValue(false);
                }
                case YsonTags.BINARY_TRUE: {
                    readNextByte();
                    return YsonToken.booleanValue(true);
                }
                case '%': {
                    return parsePercentToken();
                }
                case '#': {
                    readNextByte();
                    return YsonToken.entityValue();
                }
                default: {
                    break;
                }
            }

            if (ch == '_' || ch == '"' || Character.isAlphabetic(ch)) {
                return YsonToken.stringValue(parseString());
            }

            if (ch == '+' || ch == '-' || Character.isDigit(ch)) {
                return parseNumericToken(ch);
            }

            if (Character.isWhitespace(ch)) {
                readNextByte();
                ch = peekNextByte();
                continue;
            }

            readNextByte();
            return YsonToken.fromSymbol(ch);
        }

        return YsonToken.endOfStream();
    }

    private YsonToken parseNumericToken(byte firstByte) {
        readNextByte();
        final int int64Type = 0;
        final int uint64Type = 1;
        final int doubleType = 2;

        int numberType;
        if ('0' <= firstByte && firstByte <= '9' || firstByte == '-' || firstByte == '+') {
            numberType = int64Type;
        } else if (firstByte == '.') {
            numberType = doubleType;
        } else {
            throw new YsonError("Numeric byte was expected, but got: " + firstByte);
        }

        StringBuilder sb = new StringBuilder();
        sb.append((char) firstByte);
        while (true) {
            Byte maybeByte = peekNextByte();
            if (maybeByte == null) {
                break;
            }
            byte b = maybeByte;
            if (b >= '0' && b <= '9' || b == '-' || b == '+') {
                sb.append((char) b);
                readNextByte();
            } else if (b == '.' || b == 'e' || b == 'E') {
                sb.append((char) b);
                numberType = doubleType;
                readNextByte();
            } else if (b == 'u') {
                sb.append((char) b);
                numberType = uint64Type;
                readNextByte();
            } else {
                break;
            }
        }

        switch (numberType) {
            case int64Type: {
                String text = sb.toString();
                long value;
                try {
                    value = Long.parseLong(text);
                } catch (NumberFormatException e) {
                    throw new YsonError("Error parsing int64 literal: " + text, e);
                }
                return YsonToken.int64Value(value);
            }
            case uint64Type: {
                sb.deleteCharAt(sb.length() - 1); // trim trailing 'u'
                String text = sb.toString();
                long value;
                try {
                    value = Long.parseUnsignedLong(text);
                } catch (NumberFormatException e) {
                    throw new YsonError("Error parsing uint64 literal " + text, e);
                }
                return YsonToken.uint64Value(value);
            }
            case doubleType: {
                String text = sb.toString();
                double value;
                try {
                    value = Double.parseDouble(text);
                } catch (NumberFormatException e) {
                    throw new YsonError("Error parsing double literal " + text, e);
                }
                return YsonToken.doubleValue(value);
            }
            default:
                throw new YsonError("Unexpected number type " + numberType);
        }
    }

    private YsonToken parsePercentToken() {
        var ch = reader.readByte();
        switch (ch) {
            case 'n': {
                verifyLiteral(2, NAN_LITERAL);
                return YsonToken.doubleValue(Double.NaN);
            }
            case 't': {
                verifyLiteral(2, TRUE_LITERAL);
                return YsonToken.booleanValue(true);
            }
            case 'f': {
                verifyLiteral(2, FALSE_LITERAL);
                return YsonToken.booleanValue(false);
            }
            case 'i': {
                verifyLiteral(2, INF_LITERAL);
                return YsonToken.doubleValue(Double.POSITIVE_INFINITY);
            }
            case '+': {
                verifyLiteral(2, PLUS_INF_LITERAL);
                return YsonToken.doubleValue(Double.POSITIVE_INFINITY);
            }
            case '-': {
                verifyLiteral(2, MINUS_INF_LITERAL);
                return YsonToken.doubleValue(Double.NEGATIVE_INFINITY);
            }
            default: {
                throw new YsonError("Unknown percent literal");
            }
        }
    }

    private void verifyLiteral(int startIndex, char[] literal) {
        for (int i = startIndex; i < literal.length; ++i) {
            byte c = reader.readByte();
            if (literal[i] != c) {
                throw new YsonError(String.format(
                        "Bad yson literal: %s",
                        String.copyValueOf(literal, 0, i) + (char) c));
            }
        }
    }

    private long parseInt64() {
        long uint = reader.readVarUint64();
        return VarintUtils.decodeZigZag64(uint);
    }

    private long parseUint64() {
        return reader.readVarUint64();
    }

    private double parseDouble() {
        long bits = reader.readFixed64();
        return Double.longBitsToDouble(bits);
    }

    private String parseString() {
        var ch = peekNextByte();
        if (ch == null) {
            throw new YsonError("Premature end-of-stream while expecting string literal in Yson");
        }
        if (ch == YsonTags.BINARY_STRING) {
            return readBinaryString();
        }
        if (ch == '"') {
            return readQuotedString();
        }
        if (!(ch == '_' || ch == '%' || Character.isAlphabetic(ch))) {
            throw new YsonError(String.format("Expecting string literal but found %s in Yson", ch));
        }
        return readUnquotedString();
    }

    private String readBinaryString() {
        BufferReference ref = new BufferReference();
        readBinaryString(ref);
        return new String(
                Arrays.copyOfRange(ref.getBuffer(), ref.getOffset(), ref.getOffset() + ref.getLength()),
                StandardCharsets.UTF_8
        );
    }

    private void readBinaryString(BufferReference bufferReference) {
        long stringLength = readSInt64();
        if (stringLength < 0) {
            throw new YsonError(String.format("Yson string length is negative: %d", stringLength));
        }
        if (stringLength > Integer.MAX_VALUE) {
            throw new YsonError(
                    String.format("Yson string length exceeds limit: %d > %d",
                            stringLength,
                            Integer.MAX_VALUE
                    ));
        }
        reader.readBytes((int) stringLength, bufferReference);
    }

    private long readSInt64() {
        long uint = reader.readVarUint64();
        return VarintUtils.decodeZigZag64(uint);
    }

    private String readQuotedString() {
        expectByte((byte) '"');
        var result = new ByteArrayOutputStream();
        var pendingNextByte = false;
        while (true) {
            var ch = readNextByte();
            if (ch == null) {
                throw new YsonError("Premature end-of-stream while reading string literal in Yson");
            }
            if (ch == (byte) '"' && !pendingNextByte) {
                break;
            }
            result.write(ch);
            if (pendingNextByte) {
                pendingNextByte = false;
            } else if (ch == (byte) '\\') {
                pendingNextByte = true;
            }
        }
        return result.toString(StandardCharsets.UTF_8);
    }

    private String readUnquotedString() {
        var result = new ByteArrayOutputStream();
        while (true) {
            var ch = peekNextByte();
            if (ch != null && (Character.isAlphabetic(ch) || Character.isDigit(ch) ||
                    List.of((byte) '_', (byte) '%', (byte) '-', (byte) '.').contains(ch))) {
                readNextByte();
                result.write(ch);
            } else {
                break;
            }
        }
        return result.toString(StandardCharsets.UTF_8);
    }

    private void expectByte(byte expected) {
        var ch = readNextByte();
        if (ch == null) {
            throw new YsonError(String.format("Premature end-of-stream expecting %s in Yson", ch));
        }
        if (ch != expected) {
            throw new YsonError(String.format("Found '%s' while expecting '%s' in Yson", ch, expected));
        }
    }

    @Nullable
    private Byte peekNextByte() {
        if (lookahead != null) {
            return lookahead;
        }
        var ch = reader.tryReadByte();
        if (ch == StreamReader.END_OF_STREAM) {
            return null;
        }
        lookahead = (byte) ch;
        return lookahead;
    }

    @Nullable
    private Byte readNextByte() {
        Byte result;
        if (lookahead == null) {
            var ch = reader.tryReadByte();
            if (ch == StreamReader.END_OF_STREAM) {
                result = null;
            } else {
                result = (byte) ch;
            }
        } else {
            result = lookahead;
        }
        lookahead = null;
        return result;
    }

}
