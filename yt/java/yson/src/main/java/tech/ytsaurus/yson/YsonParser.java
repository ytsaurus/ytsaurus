package tech.ytsaurus.yson;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.UncheckedIOException;

import javax.annotation.Nullable;

/**
 * Parser of YSON data.
 * <p>
 * Class throws {@link YsonError} if yson is malformed.
 * All IO errors are translated to {@link UncheckedIOException}.
 *
 * @see <a href="https://ytsaurus.tech/docs/en/user-guide/storage/yson">
 * YSON documentation
 * </a>
 */
public class YsonParser {
    private final StreamReader tokenizer;
    private final BufferReference bufferReference = new BufferReference();
    private boolean expectFragmentSeparator = false;

    public YsonParser(InputStream input) {
        this(input, 512);
    }

    public YsonParser(InputStream input, int bufferSize) {
        this(new BufferedStreamZeroCopyInput(input, bufferSize));
    }

    public YsonParser(InputStream input, byte[] buffer) {
        this(new BufferedStreamZeroCopyInput(input, buffer));
    }

    public YsonParser(byte[] buffer) {
        this(buffer, 0, buffer.length);
    }

    public YsonParser(byte[] buffer, int offset, int length) {
        this(new ByteZeroCopyInput(buffer, offset, length));
    }

    private YsonParser(ZeroCopyInput input) {
        this.tokenizer = new StreamReader(input);
    }

    /**
     * Parse YSON node.
     * <p>
     * This method will consume whole input stream and will throw {@link YsonError} exception if
     * stream contains trailing (non whitespace) bytes.
     */
    public void parseNode(YsonConsumer consumer) {
        byte b1 = tokenizer.readByte();
        parseNodeImpl(true, b1, consumer);

        while (true) {
            int maybeByte = tokenizer.tryReadByte();
            if (maybeByte == StreamReader.END_OF_STREAM) {
                break;
            }
            byte b = (byte) maybeByte;
            if (!isSpace(b)) {
                throw new YsonError(String.format("Unexpected trailing character: %s", debugByteString(b)));
            }
        }
    }

    /**
     * Parse only attributes and return its size in bytes.
     */
    public int parseAttributes(YsonConsumer consumer) {
        byte currentByte = skipWhitespaces();
        if (currentByte == YsonTags.BEGIN_ATTRIBUTES) {
            consumer.onBeginAttributes();
            parseMapLikeImpl(YsonTags.END_ATTRIBUTES, consumer);
            consumer.onEndAttributes();
            skipSpaces();
        }
        tokenizer.unreadByte();
        return tokenizer.getPosition();
    }

    private byte skipWhitespaces() {
        byte currentByte = tokenizer.readByte();
        while (isSpace(currentByte)) {
            currentByte = tokenizer.readByte();
        }
        return currentByte;
    }

    private void skipSpaces() {
        byte currentByte = tokenizer.readByte();
        while (currentByte == ' ') {
            currentByte = tokenizer.readByte();
        }
    }

    /**
     * Parse YSON list fragment
     * <p>
     * This method will consume whole input stream and will throw {@link YsonError} exception if
     * stream contains trailing (non whitespace) bytes.
     */
    public void parseListFragment(YsonConsumer consumer) {
        while (true) {
            int maybeByte = moveToNextListItem(expectFragmentSeparator);
            if (maybeByte == StreamReader.END_OF_STREAM) {
                return;
            }
            consumer.onListItem();
            parseNodeImpl(true, (byte) maybeByte, consumer);
            expectFragmentSeparator = true;
        }
    }

    /**
     * Parses single item of list fragment.
     * <p>
     * Unlike {@link #parseListFragment} that calls {@link YsonConsumer#onListItem()} before each top level list item
     * this method doesn't call
     *
     * @return true if item is parsed or false if end of stream is reached.
     */
    public boolean parseListFragmentItem(YsonConsumer consumer) {
        int maybeByte = moveToNextListItem(expectFragmentSeparator);
        if (maybeByte == StreamReader.END_OF_STREAM) {
            return false;
        }
        parseNodeImpl(true, (byte) maybeByte, consumer);
        expectFragmentSeparator = true;
        return true;
    }

    //
    // Implementation
    //
    private void parseNodeImpl(boolean allowAttributes, byte currentByte, YsonConsumer consumer) {
        while (true) {
            switch (currentByte) {
                case YsonTags.BINARY_STRING:
                    readBinaryString(bufferReference);
                    consumer.onString(bufferReference.getBuffer(), bufferReference.getOffset(),
                            bufferReference.getLength());
                    return;
                case YsonTags.BINARY_INT: {
                    long value = readSInt64();
                    consumer.onInteger(value);
                    return;
                }
                case YsonTags.BINARY_DOUBLE: {
                    double value = readDouble();
                    consumer.onDouble(value);
                    return;
                }
                case YsonTags.BINARY_FALSE:
                    consumer.onBoolean(false);
                    return;
                case YsonTags.BINARY_TRUE:
                    consumer.onBoolean(true);
                    return;
                case YsonTags.BINARY_UINT: {
                    long value = tokenizer.readVarUint64();
                    consumer.onUnsignedInteger(value);
                    return;
                }
                case YsonTags.ENTITY:
                    consumer.onEntity();
                    return;
                case YsonTags.BEGIN_LIST:
                    consumer.onBeginList();
                    parseListImpl(consumer);
                    consumer.onEndList();
                    return;
                case YsonTags.BEGIN_ATTRIBUTES:
                    if (!allowAttributes) {
                        throw new YsonError("Unexpected token '<'");
                    }
                    consumer.onBeginAttributes();
                    parseMapLikeImpl(YsonTags.END_ATTRIBUTES, consumer);
                    consumer.onEndAttributes();
                    parseNodeImpl(false, tokenizer.readByte(), consumer);
                    return;
                case YsonTags.BEGIN_MAP:
                    consumer.onBeginMap();
                    parseMapLikeImpl(YsonTags.END_MAP, consumer);
                    consumer.onEndMap();
                    return;
                case '"': {
                    byte[] string = readQuotedString();
                    consumer.onString(string, 0, string.length);
                    return;
                }
                case '%':
                    currentByte = tokenizer.readByte();
                    if (currentByte == 'n') {
                        verifyLiteral(2, YsonLexer.NAN_LITERAL);
                        consumer.onDouble(Double.NaN);
                        return;
                    } else if (currentByte == 't') {
                        verifyLiteral(2, YsonLexer.TRUE_LITERAL);
                        consumer.onBoolean(true);
                        return;
                    } else if (currentByte == 'f') {
                        verifyLiteral(2, YsonLexer.FALSE_LITERAL);
                        consumer.onBoolean(false);
                        return;
                    } else if (currentByte == 'i') {
                        verifyLiteral(2, YsonLexer.INF_LITERAL);
                        consumer.onDouble(Double.POSITIVE_INFINITY);
                        return;
                    } else if (currentByte == '+') {
                        verifyLiteral(2, YsonLexer.PLUS_INF_LITERAL);
                        consumer.onDouble(Double.POSITIVE_INFINITY);
                        return;
                    } else if (currentByte == '-') {
                        verifyLiteral(2, YsonLexer.MINUS_INF_LITERAL);
                        consumer.onDouble(Double.NEGATIVE_INFINITY);
                        return;
                    }
                    throw new YsonError(String.format("Bad yson literal %%%s", (char) currentByte));
                default:
                    if (isSpace(currentByte)) {
                        currentByte = tokenizer.readByte();
                        continue;
                    }
                    byte[] string = tryReadUnquotedString(currentByte);
                    if (string != null) {
                        consumer.onString(string, 0, string.length);
                        return;
                    } else if (tryConsumeTextNumber(currentByte, consumer)) {
                        return;
                    }
                    throw new YsonError(String.format("Unexpected byte: %s in yson", debugByteString(currentByte)));
            }
        }
    }

    private void parseListImpl(YsonConsumer consumer) {
        boolean expectSeparator = false;
        while (true) {
            int maybeByte = moveToNextListItem(expectSeparator);
            if (maybeByte == StreamReader.END_OF_STREAM) {
                throw new YsonError("Unexpected end of stream");
            }
            byte b = (byte) maybeByte;
            if (b == YsonTags.END_LIST) {
                return;
            }
            consumer.onListItem();
            parseNodeImpl(true, b, consumer);
            expectSeparator = true;
        }
    }

    private int moveToNextListItem(boolean expectSeparator) {
        while (true) {
            int maybeByte = tokenizer.tryReadByte();
            if (maybeByte == StreamReader.END_OF_STREAM) {
                return maybeByte;
            }

            byte b = (byte) maybeByte;
            if (isSpace(b)) {
                continue;
            } else if (expectSeparator) {
                if (b == YsonTags.ITEM_SEPARATOR) {
                    expectSeparator = false;
                    continue;
                } else if (b != YsonTags.END_LIST) {
                    throw new YsonError(String.format("Expected yson ';' or ']', found: %s", debugByteString(b)));
                }
            }
            return b;
        }
    }

    private void parseMapLikeImpl(byte endByte, YsonConsumer consumer) {
        boolean expectSeparator = false;
        while (true) {
            int maybeByte = tokenizer.tryReadByte();
            if (maybeByte == StreamReader.END_OF_STREAM) {
                throw new YsonError("Unexpected end of stream");
            }
            byte b = (byte) maybeByte;
            if (b == (int) endByte) {
                return;
            } else if (isSpace(b)) {
                continue;
            } else if (expectSeparator) {
                if (b == YsonTags.ITEM_SEPARATOR) {
                    expectSeparator = false;
                    continue;
                } else {
                    throw new YsonError(
                            String.format("Expected ';' or '%s', found: %s",
                                    (char) endByte,
                                    debugByteString(b)
                            ));
                }
            }
            parseMapKey(b, consumer);
            b = tokenizer.readByte();
            if (b != '=') {
                while (isSpace(b)) {
                    b = tokenizer.readByte();
                }
                if (b != '=') {
                    throw new YsonError(String.format("Expected '='; found %s", debugByteString(b)));
                }
            }
            b = tokenizer.readByte();
            parseNodeImpl(true, b, consumer);
            expectSeparator = true;
        }
    }

    private void parseMapKey(byte currentByte, YsonConsumer consumer) {
        while (true) {
            switch (currentByte) {
                case YsonTags.BINARY_STRING:
                    readBinaryString(bufferReference);
                    consumer.onKeyedItem(bufferReference.getBuffer(), bufferReference.getOffset(),
                            bufferReference.getLength());
                    return;
                case '"': {
                    byte[] string = readQuotedString();
                    consumer.onKeyedItem(string, 0, string.length);
                    return;
                }
                default: {
                    if (isSpace(currentByte)) {
                        currentByte = tokenizer.readByte();
                        continue;
                    }
                    byte[] string = tryReadUnquotedString(currentByte);
                    if (string != null) {
                        consumer.onKeyedItem(string, 0, string.length);
                        return;
                    }
                    throw new YsonError(String.format("Expected key, found: %s byte", debugByteString(currentByte)));
                }
            }
        }
    }

    private boolean isSpace(byte b) {
        switch (b) {
            case ' ':
            case '\f':
            case '\n':
            case '\r':
            case '\t':
            case 0xb: // '\v'
                return true;
            default:
                return false;
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void verifyLiteral(int startIndex, char[] literal) {
        for (int i = startIndex; i < literal.length; ++i) {
            byte c = tokenizer.readByte();
            if (literal[i] != c) {
                throw new YsonError(String.format(
                        "Bad yson literal: %s",
                        String.copyValueOf(literal, 0, i) + (char) c));
            }
        }
    }

    private double readDouble() {
        long bits = tokenizer.readFixed64();
        return Double.longBitsToDouble(bits);
    }

    private long readSInt64() {
        long uint = tokenizer.readVarUint64();
        return VarintUtils.decodeZigZag64(uint);
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
        tokenizer.readBytes((int) stringLength, bufferReference);
    }

    @Nullable
    private byte[] tryReadUnquotedString(byte firstByte) {
        boolean isUnquotedString = 'a' <= firstByte && firstByte <= 'z' ||
                'A' <= firstByte && firstByte <= 'Z' ||
                firstByte == '_';
        if (!isUnquotedString) {
            return null;
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(firstByte);
        while (true) {
            int maybeByte = tokenizer.tryReadByte();
            if (maybeByte == StreamReader.END_OF_STREAM) {
                break;
            }
            byte b = (byte) maybeByte;
            if ('a' <= b && b <= 'z' ||
                    'A' <= b && b <= 'Z' ||
                    '0' <= b && b <= '9' ||
                    b == '_' ||
                    b == '.' ||
                    b == '-'
            ) {
                out.write(b);
            } else {
                tokenizer.unreadByte();
                break;
            }
        }
        return out.toByteArray();
    }

    private boolean tryConsumeTextNumber(byte firstByte, YsonConsumer consumer) {
        final int int64Type = 0;
        final int uint64Type = 1;
        final int doubleType = 2;

        int numberType;
        if ('0' <= firstByte && firstByte <= '9' || firstByte == '-' || firstByte == '+') {
            numberType = int64Type;
        } else if (firstByte == '.') {
            numberType = doubleType;
        } else {
            return false;
        }

        StringBuilder sb = new StringBuilder();
        sb.append((char) firstByte);
        while (true) {
            int maybeByte = tokenizer.tryReadByte();
            if (maybeByte == StreamReader.END_OF_STREAM) {
                break;
            }
            byte b = (byte) maybeByte;
            if (b >= '0' && b <= '9' || b == '-' || b == '+') {
                sb.append((char) b);
            } else if (b == '.' || b == 'e' || b == 'E') {
                sb.append((char) b);
                numberType = doubleType;
            } else if (b == 'u') {
                sb.append((char) b);
                numberType = uint64Type;
            } else {
                tokenizer.unreadByte();
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
                consumer.onInteger(value);
                return true;
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
                consumer.onUnsignedInteger(value);
                return true;
            }
            case doubleType: {
                String text = sb.toString();
                double value;
                try {
                    value = Double.parseDouble(text);
                } catch (NumberFormatException e) {
                    throw new YsonError("Error parsing double literal " + text, e);
                }
                consumer.onDouble(value);
                return true;
            }
            default:
                throw new IllegalStateException("Unexpected number type " + numberType);
        }
    }

    private byte[] readQuotedString() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while (true) {
            byte current = tokenizer.readByte();
            if (current == '"') {
                // конец строки
                break;
            }
            if (current != '\\') {
                out.write(current);
            } else {
                // какая-то escape последовательность
                switch (current = tokenizer.readByte()) {
                    case 'a':
                        out.write('\u0007');
                        continue;
                    case 'b':
                        out.write('\u0008');
                        continue;
                    case 't':
                        out.write('\t');
                        continue;
                    case 'n':
                        out.write('\n');
                        continue;
                    case 'v':
                        out.write('\u000b');
                        continue;
                    case 'f':
                        out.write('\u000c');
                        continue;
                    case 'r':
                        out.write('\r');
                        continue;
                    case '"':
                        out.write('"');
                        continue;
                    case '\\':
                        out.write('\\');
                        continue;
                    default:
                        break;
                }
                if (current == 'x') {
                    // hexadecimal sequence (maximum two numbers).
                    current = tokenizer.readByte();
                    int first = decodeDigit(current);
                    if (first > 15) {
                        tokenizer.unreadByte();
                        // decode "\x" as just "x" if there is no number
                        out.write('x');
                        continue;
                    }
                    current = tokenizer.readByte();
                    int second = decodeDigit(current);
                    if (second > 15) {
                        out.write((byte) first);
                    } else {
                        int value = (first << 4) | second;
                        out.write((byte) value);
                    }
                    continue;
                }
                int firstOctal = decodeDigit(current);
                if (firstOctal < 8) {
                    current = tokenizer.readByte();
                    int secondOctal = decodeDigit(current);
                    if (secondOctal > 7) {
                        out.write((byte) firstOctal);
                        tokenizer.unreadByte();
                        continue;
                    }

                    current = tokenizer.readByte();
                    int thirdOctal = decodeDigit(current);
                    int value = (firstOctal << 3) | secondOctal;
                    if (thirdOctal > 7) {
                        out.write((byte) ((firstOctal << 3) | secondOctal));
                        tokenizer.unreadByte();
                    } else {
                        value = (value << 3) | thirdOctal;
                        if (value > 255) {
                            throw new YsonError(String.format(
                                    "Invalid escape sequence: \\%d%d%d",
                                    firstOctal, secondOctal, thirdOctal));
                        }
                        out.write((byte) value);
                    }
                }
            }
        }
        return out.toByteArray();
    }

    private int decodeDigit(byte b) {
        switch (b) {
            case '0':
                return 0;
            case '1':
                return 1;
            case '2':
                return 2;
            case '3':
                return 3;
            case '4':
                return 4;
            case '5':
                return 5;
            case '6':
                return 6;
            case '7':
                return 7;
            case '8':
                return 8;
            case '9':
                return 9;
            case 'a':
            case 'A':
                return 10;
            case 'b':
            case 'B':
                return 11;
            case 'c':
            case 'C':
                return 12;
            case 'd':
            case 'D':
                return 13;
            case 'e':
            case 'E':
                return 14;
            case 'f':
            case 'F':
                return 15;
            default:
                return Integer.MAX_VALUE;
        }
    }

    private String debugByteString(byte b) {
        StringWriter writer = new StringWriter();
        writer.write('\'');
        YsonTextUtils.writeQuotedByte(b, writer);
        writer.write('\'');
        return writer.toString();
    }
}

class ByteZeroCopyInput implements ZeroCopyInput {
    @Nullable
    byte[] buffer;
    int offset;
    int length;

    ByteZeroCopyInput(@Nullable byte[] buffer, int offset, int length) {
        this.buffer = buffer;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public boolean next(BufferReference out) {
        if (buffer == null) {
            return false;
        } else if (length == 0) {
            buffer = null;
            return false;
        }
        out.setBuffer(buffer);
        out.setOffset(offset);
        out.setLength(length);

        buffer = null;
        return true;
    }
}
