package ru.yandex.yt.ytclient.yson;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.google.protobuf.CodedInputStream;

import ru.yandex.yt.ytclient.ytree.YTreeBuilder;
import ru.yandex.yt.ytclient.ytree.YTreeConsumer;
import ru.yandex.yt.ytclient.ytree.YTreeListNode;
import ru.yandex.yt.ytclient.ytree.YTreeMapNode;
import ru.yandex.yt.ytclient.ytree.YTreeNode;
import ru.yandex.yt.ytclient.ytree.YTreeNodeType;

public class YsonBinaryDecoder {
    private static final byte[] EMPTY_BUFFER = new byte[0];
    private static final char[] TRUE_VALUE = "true".toCharArray();
    private static final char[] FALSE_VALUE = "false".toCharArray();
    private static final int INITIAL_BUFFER_SIZE = 32;

    private final CodedInputStream input;
    private final YTreeConsumer consumer;
    private byte[] buffer = EMPTY_BUFFER;
    private int bufferLength;

    // Байт, сохранившийся после чтения значения
    private int savedByte = -1;

    public YsonBinaryDecoder(CodedInputStream input, YTreeConsumer consumer) {
        this.input = input;
        this.consumer = consumer;
    }

    private void saveByte(byte value) {
        savedByte = value & 0xff;
    }

    private void ensureNotSaved() {
        if (savedByte != -1) {
            throw new IllegalStateException("Attempt to read complex data when another byte is saved");
        }
    }

    private boolean finished() throws IOException {
        return savedByte == -1 && input.isAtEnd();
    }

    private byte readByte() throws IOException {
        if (savedByte != -1) {
            byte b = (byte) savedByte;
            savedByte = -1;
            return b;
        }
        if (input.isAtEnd()) {
            throw new YsonUnexpectedEOF();
        } else {
            return input.readRawByte();
        }
    }

    private int readVarInt32() throws IOException {
        ensureNotSaved();
        return input.readSInt32();
    }

    private long readVarInt64() throws IOException {
        ensureNotSaved();
        return input.readSInt64();
    }

    private long readVarUint64() throws IOException {
        ensureNotSaved();
        return input.readUInt64();
    }

    private double readDouble() throws IOException {
        ensureNotSaved();
        return input.readDouble();
    }

    private byte[] readRawBytes(int size) throws IOException {
        ensureNotSaved();
        return input.readRawBytes(size);
    }

    private byte readToken(boolean allowEof) throws IOException {
        while (!finished()) {
            byte b = readByte();
            if (!Character.isWhitespace(b & 0xff)) {
                return b;
            }
        }
        if (!allowEof) {
            throw new YsonUnexpectedEOF();
        }
        return YsonTags.BINARY_END;
    }

    private byte[] readBinaryString() throws IOException {
        int size = readVarInt32();
        if (size < 0) {
            throw new YsonFormatException("Negative binary string length");
        }
        return readRawBytes(size);
    }

    private void addBuffered(byte b) {
        if (buffer.length == 0) {
            buffer = new byte[INITIAL_BUFFER_SIZE];
        } else if (buffer.length == bufferLength) {
            byte[] newBuffer = new byte[buffer.length * 2];
            if (bufferLength > 0) {
                System.arraycopy(buffer, 0, newBuffer, 0, bufferLength);
            }
            buffer = newBuffer;
        }
        buffer[bufferLength++] = b;
    }

    private void addBuffered(char ch) {
        addBuffered((byte) ch);
    }

    /**
     * Читает содержимое квотированной строки после открывающей кавычки
     * <p>
     * Декодирование escape последовательностей примерно соответствует util/string/escape.cpp
     */
    private byte[] readQuotedString() throws IOException {
        bufferLength = 0;
        byte current = readByte();
        while (true) {
            if (current == '"') {
                // конец строки
                break;
            }
            if (current == '\\') {
                // какая-то escape последовательность
                switch (current = readByte()) {
                    case 'a':
                        addBuffered('\u0007');
                        current = readByte();
                        continue;
                    case 'b':
                        addBuffered('\u0008');
                        current = readByte();
                        continue;
                    case 't':
                        addBuffered('\t');
                        current = readByte();
                        continue;
                    case 'n':
                        addBuffered('\n');
                        current = readByte();
                        continue;
                    case 'v':
                        addBuffered('\u000b');
                        current = readByte();
                        continue;
                    case 'f':
                        addBuffered('\u000c');
                        current = readByte();
                        continue;
                    case 'r':
                        addBuffered('\r');
                        current = readByte();
                        continue;
                    case '"':
                        addBuffered('"');
                        current = readByte();
                        continue;
                    case '\\':
                        addBuffered('\\');
                        current = readByte();
                        continue;
                }
                if (current == 'x') {
                    // шестнадцатиричная последовательность (максимум два числа)
                    if (!YsonFormatUtil.isHexDigit(current = readByte())) {
                        // декодируем "\x" как просто "x" если нет ни одного числа
                        addBuffered('x');
                        continue;
                    }
                    int value = YsonFormatUtil.decodeHexDigit(current);
                    if (!YsonFormatUtil.isHexDigit(current = readByte())) {
                        addBuffered((byte) value);
                        continue;
                    }
                    value = (value << 4) | YsonFormatUtil.decodeHexDigit(current);
                    addBuffered((byte) value);
                    current = readByte();
                    continue;
                }
                if (YsonFormatUtil.isOctDigit(current)) {
                    // восьмеричная последовательность (максимум три числа)
                    int value = YsonFormatUtil.decodeOctDigit(current);
                    if (!YsonFormatUtil.isOctDigit(current = readByte())) {
                        addBuffered((byte) value);
                        continue;
                    }
                    value = (value << 3) | YsonFormatUtil.decodeOctDigit(current);
                    if (value < 32) {
                        // ещё есть шанс получить число < 256
                        if (!YsonFormatUtil.isOctDigit(current = readByte())) {
                            addBuffered((byte) value);
                            continue;
                        }
                        value = (value << 3) | YsonFormatUtil.decodeOctDigit(current);
                    }
                    addBuffered((byte) value);
                    current = readByte();
                    continue;
                }
            }
            // просто добавляем текущий символ
            addBuffered(current);
            current = readByte();
        }
        return Arrays.copyOf(buffer, bufferLength);
    }

    /**
     * Читает неквотированную строку (ReadUnquotedString из yt/core/yson/detail.h)
     */
    private byte[] readUnquotedString(byte first, boolean allowEof) throws IOException {
        bufferLength = 0;
        addBuffered(first);
        while (true) {
            if (allowEof && finished()) {
                break;
            }
            byte b = readByte();
            if (!YsonFormatUtil.isUnquotedStringByte(b)) {
                saveByte(b);
                break;
            }
            addBuffered(b);
        }
        return Arrays.copyOf(buffer, bufferLength);
    }

    private YsonFormatException invalidBooleanValue() {
        String value = new String(buffer, 0, bufferLength, StandardCharsets.UTF_8);
        return new YsonFormatException("Cannot parse %" + value + " as a boolean value");
    }

    /**
     * Читает boolean значение после '%' (%true или %false)
     */
    private boolean readBooleanValue() throws IOException {
        byte b;
        bufferLength = 0;
        addBuffered(b = readByte());
        if (b == TRUE_VALUE[0]) {
            for (int i = 1; i < TRUE_VALUE.length; ++i) {
                addBuffered(b = readByte());
                if (b != TRUE_VALUE[i]) {
                    throw invalidBooleanValue();
                }
            }
            return true;
        } else if (b == FALSE_VALUE[0]) {
            for (int i = 1; i < FALSE_VALUE.length; ++i) {
                addBuffered(b = readByte());
                if (b != FALSE_VALUE[i]) {
                    throw invalidBooleanValue();
                }
            }
            return false;
        } else {
            throw invalidBooleanValue();
        }
    }

    private String readMapKey(byte first) throws IOException {
        byte[] bytes;
        if (first == YsonTags.BINARY_STRING) {
            bytes = readBinaryString();
        } else if (first == '"') {
            bytes = readQuotedString();
        } else if (YsonFormatUtil.isUnquotedStringFirstByte(first)) {
            bytes = readUnquotedString(first, false);
        } else {
            throw new YsonUnexpectedToken(first, "STRING");
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private void parseMapFragment(byte endToken, boolean allowEof) throws IOException {
        byte token = readToken(allowEof);
        while (token != endToken) {
            consumer.onKeyedItem(readMapKey(token));

            token = readToken(false);
            if (token != YsonTags.KEY_VALUE_SEPARATOR) {
                throw new YsonUnexpectedToken(token, "KEY_VALUE_SEPARATOR");
            }

            parseNode(allowEof);

            token = readToken(allowEof);
            if (token == YsonTags.ITEM_SEPARATOR) {
                token = readToken(allowEof);
            } else if (token != endToken) {
                throw new YsonUnexpectedToken(token, "ITEM_SEPARATOR");
            }
        }
    }

    private void parseListFragment(byte endToken, boolean allowEof) throws IOException {
        byte token = readToken(allowEof);
        while (token != endToken) {
            consumer.onListItem();

            parseNode(token, allowEof);

            token = readToken(allowEof);
            if (token == YsonTags.ITEM_SEPARATOR) {
                token = readToken(allowEof);
            } else if (token != endToken) {
                throw new YsonUnexpectedToken(token, "ITEM_SEPARATOR");
            }
        }
    }

    private void parseNumeric(byte first, boolean allowEof) throws IOException {
        bufferLength = 0;
        addBuffered(first);
        YTreeNodeType type = YTreeNodeType.INT64;
        while (true) {
            if (allowEof && finished()) {
                break;
            }
            byte b = readByte();
            if (b >= '0' && b <= '9' || b == '-' || b == '+') {
                addBuffered(b);
            } else if (b == '.' || b == 'e' || b == 'E') {
                addBuffered(b);
                type = YTreeNodeType.DOUBLE;
            } else if (b == 'u') {
                addBuffered(b);
                type = YTreeNodeType.UINT64;
            } else if (b >= 'A' && b <= 'Z' || b >= 'a' && b <= 'z') {
                throw new YsonFormatException("Unexpected '" + (char) b + "' in numeric literal");
            } else {
                saveByte(b);
                break;
            }
        }
        switch (type) {
            case INT64: {
                String text = new String(buffer, 0, bufferLength, StandardCharsets.US_ASCII);
                long value;
                try {
                    value = Long.parseLong(text);
                } catch (NumberFormatException e) {
                    throw new YsonFormatException("Error parsing int64 literal " + text);
                }
                consumer.onInt64Scalar(value);
                break;
            }
            case UINT64: {
                String text = new String(buffer, 0, bufferLength - 1, StandardCharsets.US_ASCII);
                long value;
                try {
                    value = Long.parseUnsignedLong(text);
                } catch (NumberFormatException e) {
                    throw new YsonFormatException("Error parsing uint64 literal " + text);
                }
                consumer.onUint64Scalar(value);
                break;
            }
            case DOUBLE: {
                String text = new String(buffer, 0, bufferLength, StandardCharsets.US_ASCII);
                double value;
                try {
                    value = Double.parseDouble(text);
                } catch (NumberFormatException e) {
                    throw new YsonFormatException("Error parsing double literal " + text);
                }
                consumer.onDoubleScalar(value);
                break;
            }
            default:
                throw new IllegalStateException("Unexpected number type " + type);
        }
    }

    private void parseNode(byte first, boolean allowEof) throws IOException {
        if (first == YsonTags.BEGIN_ATTRIBUTES) {
            consumer.onBeginAttributes();
            parseMapFragment(YsonTags.END_ATTRIBUTES, false);
            consumer.onEndAttributes();
            first = readToken(allowEof);
        }

        switch (first) {
            case YsonTags.BINARY_STRING:
                consumer.onStringScalar(readBinaryString());
                break;
            case YsonTags.BINARY_INT:
                consumer.onInt64Scalar(readVarInt64());
                break;
            case YsonTags.BINARY_UINT:
                consumer.onUint64Scalar(readVarUint64());
                break;
            case YsonTags.BINARY_DOUBLE:
                consumer.onDoubleScalar(readDouble());
                break;
            case YsonTags.BINARY_FALSE:
                consumer.onBooleanScalar(false);
                break;
            case YsonTags.BINARY_TRUE:
                consumer.onBooleanScalar(true);
                break;
            case YsonTags.BEGIN_LIST:
                consumer.onBeginList();
                parseListFragment(YsonTags.END_LIST, false);
                consumer.onEndList();
                break;
            case YsonTags.BEGIN_MAP:
                consumer.onBeginMap();
                parseMapFragment(YsonTags.END_MAP, false);
                consumer.onEndMap();
                break;
            case YsonTags.ENTITY:
                consumer.onEntity();
                break;
            case '"':
                consumer.onStringScalar(readQuotedString());
                break;
            default:
                if (YsonFormatUtil.isUnquotedStringFirstByte(first)) {
                    consumer.onStringScalar(readUnquotedString(first, allowEof));
                } else if (YsonFormatUtil.isNumericFirstByte(first)) {
                    parseNumeric(first, allowEof);
                } else if (first == '%') {
                    consumer.onBooleanScalar(readBooleanValue());
                } else {
                    throw new YsonUnexpectedToken(first, "NODE");
                }
                break;
        }
    }

    private void parseNode(boolean allowEof) throws IOException {
        parseNode(readToken(false), allowEof);
    }

    private void ensureFinished() throws IOException {
        while (!finished()) {
            byte b = readToken(true);
            if (b != YsonTags.BINARY_END) {
                throw new YsonUnexpectedToken(b, "EOF");
            }
        }
    }

    public void parseNode() throws IOException {
        parseNode(true);
        ensureFinished();
    }

    public void parseMapFragment() throws IOException {
        parseMapFragment(YsonTags.BINARY_END, true);
        ensureFinished();
    }

    public void parseListFragment() throws IOException {
        parseListFragment(YsonTags.BINARY_END, true);
        ensureFinished();
    }

    public static YTreeNode parseNode(CodedInputStream in) throws IOException {
        return parseNode(in, new YTreeBuilder()).build();
    }

    public static <T extends YTreeConsumer> T parseNode(CodedInputStream in, T out) throws IOException {
        new YsonBinaryDecoder(in, out).parseNode();
        return out;
    }

    public static YTreeMapNode parseMapFragment(CodedInputStream in) throws IOException {
        return parseMapFragment(in, new YTreeBuilder().beginMap()).buildMap();
    }

    public static <T extends YTreeConsumer> T parseMapFragment(CodedInputStream in, T out) throws IOException {
        new YsonBinaryDecoder(in, out).parseMapFragment();
        return out;
    }

    public static YTreeListNode parseListFragment(CodedInputStream in) throws IOException {
        return parseListFragment(in, new YTreeBuilder().beginList()).buildList();
    }

    public static <T extends YTreeConsumer> T parseListFragment(CodedInputStream in, T out) throws IOException {
        new YsonBinaryDecoder(in, out).parseListFragment();
        return out;
    }
}
