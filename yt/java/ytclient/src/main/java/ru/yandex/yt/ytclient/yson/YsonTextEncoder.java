package ru.yandex.yt.ytclient.yson;

import java.nio.charset.StandardCharsets;

import ru.yandex.yt.ytclient.ytree.YTreeConsumer;
import ru.yandex.yt.ytclient.ytree.YTreeNode;

public class YsonTextEncoder implements YTreeConsumer {
    private final StringBuilder sb;
    private boolean firstItem;

    public YsonTextEncoder(StringBuilder sb) {
        this.sb = sb;
    }

    private void appendByte(byte b) {
        sb.append((char) (b & 0xff));
    }

    private void appendQuotedBytes(byte[] value) {
        int length = YsonFormatUtil.quotedBytesLength(value, (byte) '"');
        sb.ensureCapacity(sb.length() + length + 2);
        sb.append('"');
        YsonFormatUtil.appendQuotedBytes(sb, value, (byte) '"');
        sb.append('"');
    }

    private void appendQuotedString(String value) {
        if (YsonFormatUtil.needToQuoteString(value)) {
            appendQuotedBytes(value.getBytes(StandardCharsets.UTF_8));
        } else {
            sb.append('"').append(value).append('"');
        }
    }

    @Override
    public void onStringScalar(String value) {
        appendQuotedString(value);
    }

    @Override
    public void onStringScalar(byte[] value) {
        appendQuotedBytes(value);
    }

    @Override
    public void onInt64Scalar(long value) {
        sb.append(value);
    }

    @Override
    public void onUint64Scalar(long value) {
        sb.append(Long.toUnsignedString(value)).append('u');
    }

    @Override
    public void onDoubleScalar(double value) {
        sb.append(value);
    }

    @Override
    public void onBooleanScalar(boolean value) {
        sb.append(value ? "%true" : "%false");
    }

    @Override
    public void onEntity() {
        appendByte(YsonTags.ENTITY);
    }

    @Override
    public void onBeginList() {
        appendByte(YsonTags.BEGIN_LIST);
        firstItem = true;
    }

    @Override
    public void onListItem() {
        if (firstItem) {
            firstItem = false;
        } else {
            appendByte(YsonTags.ITEM_SEPARATOR);
        }
    }

    @Override
    public void onEndList() {
        appendByte(YsonTags.END_LIST);
        firstItem = false;
    }

    @Override
    public void onBeginMap() {
        appendByte(YsonTags.BEGIN_MAP);
        firstItem = true;
    }

    @Override
    public void onKeyedItem(String key) {
        if (firstItem) {
            firstItem = false;
        } else {
            appendByte(YsonTags.ITEM_SEPARATOR);
        }
        onStringScalar(key);
        appendByte(YsonTags.KEY_VALUE_SEPARATOR);
    }

    @Override
    public void onEndMap() {
        appendByte(YsonTags.END_MAP);
        firstItem = false;
    }

    @Override
    public void onBeginAttributes() {
        appendByte(YsonTags.BEGIN_ATTRIBUTES);
        firstItem = true;
    }

    @Override
    public void onEndAttributes() {
        appendByte(YsonTags.END_ATTRIBUTES);
        firstItem = false;
    }

    public static void encode(StringBuilder sb, YTreeNode value) {
        YsonTextEncoder encoder = new YsonTextEncoder(sb);
        if (value != null) {
            value.writeTo(encoder);
        } else {
            encoder.onEntity();
        }
    }

    public static String encode(YTreeNode value) {
        StringBuilder sb = new StringBuilder();
        encode(sb, value);
        return sb.toString();
    }
}
