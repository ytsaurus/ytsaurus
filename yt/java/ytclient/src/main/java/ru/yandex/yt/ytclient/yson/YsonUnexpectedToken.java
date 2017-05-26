package ru.yandex.yt.ytclient.yson;

public class YsonUnexpectedToken extends YsonFormatException {
    public YsonUnexpectedToken(byte token, String expected) {
        super("Unexpected " + formatToken(token) + " (expecting " + expected + ")");
    }

    private static String formatToken(byte token) {
        if (token >= 0x20 && token < 0x7f) {
            return String.format("character '%c'", (char) token);
        } else {
            return String.format("byte 0x%02x", token & 0xff);
        }
    }
}
