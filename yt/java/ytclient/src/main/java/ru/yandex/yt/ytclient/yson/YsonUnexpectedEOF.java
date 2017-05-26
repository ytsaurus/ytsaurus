package ru.yandex.yt.ytclient.yson;

public class YsonUnexpectedEOF extends YsonFormatException {
    public YsonUnexpectedEOF() {
        super("Unexpected EOF while parsing YSON");
    }
}
