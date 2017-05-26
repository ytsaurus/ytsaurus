package ru.yandex.yt.ytclient.yson;

public class YsonFormatException extends RuntimeException {
    public YsonFormatException() {
    }

    public YsonFormatException(String message) {
        super(message);
    }
}
