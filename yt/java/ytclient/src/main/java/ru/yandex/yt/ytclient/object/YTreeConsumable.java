package ru.yandex.yt.ytclient.object;

public interface YTreeConsumable {

    void onEntity();

    void onInteger(long value);

    void onBoolean(boolean value);

    void onDouble(double value);

    void onBytes(byte[] bytes);
}
