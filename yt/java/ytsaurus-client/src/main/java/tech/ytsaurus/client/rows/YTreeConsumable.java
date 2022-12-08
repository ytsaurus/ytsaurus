package tech.ytsaurus.client.rows;

public interface YTreeConsumable {

    void onEntity();

    void onInteger(long value);

    void onBoolean(boolean value);

    void onDouble(double value);

    void onBytes(byte[] bytes);
}
