package tech.ytsaurus.core.rows;

import java.io.InputStream;

import tech.ytsaurus.yson.YsonConsumer;

public interface YsonSerializable {
    void serialize(YsonConsumer consumer);
    void deserialize(InputStream input);
}
