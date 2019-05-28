package ru.yandex.yt.ytclient.object;

import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeConsumable;
import ru.yandex.yt.ytclient.tables.ColumnValueType;

public interface WireValueDeserializer<T> extends YTreeConsumable {

    void setId(int id);

    void setType(ColumnValueType type);

    void setAggregate(boolean aggregate);

    void setTimestamp(long timestamp);

    T build();
}
