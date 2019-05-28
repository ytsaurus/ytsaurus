package ru.yandex.yt.ytclient.object;

import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeConsumable;
import ru.yandex.yt.ytclient.tables.ColumnValueType;

public interface WireProtocolWriteable extends YTreeConsumable {

    void writeValueCount(int valueCount);

    void writeValueHeader(int columnId, ColumnValueType type, boolean aggregate, int length);

    void overwriteValueCount(int valueCount);

}
