package ru.yandex.yt.ytclient.object;

import ru.yandex.yt.ytclient.wire.VersionedValue;

public class VersionedValueDeserializer extends AbstractValueDeserializer<VersionedValue> {
    @Override
    public VersionedValue build() {
        return new VersionedValue(id, type, aggregate, value, timestamp);
    }
}
