package ru.yandex.yt.ytclient.object;

import ru.yandex.yt.ytclient.wire.UnversionedValue;

public class UnversionedValueDeserializer extends AbstractValueDeserializer<UnversionedValue> {
    @Override
    public UnversionedValue build() {
        return new UnversionedValue(id, type, aggregate, value);
    }
}
