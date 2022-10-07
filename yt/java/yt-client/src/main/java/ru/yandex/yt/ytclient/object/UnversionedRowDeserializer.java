package ru.yandex.yt.ytclient.object;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedValue;

public class UnversionedRowDeserializer
        extends UnversionedValueDeserializer
        implements WireRowDeserializer<UnversionedRow> {
    private List<UnversionedValue> values = Collections.emptyList();

    @Override
    @Nonnull
    public WireValueDeserializer<UnversionedValue> onNewRow(int columnCount) {
        this.values = new ArrayList<>(columnCount);
        return this;
    }

    @Override
    @Nonnull
    public UnversionedRow onCompleteRow() {
        return new UnversionedRow(this.values);
    }

    @Override
    public UnversionedRow onNullRow() {
        return null;
    }

    @Override
    public UnversionedValue build() {
        final UnversionedValue value = super.build();
        this.values.add(value);
        return value;
    }
}
