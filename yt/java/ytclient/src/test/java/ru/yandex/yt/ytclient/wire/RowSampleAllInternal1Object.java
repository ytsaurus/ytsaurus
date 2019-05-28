package ru.yandex.yt.ytclient.wire;

import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.object.NullSerializationStrategy;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;

@YTreeObject(nullSerializationStrategy = NullSerializationStrategy.IGNORE_NULL_FIELDS)
public class RowSampleAllInternal1Object {
    private int key;
    private RowSampleOject rowSampleOject;
    private RowSampleAllInternal2Object rowInternalObject;

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public RowSampleOject getRowSampleOject() {
        return rowSampleOject;
    }

    public void setRowSampleOject(RowSampleOject rowSampleOject) {
        this.rowSampleOject = rowSampleOject;
    }

    public RowSampleAllInternal2Object getRowInternalObject() {
        return rowInternalObject;
    }

    public void setRowInternalObject(RowSampleAllInternal2Object rowInternalObject) {
        this.rowInternalObject = rowInternalObject;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowSampleAllInternal1Object that = (RowSampleAllInternal1Object) o;
        return key == that.key &&
                Objects.equals(rowSampleOject, that.rowSampleOject) &&
                Objects.equals(rowInternalObject, that.rowInternalObject);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, rowSampleOject, rowInternalObject);
    }

    @Override
    public String toString() {
        return "RowSampleAllInternal1Object{" +
                "key=" + key +
                ", rowSampleOject=" + rowSampleOject +
                ", rowInternalObject=" + rowInternalObject +
                '}';
    }
}
