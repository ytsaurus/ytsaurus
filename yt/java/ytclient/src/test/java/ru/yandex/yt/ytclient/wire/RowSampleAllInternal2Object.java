package ru.yandex.yt.ytclient.wire;

import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.object.NullSerializationStrategy;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeFlattenField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;

@YTreeObject(nullSerializationStrategy = NullSerializationStrategy.IGNORE_NULL_FIELDS)
public class RowSampleAllInternal2Object {

    private int key;
    private RowSampleOject rowSampleOject;
    @YTreeFlattenField
    private RowSampleAllFlattenObject flattenObject;

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

    public RowSampleAllFlattenObject getFlattenObject() {
        return flattenObject;
    }

    public void setFlattenObject(RowSampleAllFlattenObject flattenObject) {
        this.flattenObject = flattenObject;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowSampleAllInternal2Object that = (RowSampleAllInternal2Object) o;
        return key == that.key &&
                Objects.equals(rowSampleOject, that.rowSampleOject) &&
                Objects.equals(flattenObject, that.flattenObject);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, rowSampleOject, flattenObject);
    }

    @Override
    public String toString() {
        return "RowSampleAllInternal2Object{" +
                "key=" + key +
                ", rowSampleOject=" + rowSampleOject +
                ", flattenObject=" + flattenObject +
                '}';
    }
}
