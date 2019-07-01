package ru.yandex.yt.ytclient.object;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeKeyField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.AbstractYTreeStateSupport;

@YTreeObject
public class SmallObjectWithComplexFieldAndStateSupport extends AbstractYTreeStateSupport<SmallObjectWithComplexFieldAndStateSupport> {

    @YTreeKeyField
    private int keyField;
    private List<Map<String, List<SmallPrimitiveClass>>> complexField;

    public int getKeyField() {
        return keyField;
    }

    public void setKeyField(int keyField) {
        this.keyField = keyField;
    }

    public List<Map<String, List<SmallPrimitiveClass>>> getComplexField() {
        return complexField;
    }

    public void setComplexField(List<Map<String, List<SmallPrimitiveClass>>> complexField) {
        this.complexField = complexField;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SmallObjectWithComplexFieldAndStateSupport that = (SmallObjectWithComplexFieldAndStateSupport) o;
        return keyField == that.keyField &&
                Objects.equals(complexField, that.complexField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyField, complexField);
    }

    @Override
    public String toString() {
        return "SmallObjectWithComplexFieldAndStateSupport{" +
                "keyField=" + keyField +
                ", complexField=" + complexField +
                '}';
    }
}

