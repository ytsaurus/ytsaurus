package ru.yandex.yt.ytclient.wire;

import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.object.NullSerializationStrategy;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeFlattenField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;

@YTreeObject(nullSerializationStrategy = NullSerializationStrategy.IGNORE_NULL_FIELDS)
public class RowSampleAllFlattenObject {
    private String f1;
    @YTreeFlattenField
    private RowSampleAllFlatten2Object flatten2;
    @YTreeFlattenField
    private RowSampleAllFlatten3Object flatten3;
    private String f2;


    public String getF1() {
        return f1;
    }

    public void setF1(String f1) {
        this.f1 = f1;
    }

    public String getF2() {
        return f2;
    }

    public void setF2(String f2) {
        this.f2 = f2;
    }

    public RowSampleAllFlatten2Object getFlatten2() {
        return flatten2;
    }

    public void setFlatten2(RowSampleAllFlatten2Object flatten2) {
        this.flatten2 = flatten2;
    }

    public RowSampleAllFlatten3Object getFlatten3() {
        return flatten3;
    }

    public void setFlatten3(RowSampleAllFlatten3Object flatten3) {
        this.flatten3 = flatten3;
    }

    @Override
    public String toString() {
        return "RowSampleAllFlattenObject{" +
                "f1='" + f1 + '\'' +
                ", flatten2=" + flatten2 +
                ", flatten3=" + flatten3 +
                ", f2='" + f2 + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowSampleAllFlattenObject that = (RowSampleAllFlattenObject) o;
        return Objects.equals(f1, that.f1) &&
                Objects.equals(flatten2, that.flatten2) &&
                Objects.equals(flatten3, that.flatten3) &&
                Objects.equals(f2, that.f2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(f1, flatten2, flatten3, f2);
    }

}
