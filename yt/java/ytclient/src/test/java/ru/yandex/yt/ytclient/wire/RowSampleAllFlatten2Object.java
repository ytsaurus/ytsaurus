package ru.yandex.yt.ytclient.wire;

import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.object.NullSerializationStrategy;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;

@YTreeObject(nullSerializationStrategy = NullSerializationStrategy.IGNORE_NULL_FIELDS)
public class RowSampleAllFlatten2Object {
    private String f21;
    private String f22;
    private RowSampleAllInternal3Object internalObject3;

    public String getF21() {
        return f21;
    }

    public void setF21(String f21) {
        this.f21 = f21;
    }

    public String getF22() {
        return f22;
    }

    public void setF22(String f22) {
        this.f22 = f22;
    }

    public RowSampleAllInternal3Object getInternalObject3() {
        return internalObject3;
    }

    public void setInternalObject3(RowSampleAllInternal3Object internalObject3) {
        this.internalObject3 = internalObject3;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowSampleAllFlatten2Object that = (RowSampleAllFlatten2Object) o;
        return Objects.equals(f21, that.f21) &&
                Objects.equals(f22, that.f22) &&
                Objects.equals(internalObject3, that.internalObject3);
    }

    @Override
    public String toString() {
        return "RowSampleAllFlatten2Object{" +
                "f21='" + f21 + '\'' +
                ", f22='" + f22 + '\'' +
                ", internalObject3=" + internalObject3 +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(f21, f22, internalObject3);
    }
}
