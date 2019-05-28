package ru.yandex.yt.ytclient.wire;

import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.object.NullSerializationStrategy;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;

@YTreeObject(nullSerializationStrategy = NullSerializationStrategy.IGNORE_NULL_FIELDS)
public class RowSampleAllFlatten3Object {
    private String f31;
    private String f32;

    public String getF31() {
        return f31;
    }

    public void setF31(String f31) {
        this.f31 = f31;
    }

    public String getF32() {
        return f32;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowSampleAllFlatten3Object that = (RowSampleAllFlatten3Object) o;
        return Objects.equals(f31, that.f31) &&
                Objects.equals(f32, that.f32);
    }

    @Override
    public int hashCode() {
        return Objects.hash(f31, f32);
    }

    @Override
    public String toString() {
        return "RowSampleAllFlatten3Object{" +
                "f31='" + f31 + '\'' +
                ", f32='" + f32 + '\'' +
                '}';
    }

    public void setF32(String f32) {
        this.f32 = f32;
    }
}
