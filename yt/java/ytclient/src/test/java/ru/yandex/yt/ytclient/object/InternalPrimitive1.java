package ru.yandex.yt.ytclient.object;

import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;

@YTreeObject
public class InternalPrimitive1 {
    private long longField1;
    private long longField2;
    private long longField3;
    private long longField4;
    private long longField5;

    public long getLongField1() {
        return longField1;
    }

    public void setLongField1(long longField1) {
        this.longField1 = longField1;
    }

    public long getLongField2() {
        return longField2;
    }

    public void setLongField2(long longField2) {
        this.longField2 = longField2;
    }

    public long getLongField3() {
        return longField3;
    }

    public void setLongField3(long longField3) {
        this.longField3 = longField3;
    }

    public long getLongField4() {
        return longField4;
    }

    public void setLongField4(long longField4) {
        this.longField4 = longField4;
    }

    public long getLongField5() {
        return longField5;
    }

    public void setLongField5(long longField5) {
        this.longField5 = longField5;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InternalPrimitive1 that = (InternalPrimitive1) o;
        return longField1 == that.longField1 &&
                longField2 == that.longField2 &&
                longField3 == that.longField3 &&
                longField4 == that.longField4 &&
                longField5 == that.longField5;
    }

    @Override
    public int hashCode() {
        return Objects.hash(longField1, longField2, longField3, longField4, longField5);
    }

    @Override
    public String toString() {
        return "InternalPrimitive1{" +
                "longField1=" + longField1 +
                ", longField2=" + longField2 +
                ", longField3=" + longField3 +
                ", longField4=" + longField4 +
                ", longField5=" + longField5 +
                '}';
    }
}
