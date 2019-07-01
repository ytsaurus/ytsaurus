package ru.yandex.yt.ytclient.object;

import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;

@YTreeObject
public class InternalObject1 {
    private Long longField1;
    private Long longField2;
    private Long longField3;
    private Long longField4;
    private Long longField5;

    public Long getLongField1() {
        return longField1;
    }

    public void setLongField1(Long longField1) {
        this.longField1 = longField1;
    }

    public Long getLongField2() {
        return longField2;
    }

    public void setLongField2(Long longField2) {
        this.longField2 = longField2;
    }

    public Long getLongField3() {
        return longField3;
    }

    public void setLongField3(Long longField3) {
        this.longField3 = longField3;
    }

    public Long getLongField4() {
        return longField4;
    }

    public void setLongField4(Long longField4) {
        this.longField4 = longField4;
    }

    public Long getLongField5() {
        return longField5;
    }

    public void setLongField5(Long longField5) {
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
        InternalObject1 object1 = (InternalObject1) o;
        return Objects.equals(longField1, object1.longField1) &&
                Objects.equals(longField2, object1.longField2) &&
                Objects.equals(longField3, object1.longField3) &&
                Objects.equals(longField4, object1.longField4) &&
                Objects.equals(longField5, object1.longField5);
    }

    @Override
    public int hashCode() {
        return Objects.hash(longField1, longField2, longField3, longField4, longField5);
    }

    @Override
    public String toString() {
        return "InternalObject1{" +
                "longField1=" + longField1 +
                ", longField2=" + longField2 +
                ", longField3=" + longField3 +
                ", longField4=" + longField4 +
                ", longField5=" + longField5 +
                '}';
    }

}
