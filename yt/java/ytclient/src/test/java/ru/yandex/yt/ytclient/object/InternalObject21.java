package ru.yandex.yt.ytclient.object;

import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;

@YTreeObject
public class InternalObject21 {
    private Boolean booleanField1;
    private Boolean booleanField2;
    private Boolean booleanField3;
    private Boolean booleanField4;
    private Boolean booleanField5;

    public Boolean getBooleanField1() {
        return booleanField1;
    }

    public void setBooleanField1(Boolean booleanField1) {
        this.booleanField1 = booleanField1;
    }

    public Boolean getBooleanField2() {
        return booleanField2;
    }

    public void setBooleanField2(Boolean booleanField2) {
        this.booleanField2 = booleanField2;
    }

    public Boolean getBooleanField3() {
        return booleanField3;
    }

    public void setBooleanField3(Boolean booleanField3) {
        this.booleanField3 = booleanField3;
    }

    public Boolean getBooleanField4() {
        return booleanField4;
    }

    public void setBooleanField4(Boolean booleanField4) {
        this.booleanField4 = booleanField4;
    }

    public Boolean getBooleanField5() {
        return booleanField5;
    }

    public void setBooleanField5(Boolean booleanField5) {
        this.booleanField5 = booleanField5;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InternalObject21 object21 = (InternalObject21) o;
        return Objects.equals(booleanField1, object21.booleanField1) &&
                Objects.equals(booleanField2, object21.booleanField2) &&
                Objects.equals(booleanField3, object21.booleanField3) &&
                Objects.equals(booleanField4, object21.booleanField4) &&
                Objects.equals(booleanField5, object21.booleanField5);
    }

    @Override
    public int hashCode() {
        return Objects.hash(booleanField1, booleanField2, booleanField3, booleanField4, booleanField5);
    }

    @Override
    public String toString() {
        return "InternalObject21{" +
                "booleanField1=" + booleanField1 +
                ", booleanField2=" + booleanField2 +
                ", booleanField3=" + booleanField3 +
                ", booleanField4=" + booleanField4 +
                ", booleanField5=" + booleanField5 +
                '}';
    }
}
