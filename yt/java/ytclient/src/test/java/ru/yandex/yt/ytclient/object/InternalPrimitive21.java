package ru.yandex.yt.ytclient.object;

import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;

@YTreeObject
public class InternalPrimitive21 {
    private boolean booleanField1;
    private boolean booleanField2;
    private boolean booleanField3;
    private boolean booleanField4;
    private boolean booleanField5;

    public boolean isBooleanField1() {
        return booleanField1;
    }

    public void setBooleanField1(boolean booleanField1) {
        this.booleanField1 = booleanField1;
    }

    public boolean isBooleanField2() {
        return booleanField2;
    }

    public void setBooleanField2(boolean booleanField2) {
        this.booleanField2 = booleanField2;
    }

    public boolean isBooleanField3() {
        return booleanField3;
    }

    public void setBooleanField3(boolean booleanField3) {
        this.booleanField3 = booleanField3;
    }

    public boolean isBooleanField4() {
        return booleanField4;
    }

    public void setBooleanField4(boolean booleanField4) {
        this.booleanField4 = booleanField4;
    }

    public boolean isBooleanField5() {
        return booleanField5;
    }

    public void setBooleanField5(boolean booleanField5) {
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
        InternalPrimitive21 that = (InternalPrimitive21) o;
        return booleanField1 == that.booleanField1 &&
                booleanField2 == that.booleanField2 &&
                booleanField3 == that.booleanField3 &&
                booleanField4 == that.booleanField4 &&
                booleanField5 == that.booleanField5;
    }

    @Override
    public int hashCode() {
        return Objects.hash(booleanField1, booleanField2, booleanField3, booleanField4, booleanField5);
    }

    @Override
    public String toString() {
        return "InternalPrimitive21{" +
                "booleanField1=" + booleanField1 +
                ", booleanField2=" + booleanField2 +
                ", booleanField3=" + booleanField3 +
                ", booleanField4=" + booleanField4 +
                ", booleanField5=" + booleanField5 +
                '}';
    }
}
