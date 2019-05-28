package ru.yandex.yt.ytclient.object;

import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeFlattenField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;

@YTreeObject
public class InternalFlattenPrimitive2 {
    private float floatField1;
    private float floatField2;
    private float floatField3;
    private float floatField4;
    private float floatField5;
    @YTreeFlattenField
    private InternalPrimitive21 object21;
    private double doubleField1;
    private double doubleField2;
    private double doubleField3;
    private double doubleField4;
    private double doubleField5;


    public float getFloatField1() {
        return floatField1;
    }

    public void setFloatField1(float floatField1) {
        this.floatField1 = floatField1;
    }

    public float getFloatField2() {
        return floatField2;
    }

    public void setFloatField2(float floatField2) {
        this.floatField2 = floatField2;
    }

    public float getFloatField3() {
        return floatField3;
    }

    public void setFloatField3(float floatField3) {
        this.floatField3 = floatField3;
    }

    public float getFloatField4() {
        return floatField4;
    }

    public void setFloatField4(float floatField4) {
        this.floatField4 = floatField4;
    }

    public float getFloatField5() {
        return floatField5;
    }

    public void setFloatField5(float floatField5) {
        this.floatField5 = floatField5;
    }

    public InternalPrimitive21 getObject21() {
        return object21;
    }

    public void setObject21(InternalPrimitive21 object21) {
        this.object21 = object21;
    }

    public double getDoubleField1() {
        return doubleField1;
    }

    public void setDoubleField1(double doubleField1) {
        this.doubleField1 = doubleField1;
    }

    public double getDoubleField2() {
        return doubleField2;
    }

    public void setDoubleField2(double doubleField2) {
        this.doubleField2 = doubleField2;
    }

    public double getDoubleField3() {
        return doubleField3;
    }

    public void setDoubleField3(double doubleField3) {
        this.doubleField3 = doubleField3;
    }

    public double getDoubleField4() {
        return doubleField4;
    }

    public void setDoubleField4(double doubleField4) {
        this.doubleField4 = doubleField4;
    }

    public double getDoubleField5() {
        return doubleField5;
    }

    public void setDoubleField5(double doubleField5) {
        this.doubleField5 = doubleField5;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InternalFlattenPrimitive2 that = (InternalFlattenPrimitive2) o;
        return Float.compare(that.floatField1, floatField1) == 0 &&
                Float.compare(that.floatField2, floatField2) == 0 &&
                Float.compare(that.floatField3, floatField3) == 0 &&
                Float.compare(that.floatField4, floatField4) == 0 &&
                Float.compare(that.floatField5, floatField5) == 0 &&
                Double.compare(that.doubleField1, doubleField1) == 0 &&
                Double.compare(that.doubleField2, doubleField2) == 0 &&
                Double.compare(that.doubleField3, doubleField3) == 0 &&
                Double.compare(that.doubleField4, doubleField4) == 0 &&
                Double.compare(that.doubleField5, doubleField5) == 0 &&
                Objects.equals(object21, that.object21);
    }

    @Override
    public int hashCode() {
        return Objects
                .hash(floatField1, floatField2, floatField3, floatField4, floatField5, object21, doubleField1,
                        doubleField2,
                        doubleField3, doubleField4, doubleField5);
    }

    @Override
    public String toString() {
        return "InternalFlattenPrimitive2{" +
                "floatField1=" + floatField1 +
                ", floatField2=" + floatField2 +
                ", floatField3=" + floatField3 +
                ", floatField4=" + floatField4 +
                ", floatField5=" + floatField5 +
                ", object21=" + object21 +
                ", doubleField1=" + doubleField1 +
                ", doubleField2=" + doubleField2 +
                ", doubleField3=" + doubleField3 +
                ", doubleField4=" + doubleField4 +
                ", doubleField5=" + doubleField5 +
                '}';
    }
}


