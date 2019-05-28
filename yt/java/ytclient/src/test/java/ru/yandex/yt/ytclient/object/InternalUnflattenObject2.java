package ru.yandex.yt.ytclient.object;

import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;

@YTreeObject
public class InternalUnflattenObject2 {
    private Float floatField1;
    private Float floatField2;
    private Float floatField3;
    private Float floatField4;
    private Float floatField5;
    private InternalObject21 object21;
    private Double doubleField1;
    private Double doubleField2;
    private Double doubleField3;
    private Double doubleField4;
    private Double doubleField5;

    public Float getFloatField1() {
        return floatField1;
    }

    public void setFloatField1(Float floatField1) {
        this.floatField1 = floatField1;
    }

    public Float getFloatField2() {
        return floatField2;
    }

    public void setFloatField2(Float floatField2) {
        this.floatField2 = floatField2;
    }

    public Float getFloatField3() {
        return floatField3;
    }

    public void setFloatField3(Float floatField3) {
        this.floatField3 = floatField3;
    }

    public Float getFloatField4() {
        return floatField4;
    }

    public void setFloatField4(Float floatField4) {
        this.floatField4 = floatField4;
    }

    public Float getFloatField5() {
        return floatField5;
    }

    public void setFloatField5(Float floatField5) {
        this.floatField5 = floatField5;
    }

    public InternalObject21 getObject21() {
        return object21;
    }

    public void setObject21(InternalObject21 object21) {
        this.object21 = object21;
    }

    public Double getDoubleField1() {
        return doubleField1;
    }

    public void setDoubleField1(Double doubleField1) {
        this.doubleField1 = doubleField1;
    }

    public Double getDoubleField2() {
        return doubleField2;
    }

    public void setDoubleField2(Double doubleField2) {
        this.doubleField2 = doubleField2;
    }

    public Double getDoubleField3() {
        return doubleField3;
    }

    public void setDoubleField3(Double doubleField3) {
        this.doubleField3 = doubleField3;
    }

    public Double getDoubleField4() {
        return doubleField4;
    }

    public void setDoubleField4(Double doubleField4) {
        this.doubleField4 = doubleField4;
    }

    public Double getDoubleField5() {
        return doubleField5;
    }

    public void setDoubleField5(Double doubleField5) {
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
        InternalUnflattenObject2 that = (InternalUnflattenObject2) o;
        return Objects.equals(floatField1, that.floatField1) &&
                Objects.equals(floatField2, that.floatField2) &&
                Objects.equals(floatField3, that.floatField3) &&
                Objects.equals(floatField4, that.floatField4) &&
                Objects.equals(floatField5, that.floatField5) &&
                Objects.equals(object21, that.object21) &&
                Objects.equals(doubleField1, that.doubleField1) &&
                Objects.equals(doubleField2, that.doubleField2) &&
                Objects.equals(doubleField3, that.doubleField3) &&
                Objects.equals(doubleField4, that.doubleField4) &&
                Objects.equals(doubleField5, that.doubleField5);
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
        return "InternalUnflattenObject2{" +
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
