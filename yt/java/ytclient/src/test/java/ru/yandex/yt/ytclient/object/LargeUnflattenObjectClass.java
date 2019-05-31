package ru.yandex.yt.ytclient.object;

import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeKeyField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;

@YTreeObject
public class LargeUnflattenObjectClass {
    @YTreeKeyField
    private Integer intField1;
    private Integer intField2;
    private Integer intField3;
    private Integer intField4;
    private Integer intField5;
    private InternalObject1 object1;
    private InternalUnflattenObject2 object2;
    private String stringField1;
    private String stringField2;
    private String stringField3;
    private String stringField4;
    private String stringField5;

    public Integer getIntField1() {
        return intField1;
    }

    public void setIntField1(Integer intField1) {
        this.intField1 = intField1;
    }

    public Integer getIntField2() {
        return intField2;
    }

    public void setIntField2(Integer intField2) {
        this.intField2 = intField2;
    }

    public Integer getIntField3() {
        return intField3;
    }

    public void setIntField3(Integer intField3) {
        this.intField3 = intField3;
    }

    public Integer getIntField4() {
        return intField4;
    }

    public void setIntField4(Integer intField4) {
        this.intField4 = intField4;
    }

    public Integer getIntField5() {
        return intField5;
    }

    public void setIntField5(Integer intField5) {
        this.intField5 = intField5;
    }

    public InternalObject1 getObject1() {
        return object1;
    }

    public void setObject1(InternalObject1 object1) {
        this.object1 = object1;
    }

    public InternalUnflattenObject2 getObject2() {
        return object2;
    }

    public void setObject2(InternalUnflattenObject2 object2) {
        this.object2 = object2;
    }

    public String getStringField1() {
        return stringField1;
    }

    public void setStringField1(String stringField1) {
        this.stringField1 = stringField1;
    }

    public String getStringField2() {
        return stringField2;
    }

    public void setStringField2(String stringField2) {
        this.stringField2 = stringField2;
    }

    public String getStringField3() {
        return stringField3;
    }

    public void setStringField3(String stringField3) {
        this.stringField3 = stringField3;
    }

    public String getStringField4() {
        return stringField4;
    }

    public void setStringField4(String stringField4) {
        this.stringField4 = stringField4;
    }

    public String getStringField5() {
        return stringField5;
    }

    public void setStringField5(String stringField5) {
        this.stringField5 = stringField5;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LargeUnflattenObjectClass that = (LargeUnflattenObjectClass) o;
        return Objects.equals(intField1, that.intField1) &&
                Objects.equals(intField2, that.intField2) &&
                Objects.equals(intField3, that.intField3) &&
                Objects.equals(intField4, that.intField4) &&
                Objects.equals(intField5, that.intField5) &&
                Objects.equals(object1, that.object1) &&
                Objects.equals(object2, that.object2) &&
                Objects.equals(stringField1, that.stringField1) &&
                Objects.equals(stringField2, that.stringField2) &&
                Objects.equals(stringField3, that.stringField3) &&
                Objects.equals(stringField4, that.stringField4) &&
                Objects.equals(stringField5, that.stringField5);
    }

    @Override
    public int hashCode() {
        return Objects
                .hash(intField1, intField2, intField3, intField4, intField5, object1, object2, stringField1,
                        stringField2,
                        stringField3, stringField4, stringField5);
    }

    @Override
    public String toString() {
        return "LargeUnflattenObjectClass{" +
                "intField1=" + intField1 +
                ", intField2=" + intField2 +
                ", intField3=" + intField3 +
                ", intField4=" + intField4 +
                ", intField5=" + intField5 +
                ", object1=" + object1 +
                ", object2=" + object2 +
                ", stringField1='" + stringField1 + '\'' +
                ", stringField2='" + stringField2 + '\'' +
                ", stringField3='" + stringField3 + '\'' +
                ", stringField4='" + stringField4 + '\'' +
                ", stringField5='" + stringField5 + '\'' +
                '}';
    }
}
