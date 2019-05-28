package ru.yandex.yt.ytclient.object;

import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeFlattenField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;

@YTreeObject
public class LargeFlattenPrimitiveClass {
    @YTreeField(isKeyField = true)
    private int intField1;
    private int intField2;
    private int intField3;
    private int intField4;
    private int intField5;
    @YTreeFlattenField
    private InternalPrimitive1 object1;
    @YTreeFlattenField
    private InternalFlattenPrimitive2 object2;
    private String stringField1;
    private String stringField2;
    private String stringField3;
    private String stringField4;
    private String stringField5;

    public int getIntField1() {
        return intField1;
    }

    public void setIntField1(int intField1) {
        this.intField1 = intField1;
    }

    public int getIntField2() {
        return intField2;
    }

    public void setIntField2(int intField2) {
        this.intField2 = intField2;
    }

    public int getIntField3() {
        return intField3;
    }

    public void setIntField3(int intField3) {
        this.intField3 = intField3;
    }

    public int getIntField4() {
        return intField4;
    }

    public void setIntField4(int intField4) {
        this.intField4 = intField4;
    }

    public int getIntField5() {
        return intField5;
    }

    public void setIntField5(int intField5) {
        this.intField5 = intField5;
    }

    public InternalPrimitive1 getObject1() {
        return object1;
    }

    public void setObject1(InternalPrimitive1 object1) {
        this.object1 = object1;
    }

    public InternalFlattenPrimitive2 getObject2() {
        return object2;
    }

    public void setObject2(InternalFlattenPrimitive2 object2) {
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
        LargeFlattenPrimitiveClass that = (LargeFlattenPrimitiveClass) o;
        return intField1 == that.intField1 &&
                intField2 == that.intField2 &&
                intField3 == that.intField3 &&
                intField4 == that.intField4 &&
                intField5 == that.intField5 &&
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
        return "LargeFlattenPrimitiveClass{" +
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
