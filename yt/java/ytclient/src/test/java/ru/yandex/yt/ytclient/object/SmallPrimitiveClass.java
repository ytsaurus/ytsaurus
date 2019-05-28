package ru.yandex.yt.ytclient.object;

import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;

@YTreeObject
public class SmallPrimitiveClass {
    @YTreeField(isKeyField = true)
    private int intField;
    private long longField;
    private float floatField;
    private double doubleField;
    private boolean booleanField;
    private String stringField;

    public int getIntField() {
        return intField;
    }

    public void setIntField(int intField) {
        this.intField = intField;
    }

    public long getLongField() {
        return longField;
    }

    public void setLongField(long longField) {
        this.longField = longField;
    }

    public float getFloatField() {
        return floatField;
    }

    public void setFloatField(float floatField) {
        this.floatField = floatField;
    }

    public double getDoubleField() {
        return doubleField;
    }

    public void setDoubleField(double doubleField) {
        this.doubleField = doubleField;
    }

    public boolean isBooleanField() {
        return booleanField;
    }

    public void setBooleanField(boolean booleanField) {
        this.booleanField = booleanField;
    }

    public String getStringField() {
        return stringField;
    }

    public void setStringField(String stringField) {
        this.stringField = stringField;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SmallPrimitiveClass that = (SmallPrimitiveClass) o;
        return intField == that.intField &&
                longField == that.longField &&
                Float.compare(that.floatField, floatField) == 0 &&
                Double.compare(that.doubleField, doubleField) == 0 &&
                booleanField == that.booleanField &&
                Objects.equals(stringField, that.stringField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(intField, longField, floatField, doubleField, booleanField, stringField);
    }

    @Override
    public String toString() {
        return "SmallPrimitiveClass{" +
                "intField=" + intField +
                ", longField=" + longField +
                ", floatField=" + floatField +
                ", doubleField=" + doubleField +
                ", booleanField=" + booleanField +
                ", stringField='" + stringField + '\'' +
                '}';
    }
}
