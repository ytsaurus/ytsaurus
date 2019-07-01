package ru.yandex.yt.ytclient.object;

import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeKeyField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.AbstractYTreeStateSupport;

@YTreeObject
public class SmallObjectClassWithStateSupport extends AbstractYTreeStateSupport<SmallObjectClassWithStateSupport> {
    @YTreeKeyField
    private Integer intField;
    private Long longField;
    private Float floatField;
    private Double doubleField;
    private Boolean booleanField;
    private String stringField;

    public Integer getIntField() {
        return intField;
    }

    public void setIntField(Integer intField) {
        this.intField = intField;
    }

    public Long getLongField() {
        return longField;
    }

    public void setLongField(Long longField) {
        this.longField = longField;
    }

    public Float getFloatField() {
        return floatField;
    }

    public void setFloatField(Float floatField) {
        this.floatField = floatField;
    }

    public Double getDoubleField() {
        return doubleField;
    }

    public void setDoubleField(Double doubleField) {
        this.doubleField = doubleField;
    }

    public Boolean getBooleanField() {
        return booleanField;
    }

    public void setBooleanField(Boolean booleanField) {
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
        SmallObjectClassWithStateSupport that = (SmallObjectClassWithStateSupport) o;
        return Objects.equals(intField, that.intField) &&
                Objects.equals(longField, that.longField) &&
                Objects.equals(floatField, that.floatField) &&
                Objects.equals(doubleField, that.doubleField) &&
                Objects.equals(booleanField, that.booleanField) &&
                Objects.equals(stringField, that.stringField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(intField, longField, floatField, doubleField, booleanField, stringField);
    }

    @Override
    public String toString() {
        return "SmallObjectClassWithStateSupport{" +
                "intField=" + intField +
                ", longField=" + longField +
                ", floatField=" + floatField +
                ", doubleField=" + doubleField +
                ", booleanField=" + booleanField +
                ", stringField='" + stringField + '\'' +
                '}';
    }
}
