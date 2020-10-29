package ru.yandex.yt.ytclient.proxy;

import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeKeyField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;

@YTreeObject
public class MappedObject {
    @YTreeKeyField
    private int k1;
    private String v1;
    private long l1;

    public MappedObject() {
        //
    }

    public MappedObject(int k1, String v1) {
        this.k1 = k1;
        this.v1 = v1;
    }

    public MappedObject(int k1, String v1, long l1) {
        this.k1 = k1;
        this.v1 = v1;
        this.l1 = l1;
    }

    public int getK1() {
        return k1;
    }

    public void setK1(int k1) {
        this.k1 = k1;
    }

    public String getV1() {
        return v1;
    }

    public void setV1(String v1) {
        this.v1 = v1;
    }

    public long getL1() {
        return l1;
    }

    public void setL1(long l1) {
        this.l1 = l1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MappedObject)) {
            return false;
        }
        MappedObject that = (MappedObject) o;
        return k1 == that.k1 &&
                l1 == that.l1 &&
                Objects.equals(v1, that.v1);
    }

    @Override
    public int hashCode() {
        return Objects.hash(k1, v1, l1);
    }

    @Override
    public String toString() {
        return "MappedObject{" +
                "k1=" + k1 +
                ", v1='" + v1 + '\'' +
                ", l1=" + l1 +
                '}';
    }
}
