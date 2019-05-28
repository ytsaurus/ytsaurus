package ru.yandex.yt.ytclient.wire;

import java.util.Arrays;
import java.util.Objects;

import ru.yandex.inside.yt.kosher.impl.ytree.object.NullSerializationStrategy;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeSerializerClass;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeBytesSerializer;
import ru.yandex.misc.lang.number.UnsignedLong;

@YTreeObject(nullSerializationStrategy = NullSerializationStrategy.IGNORE_NULL_FIELDS)
public class RowSampleOject {
    @YTreeField(isKeyField = true)
    @YTreeSerializerClass(YTreeBytesSerializer.class)
    private Object vNull;
    @YTreeField(isKeyField = true)
    @YTreeSerializerClass(YTreeBytesSerializer.class)
    private Object vNullAggr;
    private long vInt64;
    private long vInt64Aggr;
    private UnsignedLong vUint64;
    private UnsignedLong vUint64Aggr;
    private double vDouble;
    private double vDoubleAggr;
    private boolean vBoolean;
    private boolean vBooleanAggr;
    private String vString;
    private String vStringAggr;
    private byte[] vAny;
    private byte[] vAnyAggr;

    public Object getvNull() {
        return vNull;
    }

    public void setvNull(Object vNull) {
        this.vNull = vNull;
    }

    public Object getvNullAggr() {
        return vNullAggr;
    }

    public void setvNullAggr(Object vNullAggr) {
        this.vNullAggr = vNullAggr;
    }

    public long getvInt64() {
        return vInt64;
    }

    public void setvInt64(long vInt64) {
        this.vInt64 = vInt64;
    }

    public long getvInt64Aggr() {
        return vInt64Aggr;
    }

    public void setvInt64Aggr(long vInt64Aggr) {
        this.vInt64Aggr = vInt64Aggr;
    }

    public UnsignedLong getvUint64() {
        return vUint64;
    }

    public void setvUint64(UnsignedLong vUint64) {
        this.vUint64 = vUint64;
    }

    public UnsignedLong getvUint64Aggr() {
        return vUint64Aggr;
    }

    public void setvUint64Aggr(UnsignedLong vUint64Aggr) {
        this.vUint64Aggr = vUint64Aggr;
    }

    public double getvDouble() {
        return vDouble;
    }

    public void setvDouble(double vDouble) {
        this.vDouble = vDouble;
    }

    public double getvDoubleAggr() {
        return vDoubleAggr;
    }

    public void setvDoubleAggr(double vDoubleAggr) {
        this.vDoubleAggr = vDoubleAggr;
    }

    public boolean isvBoolean() {
        return vBoolean;
    }

    public void setvBoolean(boolean vBoolean) {
        this.vBoolean = vBoolean;
    }

    public boolean isvBooleanAggr() {
        return vBooleanAggr;
    }

    public void setvBooleanAggr(boolean vBooleanAggr) {
        this.vBooleanAggr = vBooleanAggr;
    }

    public String getvString() {
        return vString;
    }

    public void setvString(String vString) {
        this.vString = vString;
    }

    public String getvStringAggr() {
        return vStringAggr;
    }

    public void setvStringAggr(String vStringAggr) {
        this.vStringAggr = vStringAggr;
    }

    public Object getvAny() {
        return vAny;
    }

    public void setvAny(byte[] vAny) {
        this.vAny = vAny;
    }

    public Object getvAnyAggr() {
        return vAnyAggr;
    }

    public void setvAnyAggr(byte[] vAnyAggr) {
        this.vAnyAggr = vAnyAggr;
    }

    @Override
    public String toString() {
        return "RowSampleOject{" +
                "vNull=" + vNull +
                ", vNullAggr=" + vNullAggr +
                ", vInt64=" + vInt64 +
                ", vInt64Aggr=" + vInt64Aggr +
                ", vUint64=" + vUint64 +
                ", vUint64Aggr=" + vUint64Aggr +
                ", vDouble=" + vDouble +
                ", vDoubleAggr=" + vDoubleAggr +
                ", vBoolean=" + vBoolean +
                ", vBooleanAggr=" + vBooleanAggr +
                ", vString='" + vString + '\'' +
                ", vStringAggr='" + vStringAggr + '\'' +
                ", vAny=" + Arrays.toString(vAny) +
                ", vAnyAggr=" + Arrays.toString(vAnyAggr) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowSampleOject that = (RowSampleOject) o;
        return vInt64 == that.vInt64 &&
                vInt64Aggr == that.vInt64Aggr &&
                Double.compare(that.vDouble, vDouble) == 0 &&
                Double.compare(that.vDoubleAggr, vDoubleAggr) == 0 &&
                vBoolean == that.vBoolean &&
                vBooleanAggr == that.vBooleanAggr &&
                Objects.equals(vNull, that.vNull) &&
                Objects.equals(vNullAggr, that.vNullAggr) &&
                Objects.equals(vUint64, that.vUint64) &&
                Objects.equals(vUint64Aggr, that.vUint64Aggr) &&
                Objects.equals(vString, that.vString) &&
                Objects.equals(vStringAggr, that.vStringAggr) &&
                Arrays.equals(vAny, that.vAny) &&
                Arrays.equals(vAnyAggr, that.vAnyAggr);
    }

    @Override
    public int hashCode() {
        int result =
                Objects.hash(vNull, vNullAggr, vInt64, vInt64Aggr, vUint64, vUint64Aggr, vDouble, vDoubleAggr, vBoolean,
                        vBooleanAggr, vString, vStringAggr);
        result = 31 * result + Arrays.hashCode(vAny);
        result = 31 * result + Arrays.hashCode(vAnyAggr);
        return result;
    }
}
