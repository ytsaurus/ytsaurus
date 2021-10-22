package ru.yandex.yt.ytclient.wire;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import ru.yandex.inside.yt.kosher.impl.ytree.object.NullSerializationStrategy;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeFlattenField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeKeyField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.AbstractYTreeStateSupport;
import ru.yandex.misc.lang.number.UnsignedLong;

@YTreeObject(nullSerializationStrategy = NullSerializationStrategy.IGNORE_NULL_FIELDS)
public class RowSampleAllObject extends AbstractYTreeStateSupport<RowSampleAllObject> {
    @YTreeKeyField
    private int int64_as_int;
    private Integer int64_as_Integer;

    private long int64_as_long;
    private Long int64_as_Long;

    private int uint64_as_int;
    private Integer uint64_as_Integer;

    private long uint64_as_long;
    private Long uint64_as_Long;

    private UnsignedLong uint64_as_UnsignedLong;

    private double double_as_double;
    private Double double_as_Double;

    private boolean boolean_as_boolean;
    private Boolean boolean_as_Boolean;

    private String string_as_string;
    private byte[] string_as_bytes;

    private String any_as_string;
    private byte[] any_as_bytes;

    private RowSampleObject sampleObject;
    private RowSampleAllInternal1Object internalObject;

    @YTreeFlattenField
    private RowSampleAllFlattenObject flatten;

    private Map<String, String> simpleMapObject;
    private Map<String, RowSampleObject> complexMapObject;
    private List<String> simpleListObject;
    private List<RowSampleObject> complexListObject;
    private String[] simpleArrayObject;
    private RowSampleObject[] complexArrayObject;
    private int[] primitiveArrayObject;
    private Set<String> simpleSetObjects;
    private Set<RowSampleObject> complexSetObjects;

    public int getInt64_as_int() {
        return int64_as_int;
    }

    public void setInt64_as_int(int int64_as_int) {
        this.int64_as_int = int64_as_int;
    }

    public Integer getInt64_as_Integer() {
        return int64_as_Integer;
    }

    public void setInt64_as_Integer(Integer int64_as_Integer) {
        this.int64_as_Integer = int64_as_Integer;
    }

    public long getInt64_as_long() {
        return int64_as_long;
    }

    public void setInt64_as_long(long int64_as_long) {
        this.int64_as_long = int64_as_long;
    }

    public Long getInt64_as_Long() {
        return int64_as_Long;
    }

    public void setInt64_as_Long(Long int64_as_Long) {
        this.int64_as_Long = int64_as_Long;
    }

    public int getUint64_as_int() {
        return uint64_as_int;
    }

    public void setUint64_as_int(int uint64_as_int) {
        this.uint64_as_int = uint64_as_int;
    }

    public Integer getUint64_as_Integer() {
        return uint64_as_Integer;
    }

    public void setUint64_as_Integer(Integer uint64_as_Integer) {
        this.uint64_as_Integer = uint64_as_Integer;
    }

    public long getUint64_as_long() {
        return uint64_as_long;
    }

    public void setUint64_as_long(long uint64_as_long) {
        this.uint64_as_long = uint64_as_long;
    }

    public Long getUint64_as_Long() {
        return uint64_as_Long;
    }

    public void setUint64_as_Long(Long uint64_as_Long) {
        this.uint64_as_Long = uint64_as_Long;
    }

    public UnsignedLong getUint64_as_UnsignedLong() {
        return uint64_as_UnsignedLong;
    }

    public void setUint64_as_UnsignedLong(UnsignedLong uint64_as_UnsignedLong) {
        this.uint64_as_UnsignedLong = uint64_as_UnsignedLong;
    }

    public double getDouble_as_double() {
        return double_as_double;
    }

    public void setDouble_as_double(double double_as_double) {
        this.double_as_double = double_as_double;
    }

    public Double getDouble_as_Double() {
        return double_as_Double;
    }

    public void setDouble_as_Double(Double double_as_Double) {
        this.double_as_Double = double_as_Double;
    }

    public boolean isBoolean_as_boolean() {
        return boolean_as_boolean;
    }

    public void setBoolean_as_boolean(boolean boolean_as_boolean) {
        this.boolean_as_boolean = boolean_as_boolean;
    }

    public Boolean getBoolean_as_Boolean() {
        return boolean_as_Boolean;
    }

    public void setBoolean_as_Boolean(Boolean boolean_as_Boolean) {
        this.boolean_as_Boolean = boolean_as_Boolean;
    }

    public String getString_as_string() {
        return string_as_string;
    }

    public void setString_as_string(String string_as_string) {
        this.string_as_string = string_as_string;
    }

    public byte[] getString_as_bytes() {
        return string_as_bytes;
    }

    public void setString_as_bytes(byte[] string_as_bytes) {
        this.string_as_bytes = string_as_bytes;
    }

    public String getAny_as_string() {
        return any_as_string;
    }

    public void setAny_as_string(String any_as_string) {
        this.any_as_string = any_as_string;
    }

    public byte[] getAny_as_bytes() {
        return any_as_bytes;
    }

    public void setAny_as_bytes(byte[] any_as_bytes) {
        this.any_as_bytes = any_as_bytes;
    }

    public RowSampleObject getSampleObject() {
        return sampleObject;
    }

    public void setSampleObject(RowSampleObject sampleObject) {
        this.sampleObject = sampleObject;
    }

    public RowSampleAllInternal1Object getInternalObject() {
        return internalObject;
    }

    public void setInternalObject(RowSampleAllInternal1Object internalObject) {
        this.internalObject = internalObject;
    }

    public Map<String, String> getSimpleMapObject() {
        return simpleMapObject;
    }

    public Map<String, String> getSimpleMapObject0() {
        if (simpleMapObject == null) {
            simpleMapObject = new HashMap<>();
        }
        return simpleMapObject;
    }

    public void setSimpleMapObject(Map<String, String> simpleMapObject) {
        this.simpleMapObject = simpleMapObject;
    }

    public Map<String, RowSampleObject> getComplexMapObject() {
        return complexMapObject;
    }

    public Map<String, RowSampleObject> getComplexMapObject0() {
        if (complexMapObject == null) {
            complexMapObject = new HashMap<>();
        }
        return complexMapObject;
    }

    public void setComplexMapObject(Map<String, RowSampleObject> complexMapObject) {
        this.complexMapObject = complexMapObject;
    }

    public List<String> getSimpleListObject() {
        return simpleListObject;
    }

    public List<String> getSimpleListObject0() {
        if (simpleListObject == null) {
            simpleListObject = new ArrayList<>();
        }
        return simpleListObject;
    }

    public void setSimpleListObject(List<String> simpleListObject) {
        this.simpleListObject = simpleListObject;
    }

    public List<RowSampleObject> getComplexListObject() {
        return complexListObject;
    }

    public List<RowSampleObject> getComplexListObject0() {
        if (complexListObject == null) {
            complexListObject = new ArrayList<>();
        }
        return complexListObject;
    }

    public void setComplexListObject(List<RowSampleObject> complexListObject) {
        this.complexListObject = complexListObject;
    }

    public String[] getSimpleArrayObject() {
        return simpleArrayObject;
    }

    public void setSimpleArrayObject(String... simpleArrayObject) {
        this.simpleArrayObject = simpleArrayObject;
    }

    public RowSampleObject[] getComplexArrayObject() {
        return complexArrayObject;
    }

    public void setComplexArrayObject(RowSampleObject... complexArrayObject) {
        this.complexArrayObject = complexArrayObject;
    }

    public int[] getPrimitiveArrayObject() {
        return primitiveArrayObject;
    }

    public void setPrimitiveArrayObject(int... primitiveArrayObject) {
        this.primitiveArrayObject = primitiveArrayObject;
    }

    public RowSampleAllFlattenObject getFlatten() {
        return flatten;
    }

    public void setFlatten(RowSampleAllFlattenObject flatten) {
        this.flatten = flatten;
    }

    public Set<String> getSimpleSetObjects() {
        return simpleSetObjects;
    }

    public Set<String> getSimpleSetObjects0() {
        if (simpleSetObjects == null) {
            simpleSetObjects = new HashSet<>();
        }
        return simpleSetObjects;
    }

    public void setSimpleSetObjects(Set<String> simpleSetObjects) {
        this.simpleSetObjects = simpleSetObjects;
    }

    public Set<RowSampleObject> getComplexSetObjects() {
        return complexSetObjects;
    }

    public Set<RowSampleObject> getComplexSetObjects0() {
        if (complexSetObjects == null) {
            complexSetObjects = new HashSet<>();
        }
        return complexSetObjects;
    }

    public void setComplexSetObjects(Set<RowSampleObject> complexSetObjects) {
        this.complexSetObjects = complexSetObjects;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowSampleAllObject that = (RowSampleAllObject) o;
        return int64_as_int == that.int64_as_int &&
                int64_as_long == that.int64_as_long &&
                uint64_as_int == that.uint64_as_int &&
                uint64_as_long == that.uint64_as_long &&
                Double.compare(that.double_as_double, double_as_double) == 0 &&
                boolean_as_boolean == that.boolean_as_boolean &&
                Objects.equals(int64_as_Integer, that.int64_as_Integer) &&
                Objects.equals(int64_as_Long, that.int64_as_Long) &&
                Objects.equals(uint64_as_Integer, that.uint64_as_Integer) &&
                Objects.equals(uint64_as_Long, that.uint64_as_Long) &&
                Objects.equals(uint64_as_UnsignedLong, that.uint64_as_UnsignedLong) &&
                Objects.equals(double_as_Double, that.double_as_Double) &&
                Objects.equals(boolean_as_Boolean, that.boolean_as_Boolean) &&
                Objects.equals(string_as_string, that.string_as_string) &&
                Arrays.equals(string_as_bytes, that.string_as_bytes) &&
                Objects.equals(any_as_string, that.any_as_string) &&
                Arrays.equals(any_as_bytes, that.any_as_bytes) &&
                Objects.equals(sampleObject, that.sampleObject) &&
                Objects.equals(internalObject, that.internalObject) &&
                Objects.equals(flatten, that.flatten) &&
                Objects.equals(simpleMapObject, that.simpleMapObject) &&
                Objects.equals(complexMapObject, that.complexMapObject) &&
                Objects.equals(simpleListObject, that.simpleListObject) &&
                Objects.equals(complexListObject, that.complexListObject) &&
                Arrays.equals(simpleArrayObject, that.simpleArrayObject) &&
                Arrays.equals(complexArrayObject, that.complexArrayObject) &&
                Arrays.equals(primitiveArrayObject, that.primitiveArrayObject) &&
                Objects.equals(simpleSetObjects, that.simpleSetObjects) &&
                Objects.equals(complexSetObjects, that.complexSetObjects);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(int64_as_int, int64_as_Integer, int64_as_long, int64_as_Long, uint64_as_int,
                uint64_as_Integer, uint64_as_long, uint64_as_Long, uint64_as_UnsignedLong, double_as_double,
                double_as_Double, boolean_as_boolean, boolean_as_Boolean, string_as_string, any_as_string,
                sampleObject, internalObject, flatten, simpleMapObject, complexMapObject, simpleListObject,
                complexListObject, simpleSetObjects, complexSetObjects);
        result = 31 * result + Arrays.hashCode(string_as_bytes);
        result = 31 * result + Arrays.hashCode(any_as_bytes);
        result = 31 * result + Arrays.hashCode(simpleArrayObject);
        result = 31 * result + Arrays.hashCode(complexArrayObject);
        result = 31 * result + Arrays.hashCode(primitiveArrayObject);
        return result;
    }

    @Override
    public String toString() {
        return "RowSampleAllObject{" +
                "int64_as_int=" + int64_as_int +
                ", int64_as_Integer=" + int64_as_Integer +
                ", int64_as_long=" + int64_as_long +
                ", int64_as_Long=" + int64_as_Long +
                ", uint64_as_int=" + uint64_as_int +
                ", uint64_as_Integer=" + uint64_as_Integer +
                ", uint64_as_long=" + uint64_as_long +
                ", uint64_as_Long=" + uint64_as_Long +
                ", uint64_as_UnsignedLong=" + uint64_as_UnsignedLong +
                ", double_as_double=" + double_as_double +
                ", double_as_Double=" + double_as_Double +
                ", boolean_as_boolean=" + boolean_as_boolean +
                ", boolean_as_Boolean=" + boolean_as_Boolean +
                ", string_as_string='" + string_as_string + '\'' +
                ", string_as_bytes=" + Arrays.toString(string_as_bytes) +
                ", any_as_string='" + any_as_string + '\'' +
                ", any_as_bytes=" + Arrays.toString(any_as_bytes) +
                ", sampleObject=" + sampleObject +
                ", internalObject=" + internalObject +
                ", flatten=" + flatten +
                ", simpleMapObject=" + simpleMapObject +
                ", complexMapObject=" + complexMapObject +
                ", simpleListObject=" + simpleListObject +
                ", complexListObject=" + complexListObject +
                ", simpleArrayObject=" + Arrays.toString(simpleArrayObject) +
                ", complexArrayObject=" + Arrays.toString(complexArrayObject) +
                ", primitiveArrayObject=" + Arrays.toString(primitiveArrayObject) +
                ", simpleSetObjects=" + simpleSetObjects +
                ", complexSetObjects=" + complexSetObjects +
                '}';
    }
}
