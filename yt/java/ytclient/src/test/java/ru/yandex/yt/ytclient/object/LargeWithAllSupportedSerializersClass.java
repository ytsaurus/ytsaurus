package ru.yandex.yt.ytclient.object;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeFlattenField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeKeyField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeSerializerClass;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeDateSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeDateTimeSerializer;
import ru.yandex.misc.enums.SerializedAsIntEnum;
import ru.yandex.misc.lang.number.UnsignedLong;

@YTreeObject
public class LargeWithAllSupportedSerializersClass {

    public enum StringEnum implements ru.yandex.misc.enums.StringEnum {
        VS1("value1"), VS2("value2"), VS3("value3");

        private final String value;

        StringEnum(String value) {
            this.value = Objects.requireNonNull(value);
        }

        @Override
        public String value() {
            return value;
        }
    }

    public enum IntEnum implements ru.yandex.misc.enums.IntEnum {
        VI1(1), VI2(2), VI3(3);

        private final int value;

        IntEnum(int value) {
            this.value = value;
        }

        @Override
        public int value() {
            return value;
        }
    }

    public enum IntEnumSerialized implements ru.yandex.misc.enums.IntEnum, SerializedAsIntEnum {
        VIS1(1), VIS2(2), VIS3(3);

        private final int value;

        IntEnumSerialized(int value) {
            this.value = value;
        }

        @Override
        public int value() {
            return value;
        }
    }

    private byte[] bytesValues;
    private UnsignedLong unsignedLongValue;

    @YTreeKeyField
    private long longValue;

    private Long longObjectValue;
    private Instant instantValue;
    private java.time.Instant javaInstantValue;
    private Duration durationValue;
    private List<Long> longListValue;
    private List<Duration> durationListValue;
    private List<InternalObject1> internalObjectListValue;
    @YTreeSerializerClass(YTreeCustomListSerializer.class)
    private List<Long> longListValueCustom;
    @YTreeSerializerClass(YTreeCustomListSerializer.class)
    private List<Duration> durationListValueCustom;
    @YTreeSerializerClass(YTreeCustomListSerializer.class)
    private List<InternalObject1> internalObjectListValueCustom;
    private StringEnum stringEnumValue;
    private Set<Long> longSetValue;
    private Set<Duration> durationSetValue;
    private Set<InternalObject1> internalObjectSetValue;
    @YTreeSerializerClass(YTreeCustomSetSerializer.class)
    private Set<Long> longSetValueCustom;
    @YTreeSerializerClass(YTreeCustomSetSerializer.class)
    private Set<Duration> durationSetValueCustom;
    @YTreeSerializerClass(YTreeCustomSetSerializer.class)
    private Set<InternalObject1> internalObjectSetValueCustom;
    private boolean booleanValue;
    private Boolean booleanObjectValue;
    private LocalDateTime localDateTimeValue;
    private IntEnum intEnumValue;
    private IntEnumSerialized intEnumSerialized;
    private Map<String, Long> longMapValue;
    private Map<String, Duration> durationMapValue;
    private Map<String, InternalObject1> internalObjectMapValue;
    @YTreeSerializerClass(YTreeCustomMapSerializer.class)
    private Map<String, Long> longMapValueCustom;
    @YTreeSerializerClass(YTreeCustomMapSerializer.class)
    private Map<String, Duration> durationMapValueCustom;
    @YTreeSerializerClass(YTreeCustomMapSerializer.class)
    private Map<String, InternalObject1> internalObjectMapValueCustom;
    private InternalObject1 internalObject;

    @YTreeFlattenField
    private InternalObject1 internalFlattenObject;

    private String stringValue;
    private double doubleValue;
    private Double doubleObjectValue;
    private int integerValue;
    private Integer integerObjectValue;

    @YTreeSerializerClass(YTreeDateTimeSerializer.class)
    private DateTime dateTimeValueDateTimeSerializer;

    @YTreeSerializerClass(YTreeDateSerializer.class)
    private DateTime dateTimeValueDateSerializer;

    private long[] longArrayValue;
    private Long[] longObjectArrayValue;
    private Duration[] durationArrayValue;
    private InternalObject1[] internalObjectArrayValue;

    private float floatValue;
    private Float floatObjectValue;

    public byte[] getBytesValues() {
        return bytesValues;
    }

    public void setBytesValues(byte[] bytesValues) {
        this.bytesValues = bytesValues;
    }

    public UnsignedLong getUnsignedLongValue() {
        return unsignedLongValue;
    }

    public void setUnsignedLongValue(UnsignedLong unsignedLongValue) {
        this.unsignedLongValue = unsignedLongValue;
    }

    public long getLongValue() {
        return longValue;
    }

    public void setLongValue(long longValue) {
        this.longValue = longValue;
    }

    public Long getLongObjectValue() {
        return longObjectValue;
    }

    public void setLongObjectValue(Long longObjectValue) {
        this.longObjectValue = longObjectValue;
    }

    public Instant getInstantValue() {
        return instantValue;
    }

    public void setInstantValue(Instant instantValue) {
        this.instantValue = instantValue;
    }

    public java.time.Instant getJavaInstantValue() {
        return javaInstantValue;
    }

    public void setJavaInstantValue(java.time.Instant javaInstantValue) {
        this.javaInstantValue = javaInstantValue;
    }

    public Duration getDurationValue() {
        return durationValue;
    }

    public void setDurationValue(Duration durationValue) {
        this.durationValue = durationValue;
    }

    public List<Long> getLongListValue() {
        return longListValue;
    }

    public void setLongListValue(List<Long> longListValue) {
        this.longListValue = longListValue;
    }

    public List<Duration> getDurationListValue() {
        return durationListValue;
    }

    public void setDurationListValue(List<Duration> durationListValue) {
        this.durationListValue = durationListValue;
    }

    public List<InternalObject1> getInternalObjectListValue() {
        return internalObjectListValue;
    }

    public void setInternalObjectListValue(List<InternalObject1> internalObjectListValue) {
        this.internalObjectListValue = internalObjectListValue;
    }

    public List<Long> getLongListValueCustom() {
        return longListValueCustom;
    }

    public void setLongListValueCustom(List<Long> longListValueCustom) {
        this.longListValueCustom = longListValueCustom;
    }

    public List<Duration> getDurationListValueCustom() {
        return durationListValueCustom;
    }

    public void setDurationListValueCustom(List<Duration> durationListValueCustom) {
        this.durationListValueCustom = durationListValueCustom;
    }

    public List<InternalObject1> getInternalObjectListValueCustom() {
        return internalObjectListValueCustom;
    }

    public void setInternalObjectListValueCustom(List<InternalObject1> internalObjectListValueCustom) {
        this.internalObjectListValueCustom = internalObjectListValueCustom;
    }

    public StringEnum getStringEnumValue() {
        return stringEnumValue;
    }

    public void setStringEnumValue(StringEnum stringEnumValue) {
        this.stringEnumValue = stringEnumValue;
    }

    public Set<Long> getLongSetValue() {
        return longSetValue;
    }

    public void setLongSetValue(Set<Long> longSetValue) {
        this.longSetValue = longSetValue;
    }

    public Set<Duration> getDurationSetValue() {
        return durationSetValue;
    }

    public void setDurationSetValue(Set<Duration> durationSetValue) {
        this.durationSetValue = durationSetValue;
    }

    public Set<InternalObject1> getInternalObjectSetValue() {
        return internalObjectSetValue;
    }

    public void setInternalObjectSetValue(Set<InternalObject1> internalObjectSetValue) {
        this.internalObjectSetValue = internalObjectSetValue;
    }

    public Set<Long> getLongSetValueCustom() {
        return longSetValueCustom;
    }

    public void setLongSetValueCustom(Set<Long> longSetValueCustom) {
        this.longSetValueCustom = longSetValueCustom;
    }

    public Set<Duration> getDurationSetValueCustom() {
        return durationSetValueCustom;
    }

    public void setDurationSetValueCustom(Set<Duration> durationSetValueCustom) {
        this.durationSetValueCustom = durationSetValueCustom;
    }

    public Set<InternalObject1> getInternalObjectSetValueCustom() {
        return internalObjectSetValueCustom;
    }

    public void setInternalObjectSetValueCustom(Set<InternalObject1> internalObjectSetValueCustom) {
        this.internalObjectSetValueCustom = internalObjectSetValueCustom;
    }

    public boolean isBooleanValue() {
        return booleanValue;
    }

    public void setBooleanValue(boolean booleanValue) {
        this.booleanValue = booleanValue;
    }

    public Boolean getBooleanObjectValue() {
        return booleanObjectValue;
    }

    public void setBooleanObjectValue(Boolean booleanObjectValue) {
        this.booleanObjectValue = booleanObjectValue;
    }

    public LocalDateTime getLocalDateTimeValue() {
        return localDateTimeValue;
    }

    public void setLocalDateTimeValue(LocalDateTime localDateTimeValue) {
        this.localDateTimeValue = localDateTimeValue;
    }

    public IntEnum getIntEnumValue() {
        return intEnumValue;
    }

    public void setIntEnumValue(IntEnum intEnumValue) {
        this.intEnumValue = intEnumValue;
    }

    public IntEnumSerialized getIntEnumSerialized() {
        return intEnumSerialized;
    }

    public void setIntEnumSerialized(IntEnumSerialized intEnumSerialized) {
        this.intEnumSerialized = intEnumSerialized;
    }

    public Map<String, Long> getLongMapValue() {
        return longMapValue;
    }

    public void setLongMapValue(Map<String, Long> longMapValue) {
        this.longMapValue = longMapValue;
    }

    public Map<String, Duration> getDurationMapValue() {
        return durationMapValue;
    }

    public void setDurationMapValue(Map<String, Duration> durationMapValue) {
        this.durationMapValue = durationMapValue;
    }

    public Map<String, InternalObject1> getInternalObjectMapValue() {
        return internalObjectMapValue;
    }

    public void setInternalObjectMapValue(Map<String, InternalObject1> internalObjectMapValue) {
        this.internalObjectMapValue = internalObjectMapValue;
    }

    public Map<String, Long> getLongMapValueCustom() {
        return longMapValueCustom;
    }

    public void setLongMapValueCustom(Map<String, Long> longMapValueCustom) {
        this.longMapValueCustom = longMapValueCustom;
    }

    public Map<String, Duration> getDurationMapValueCustom() {
        return durationMapValueCustom;
    }

    public void setDurationMapValueCustom(Map<String, Duration> durationMapValueCustom) {
        this.durationMapValueCustom = durationMapValueCustom;
    }

    public Map<String, InternalObject1> getInternalObjectMapValueCustom() {
        return internalObjectMapValueCustom;
    }

    public void setInternalObjectMapValueCustom(Map<String, InternalObject1> internalObjectMapValueCustom) {
        this.internalObjectMapValueCustom = internalObjectMapValueCustom;
    }

    public InternalObject1 getInternalObject() {
        return internalObject;
    }

    public void setInternalObject(InternalObject1 internalObject) {
        this.internalObject = internalObject;
    }

    public InternalObject1 getInternalFlattenObject() {
        return internalFlattenObject;
    }

    public void setInternalFlattenObject(InternalObject1 internalFlattenObject) {
        this.internalFlattenObject = internalFlattenObject;
    }

    public String getStringValue() {
        return stringValue;
    }

    public void setStringValue(String stringValue) {
        this.stringValue = stringValue;
    }

    public double getDoubleValue() {
        return doubleValue;
    }

    public void setDoubleValue(double doubleValue) {
        this.doubleValue = doubleValue;
    }

    public Double getDoubleObjectValue() {
        return doubleObjectValue;
    }

    public void setDoubleObjectValue(Double doubleObjectValue) {
        this.doubleObjectValue = doubleObjectValue;
    }

    public int getIntegerValue() {
        return integerValue;
    }

    public void setIntegerValue(int integerValue) {
        this.integerValue = integerValue;
    }

    public Integer getIntegerObjectValue() {
        return integerObjectValue;
    }

    public void setIntegerObjectValue(Integer integerObjectValue) {
        this.integerObjectValue = integerObjectValue;
    }

    public DateTime getDateTimeValueDateTimeSerializer() {
        return dateTimeValueDateTimeSerializer;
    }

    public void setDateTimeValueDateTimeSerializer(DateTime dateTimeValueDateTimeSerializer) {
        this.dateTimeValueDateTimeSerializer = dateTimeValueDateTimeSerializer;
    }

    public DateTime getDateTimeValueDateSerializer() {
        return dateTimeValueDateSerializer;
    }

    public void setDateTimeValueDateSerializer(DateTime dateTimeValueDateSerializer) {
        this.dateTimeValueDateSerializer = dateTimeValueDateSerializer;
    }

    public long[] getLongArrayValue() {
        return longArrayValue;
    }

    public void setLongArrayValue(long[] longArrayValue) {
        this.longArrayValue = longArrayValue;
    }

    public Long[] getLongObjectArrayValue() {
        return longObjectArrayValue;
    }

    public void setLongObjectArrayValue(Long[] longObjectArrayValue) {
        this.longObjectArrayValue = longObjectArrayValue;
    }

    public Duration[] getDurationArrayValue() {
        return durationArrayValue;
    }

    public void setDurationArrayValue(Duration[] durationArrayValue) {
        this.durationArrayValue = durationArrayValue;
    }

    public InternalObject1[] getInternalObjectArrayValue() {
        return internalObjectArrayValue;
    }

    public void setInternalObjectArrayValue(InternalObject1[] internalObjectArrayValue) {
        this.internalObjectArrayValue = internalObjectArrayValue;
    }

    public float getFloatValue() {
        return floatValue;
    }

    public void setFloatValue(float floatValue) {
        this.floatValue = floatValue;
    }

    public Float getFloatObjectValue() {
        return floatObjectValue;
    }

    public void setFloatObjectValue(Float floatObjectValue) {
        this.floatObjectValue = floatObjectValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LargeWithAllSupportedSerializersClass)) {
            return false;
        }
        LargeWithAllSupportedSerializersClass that = (LargeWithAllSupportedSerializersClass) o;
        return longValue == that.longValue &&
                booleanValue == that.booleanValue &&
                Double.compare(that.doubleValue, doubleValue) == 0 &&
                integerValue == that.integerValue &&
                Float.compare(that.floatValue, floatValue) == 0 &&
                Arrays.equals(bytesValues, that.bytesValues) &&
                Objects.equals(unsignedLongValue, that.unsignedLongValue) &&
                Objects.equals(longObjectValue, that.longObjectValue) &&
                Objects.equals(instantValue, that.instantValue) &&
                Objects.equals(javaInstantValue, that.javaInstantValue) &&
                Objects.equals(durationValue, that.durationValue) &&
                Objects.equals(longListValue, that.longListValue) &&
                Objects.equals(durationListValue, that.durationListValue) &&
                Objects.equals(internalObjectListValue, that.internalObjectListValue) &&
                Objects.equals(longListValueCustom, that.longListValueCustom) &&
                Objects.equals(durationListValueCustom, that.durationListValueCustom) &&
                Objects.equals(internalObjectListValueCustom, that.internalObjectListValueCustom) &&
                stringEnumValue == that.stringEnumValue &&
                Objects.equals(longSetValue, that.longSetValue) &&
                Objects.equals(durationSetValue, that.durationSetValue) &&
                Objects.equals(internalObjectSetValue, that.internalObjectSetValue) &&
                Objects.equals(longSetValueCustom, that.longSetValueCustom) &&
                Objects.equals(durationSetValueCustom, that.durationSetValueCustom) &&
                Objects.equals(internalObjectSetValueCustom, that.internalObjectSetValueCustom) &&
                Objects.equals(booleanObjectValue, that.booleanObjectValue) &&
                Objects.equals(localDateTimeValue, that.localDateTimeValue) &&
                intEnumValue == that.intEnumValue &&
                intEnumSerialized == that.intEnumSerialized &&
                Objects.equals(longMapValue, that.longMapValue) &&
                Objects.equals(durationMapValue, that.durationMapValue) &&
                Objects.equals(internalObjectMapValue, that.internalObjectMapValue) &&
                Objects.equals(longMapValueCustom, that.longMapValueCustom) &&
                Objects.equals(durationMapValueCustom, that.durationMapValueCustom) &&
                Objects.equals(internalObjectMapValueCustom, that.internalObjectMapValueCustom) &&
                Objects.equals(internalObject, that.internalObject) &&
                Objects.equals(internalFlattenObject, that.internalFlattenObject) &&
                Objects.equals(stringValue, that.stringValue) &&
                Objects.equals(doubleObjectValue, that.doubleObjectValue) &&
                Objects.equals(integerObjectValue, that.integerObjectValue) &&
                Objects.equals(dateTimeValueDateTimeSerializer, that.dateTimeValueDateTimeSerializer) &&
                Objects.equals(dateTimeValueDateSerializer, that.dateTimeValueDateSerializer) &&
                Arrays.equals(longArrayValue, that.longArrayValue) &&
                Arrays.equals(longObjectArrayValue, that.longObjectArrayValue) &&
                Arrays.equals(durationArrayValue, that.durationArrayValue) &&
                Arrays.equals(internalObjectArrayValue, that.internalObjectArrayValue) &&
                Objects.equals(floatObjectValue, that.floatObjectValue);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(unsignedLongValue, longValue, longObjectValue, instantValue, javaInstantValue,
                durationValue, longListValue, durationListValue, internalObjectListValue, longListValueCustom,
                durationListValueCustom, internalObjectListValueCustom, stringEnumValue, longSetValue,
                durationSetValue, internalObjectSetValue, longSetValueCustom, durationSetValueCustom,
                internalObjectSetValueCustom, booleanValue, booleanObjectValue, localDateTimeValue, intEnumValue,
                intEnumSerialized, longMapValue, durationMapValue, internalObjectMapValue, longMapValueCustom,
                durationMapValueCustom, internalObjectMapValueCustom, internalObject, internalFlattenObject,
                stringValue, doubleValue, doubleObjectValue, integerValue, integerObjectValue,
                dateTimeValueDateTimeSerializer, dateTimeValueDateSerializer, floatValue, floatObjectValue);
        result = 31 * result + Arrays.hashCode(bytesValues);
        result = 31 * result + Arrays.hashCode(longArrayValue);
        result = 31 * result + Arrays.hashCode(longObjectArrayValue);
        result = 31 * result + Arrays.hashCode(durationArrayValue);
        result = 31 * result + Arrays.hashCode(internalObjectArrayValue);
        return result;
    }

    @Override
    public String toString() {
        return "LargeWithAllSupportedSerializersClass{" +
                "bytesValues=" + Arrays.toString(bytesValues) +
                ", unsignedLongValue=" + unsignedLongValue +
                ", longValue=" + longValue +
                ", longObjectValue=" + longObjectValue +
                ", instantValue=" + instantValue +
                ", javaInstantValue=" + javaInstantValue +
                ", durationValue=" + durationValue +
                ", longListValue=" + longListValue +
                ", durationListValue=" + durationListValue +
                ", internalObjectListValue=" + internalObjectListValue +
                ", longListValueCustom=" + longListValueCustom +
                ", durationListValueCustom=" + durationListValueCustom +
                ", internalObjectListValueCustom=" + internalObjectListValueCustom +
                ", stringEnumValue=" + stringEnumValue +
                ", longSetValue=" + longSetValue +
                ", durationSetValue=" + durationSetValue +
                ", internalObjectSetValue=" + internalObjectSetValue +
                ", longSetValueCustom=" + longSetValueCustom +
                ", durationSetValueCustom=" + durationSetValueCustom +
                ", internalObjectSetValueCustom=" + internalObjectSetValueCustom +
                ", booleanValue=" + booleanValue +
                ", booleanObjectValue=" + booleanObjectValue +
                ", localDateTimeValue=" + localDateTimeValue +
                ", intEnumValue=" + intEnumValue +
                ", intEnumSerialized=" + intEnumSerialized +
                ", longMapValue=" + longMapValue +
                ", durationMapValue=" + durationMapValue +
                ", internalObjectMapValue=" + internalObjectMapValue +
                ", longMapValueCustom=" + longMapValueCustom +
                ", durationMapValueCustom=" + durationMapValueCustom +
                ", internalObjectMapValueCustom=" + internalObjectMapValueCustom +
                ", internalObject=" + internalObject +
                ", internalFlattenObject=" + internalFlattenObject +
                ", stringValue='" + stringValue + '\'' +
                ", doubleValue=" + doubleValue +
                ", doubleObjectValue=" + doubleObjectValue +
                ", integerValue=" + integerValue +
                ", integerObjectValue=" + integerObjectValue +
                ", dateTimeValueDateTimeSerializer=" + dateTimeValueDateTimeSerializer +
                ", dateTimeValueDateSerializer=" + dateTimeValueDateSerializer +
                ", longArrayValue=" + Arrays.toString(longArrayValue) +
                ", longObjectArrayValue=" + Arrays.toString(longObjectArrayValue) +
                ", durationArrayValue=" + Arrays.toString(durationArrayValue) +
                ", internalObjectArrayValue=" + Arrays.toString(internalObjectArrayValue) +
                ", floatValue=" + floatValue +
                ", floatObjectValue=" + floatObjectValue +
                '}';
    }
}
