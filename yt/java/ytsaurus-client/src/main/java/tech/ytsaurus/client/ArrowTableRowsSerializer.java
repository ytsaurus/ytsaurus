package tech.ytsaurus.client;

import io.netty.buffer.ByteBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import tech.ytsaurus.rpcproxy.ERowsetFormat;
import tech.ytsaurus.rpcproxy.TRowsetDescriptor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ArrowTableRowsSerializer<Struct, List, Dict, Getters extends tech.ytsaurus.core.rows.Getters<Struct, List, Dict>> extends TableRowsSerializer<Struct> implements AutoCloseable {
    private static final RootAllocator ROOT_ALLOCATOR = new RootAllocator(Long.MAX_VALUE);

    private abstract class ArrowGetterFromStruct {
        public final Field field;
        public final ArrowType arrowType;

        ArrowGetterFromStruct(Field field) {
            super();
            this.field = field;
            this.arrowType = field.getType();
        }

        public final ArrowType getArrowType() {
            return arrowType;
        }

        public abstract ArrowWriterFromStruct writer(ValueVector valueVector);
    }

    private abstract class ArrowWriterFromStruct {
        abstract void setFromStruct(Struct struct);
    }

    private abstract class ArrowGetterFromList {
        public final Field field;
        public final ArrowType arrowType;

        ArrowGetterFromList(Field field) {
            this.field = field;
            this.arrowType = field.getType();
        }

        public final ArrowType getArrowType() {
            return arrowType;
        }

        public abstract ArrowWriterFromList writer(ValueVector valueVector);
    }

    private abstract class ArrowWriterFromList {
        abstract void setFromList(List list, int i);
    }

    private ArrowGetterFromList arrowGetter(String name, Getters.FromList getter) {
        Getters.FromListToOptional optionalGetter = getter instanceof tech.ytsaurus.client.Getters.FromListToOptional
                ? (Getters.FromListToOptional) getter
                : null;
        var arrowGetter = nonComplexArrowGetter(name, optionalGetter != null ? (Getters.FromList) optionalGetter.getNotEmptyGetter() : getter);
        if (arrowGetter != null) {
            return optionalGetter == null ? arrowGetter : new ArrowGetterFromList(new Field(name, new FieldType(
                    true, arrowGetter.field.getType(), null
            ), arrowGetter.field.getChildren())) {
                @Override
                public ArrowWriterFromList writer(ValueVector valueVector) {
                    var nonOptionalWriter = arrowGetter.writer(valueVector);
                    return new ArrowWriterFromList() {
                        @Override
                        public void setFromList(List list, int i) {
                            nonOptionalWriter.setFromList(optionalGetter.isEmpty(list, i) ? null : list, i);
                        }
                    };
                }
            };
        }
        return new ArrowGetterFromList(new Field(name, new FieldType(
                optionalGetter != null, new ArrowType.Binary(), null
        ), new ArrayList<>())) {
            @Override
            public ArrowWriterFromList writer(ValueVector valueVector) {
                var varBinaryVector = (VarBinaryVector) valueVector;
                return new ArrowWriterFromList() {
                    @Override
                    public void setFromList(List list, int i) {
                        if (optionalGetter != null && optionalGetter.isEmpty(list, i)) {
                            varBinaryVector.setNull(varBinaryVector.getValueCount());
                        } else {
                            varBinaryVector.set(varBinaryVector.getValueCount(), getter.getYTreeNode(list, i).toBinary());
                        }
                        varBinaryVector.setValueCount(varBinaryVector.getValueCount() + 1);
                    }
                };
            }
        };
    }

    private ArrowGetterFromStruct arrowGetter(String name, Getters.FromStruct getter) {
        Getters.FromStructToOptional optionalGetter = getter instanceof tech.ytsaurus.client.Getters.FromStructToOptional
                ? (Getters.FromStructToOptional) getter
                : null;
        var arrowGetter = nonComplexArrowGetter(name, optionalGetter != null ? (Getters.FromStruct) optionalGetter.getNotEmptyGetter() : getter);
        if (arrowGetter != null) {
            return optionalGetter == null ? arrowGetter : new ArrowGetterFromStruct(new Field(name, new FieldType(
                    true, arrowGetter.field.getType(), null
            ), arrowGetter.field.getChildren())) {
                @Override
                public ArrowWriterFromStruct writer(ValueVector valueVector) {
                    var nonOptionalWriter = arrowGetter.writer(valueVector);
                    return new ArrowWriterFromStruct() {
                        @Override
                        public void setFromStruct(Struct struct) {
                            nonOptionalWriter.setFromStruct(optionalGetter.isEmpty(struct) ? null : struct);
                        }
                    };
                }
            };
        } else {
            return new ArrowGetterFromStruct(new Field(name, new FieldType(
                    optionalGetter != null, new ArrowType.Binary(), null
            ), new ArrayList<>())) {
                @Override
                public ArrowWriterFromStruct writer(ValueVector valueVector) {
                    var varBinaryVector = (VarBinaryVector) valueVector;
                    return new ArrowWriterFromStruct() {
                        @Override
                        void setFromStruct(Struct struct) {
                            if (optionalGetter != null && optionalGetter.isEmpty(struct)) {
                                varBinaryVector.setNull(varBinaryVector.getValueCount());
                            } else {
                                varBinaryVector.set(varBinaryVector.getValueCount(), getter.getYTreeNode(struct).toBinary());
                            }
                            varBinaryVector.setValueCount(varBinaryVector.getValueCount() + 1);
                        }
                    };
                }
            };
        }
    }

    private Field field(String name, ArrowType arrowType) {
        return new Field(name, new FieldType(false, arrowType, null), Collections.emptyList());
    }

    private ArrowGetterFromList nonComplexArrowGetter(String name, Getters.FromList getter) {
        var tiType = getter.getTiType();
        switch (tiType.getTypeName()) {
            case String: {
                var stringGetter = (Getters.FromListToString) getter;
                return new ArrowGetterFromList(
                        new Field(name, new FieldType(false, new ArrowType.Binary(), null), new ArrayList<>())
                ) {
                    @Override
                    public ArrowWriterFromList writer(ValueVector valueVector) {
                        var varBinaryVector = (VarBinaryVector) valueVector;
                        return new ArrowWriterFromList() {
                            @Override
                            void setFromList(List list, int i) {
                                if (list == null) {
                                    varBinaryVector.setNull(varBinaryVector.getValueCount());
                                } else {
                                    varBinaryVector.set(
                                            varBinaryVector.getValueCount(), stringGetter.getString(list, i).getBytes()
                                    );
                                }
                                varBinaryVector.setValueCount(varBinaryVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Int8: {
                var byteGetter = (Getters.FromListToByte) getter;
                return new ArrowGetterFromList(field(name, new ArrowType.Int(8, true))) {
                    @Override
                    public ArrowWriterFromList writer(ValueVector valueVector) {
                        var tinyIntVector = (TinyIntVector) valueVector;
                        return new ArrowWriterFromList() {
                            @Override
                            void setFromList(List list, int i) {
                                if (list == null) {
                                    tinyIntVector.setNull(tinyIntVector.getValueCount());
                                } else {
                                    tinyIntVector.set(tinyIntVector.getValueCount(), byteGetter.getByte(list, i));
                                }
                                tinyIntVector.setValueCount(tinyIntVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Uint8: {
                var byteGetter = (Getters.FromListToByte) getter;
                return new ArrowGetterFromList(field(name, new ArrowType.Int(8, false))) {
                    @Override
                    public ArrowWriterFromList writer(ValueVector valueVector) {
                        var uInt1Vector = (UInt1Vector) valueVector;
                        return new ArrowWriterFromList() {
                            @Override
                            void setFromList(List list, int i) {
                                if (list == null) {
                                    uInt1Vector.setNull(uInt1Vector.getValueCount());
                                } else {
                                    uInt1Vector.set(uInt1Vector.getValueCount(), byteGetter.getByte(list, i));
                                }
                                uInt1Vector.setValueCount(uInt1Vector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Int16: {
                var shortGetter = (Getters.FromListToShort) getter;
                return new ArrowGetterFromList(field(name, new ArrowType.Int(16, true))) {
                    @Override
                    public ArrowWriterFromList writer(ValueVector valueVector) {
                        var smallIntVector = (SmallIntVector) valueVector;
                        return new ArrowWriterFromList() {
                            @Override
                            void setFromList(List list, int i) {
                                if (list == null) {
                                    smallIntVector.setNull(smallIntVector.getValueCount());
                                } else {
                                    smallIntVector.set(smallIntVector.getValueCount(), shortGetter.getShort(list, i));
                                }
                                smallIntVector.setValueCount(smallIntVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Uint16: {
                var shortGetter = (Getters.FromListToShort) getter;
                return new ArrowGetterFromList(field(name, new ArrowType.Int(16, false))) {
                    @Override
                    public ArrowWriterFromList writer(ValueVector valueVector) {
                        var uInt2Vector = (UInt2Vector) valueVector;
                        return new ArrowWriterFromList() {
                            @Override
                            void setFromList(List list, int i) {
                                if (list == null) {
                                    uInt2Vector.setNull(uInt2Vector.getValueCount());
                                } else {
                                    uInt2Vector.set(uInt2Vector.getValueCount(), shortGetter.getShort(list, i));
                                }
                                uInt2Vector.setValueCount(uInt2Vector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Int32: {
                var intGetter = (Getters.FromListToInt) getter;
                return new ArrowGetterFromList(field(name, new ArrowType.Int(32, true))) {
                    @Override
                    public ArrowWriterFromList writer(ValueVector valueVector) {
                        var intVector = (IntVector) valueVector;
                        return new ArrowWriterFromList() {
                            @Override
                            void setFromList(List list, int i) {
                                if (list == null) {
                                    intVector.setNull(intVector.getValueCount());
                                } else {
                                    intVector.set(intVector.getValueCount(), intGetter.getInt(list, i));
                                }
                                intVector.setValueCount(intVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Uint32: {
                var intGetter = (Getters.FromListToInt) getter;
                return new ArrowGetterFromList(field(name, new ArrowType.Int(32, false))) {
                    @Override
                    public ArrowWriterFromList writer(ValueVector valueVector) {
                        var uInt4Vector = (UInt4Vector) valueVector;
                        return new ArrowWriterFromList() {
                            @Override
                            void setFromList(List list, int i) {
                                if (list == null) {
                                    uInt4Vector.setNull(uInt4Vector.getValueCount());
                                } else {
                                    uInt4Vector.set(uInt4Vector.getValueCount(), intGetter.getInt(list, i));
                                }
                                uInt4Vector.setValueCount(uInt4Vector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Interval:
            case Int64: {
                var longGetter = (Getters.FromListToLong) getter;
                return new ArrowGetterFromList(field(name, new ArrowType.Int(64, true))) {
                    @Override
                    public ArrowWriterFromList writer(ValueVector valueVector) {
                        var bigIntVector = (BigIntVector) valueVector;
                        return new ArrowWriterFromList() {
                            @Override
                            void setFromList(List list, int i) {
                                if (list == null) {
                                    bigIntVector.setNull(bigIntVector.getValueCount());
                                } else {
                                    bigIntVector.set(bigIntVector.getValueCount(), longGetter.getLong(list, i));
                                }
                                bigIntVector.setValueCount(bigIntVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Uint64: {
                var longGetter = (Getters.FromListToLong) getter;
                return new ArrowGetterFromList(field(name, new ArrowType.Int(64, false))) {
                    @Override
                    public ArrowWriterFromList writer(ValueVector valueVector) {
                        var uInt8Vector = (UInt8Vector) valueVector;
                        return new ArrowWriterFromList() {
                            @Override
                            void setFromList(List list, int i) {
                                if (list == null) {
                                    uInt8Vector.setNull(uInt8Vector.getValueCount());
                                } else {
                                    uInt8Vector.set(uInt8Vector.getValueCount(), longGetter.getLong(list, i));
                                }
                                uInt8Vector.setValueCount(uInt8Vector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Bool: {
                var booleanGetter = (Getters.FromListToBoolean) getter;
                return new ArrowGetterFromList(field(name, new ArrowType.Bool())) {
                    @Override
                    public ArrowWriterFromList writer(ValueVector valueVector) {
                        var bitVector = (BitVector) valueVector;
                        return new ArrowWriterFromList() {
                            @Override
                            void setFromList(List list, int i) {
                                if (list == null) {
                                    bitVector.setNull(bitVector.getValueCount());
                                } else {
                                    bitVector.set(bitVector.getValueCount(), booleanGetter.getBoolean(list, i) ? 1 : 0);
                                }
                                bitVector.setValueCount(bitVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Float: {
                var floatGetter = (Getters.FromListToFloat) getter;
                return new ArrowGetterFromList(field(name, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))) {
                    @Override
                    public ArrowWriterFromList writer(ValueVector valueVector) {
                        var float4Vector = (Float4Vector) valueVector;
                        return new ArrowWriterFromList() {
                            @Override
                            void setFromList(List list, int i) {
                                if (list == null) {
                                    float4Vector.setNull(float4Vector.getValueCount());
                                } else {
                                    float4Vector.set(float4Vector.getValueCount(), floatGetter.getFloat(list, i));
                                }
                                float4Vector.setValueCount(float4Vector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Double: {
                var doubleGetter = (Getters.FromListToDouble) getter;
                return new ArrowGetterFromList(field(name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))) {
                    @Override
                    public ArrowWriterFromList writer(ValueVector valueVector) {
                        var float8Vector = (Float8Vector) valueVector;
                        return new ArrowWriterFromList() {
                            @Override
                            void setFromList(List list, int i) {
                                if (list == null) {
                                    float8Vector.setNull(float8Vector.getValueCount());
                                } else {
                                    float8Vector.set(float8Vector.getValueCount(), doubleGetter.getDouble(list, i));
                                }
                                float8Vector.setValueCount(float8Vector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Date: {
                var intGetter = (Getters.FromListToInt) getter;
                return new ArrowGetterFromList(field(name, new ArrowType.Date(DateUnit.DAY))) {
                    @Override
                    public ArrowWriterFromList writer(ValueVector valueVector) {
                        var dateDayVector = (DateDayVector) valueVector;
                        return new ArrowWriterFromList() {
                            @Override
                            void setFromList(List list, int i) {
                                if (list == null) {
                                    dateDayVector.setNull(dateDayVector.getValueCount());
                                } else {
                                    dateDayVector.set(dateDayVector.getValueCount(), intGetter.getInt(list, i));
                                }
                                dateDayVector.setValueCount(dateDayVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Datetime: {
                var longGetter = (Getters.FromListToLong) getter;
                return new ArrowGetterFromList(field(name, new ArrowType.Date(DateUnit.MILLISECOND))) {
                    @Override
                    public ArrowWriterFromList writer(ValueVector valueVector) {
                        var dateMilliVector = (DateMilliVector) valueVector;
                        return new ArrowWriterFromList() {
                            @Override
                            void setFromList(List list, int i) {
                                if (list == null) {
                                    dateMilliVector.setNull(dateMilliVector.getValueCount());
                                } else {
                                    dateMilliVector.set(dateMilliVector.getValueCount(), longGetter.getLong(list, i));
                                }
                                dateMilliVector.setValueCount(dateMilliVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Timestamp: {
                var longGetter = (Getters.FromListToLong) getter;
                return new ArrowGetterFromList(field(name, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null))) {
                    @Override
                    public ArrowWriterFromList writer(ValueVector valueVector) {
                        var timeStampMicroVector = (TimeStampMicroVector) valueVector;
                        return new ArrowWriterFromList() {
                            @Override
                            void setFromList(List list, int i) {
                                if (list == null) {
                                    timeStampMicroVector.setNull(timeStampMicroVector.getValueCount());
                                } else {
                                    timeStampMicroVector.set(timeStampMicroVector.getValueCount(), longGetter.getLong(list, i));
                                }
                                timeStampMicroVector.setValueCount(timeStampMicroVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case List: {
                var listGetter = (Getters.FromListToList) getter;
                var elementGetter = listGetter.getElementGetter();
                var itemGetter = arrowGetter("item", (Getters.FromList) elementGetter);
                return new ArrowGetterFromList(new Field(name, new FieldType(
                        false, new ArrowType.List(), null
                ), Collections.singletonList(itemGetter.field))) {
                    @Override
                    public ArrowWriterFromList writer(ValueVector valueVector) {
                        var listVector = (ListVector) valueVector;
                        var dataWriter = itemGetter.writer(listVector.getDataVector());
                        return new ArrowWriterFromList() {
                            @Override
                            void setFromList(List list, int i) {
                                var value = list == null ? null : (List) listGetter.getList(list, i);
                                if (value != null) {
                                    int size = elementGetter.getSize(value);
                                    listVector.startNewValue(listVector.getValueCount());
                                    for (int j = 0; j < size; j++) {
                                        dataWriter.setFromList(value, j);
                                    }
                                    listVector.endValue(listVector.getValueCount(), size);
                                }
                                listVector.setValueCount(listVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Dict: {
                var dictGetter = (Getters.FromListToDict) getter;
                var fromDictGetter = dictGetter.getGetter();
                var keyGetter = arrowGetter("key", (Getters.FromList) fromDictGetter.getKeyGetter());
                var valueGetter = arrowGetter("value", (Getters.FromList) fromDictGetter.getValueGetter());
                return new ArrowGetterFromList(new Field(
                        name, new FieldType(false, new ArrowType.Map(false), null),
                        Collections.singletonList(new Field(
                                "entries", new FieldType(false, new ArrowType.List(), null),
                                Arrays.asList(keyGetter.field, valueGetter.field)
                        ))
                )) {
                    @Override
                    public ArrowWriterFromList writer(ValueVector valueVector) {
                        var mapVector = (MapVector) valueVector;
                        var structVector = (StructVector) mapVector.getDataVector();
                        var keyWriter = keyGetter.writer(structVector.getChildByOrdinal(0));
                        var valueWriter = valueGetter.writer(structVector.getChildByOrdinal(1));
                        return new ArrowWriterFromList() {
                            @Override
                            void setFromList(List list, int i) {
                                var dict = list == null ? null : dictGetter.getDict(list, i);
                                if (dict != null) {
                                    int size = fromDictGetter.getSize(dict);
                                    var keys = fromDictGetter.getKeys(dict);
                                    var values = fromDictGetter.getValues(dict);
                                    mapVector.startNewValue(mapVector.getValueCount());
                                    for (int j = 0; j < size; j++) {
                                        keyWriter.setFromList((List) keys, j);
                                        valueWriter.setFromList((List) values, j);
                                    }
                                    mapVector.endValue(mapVector.getValueCount(), size);
                                }
                                mapVector.setValueCount(mapVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Struct: {
                var structGetter = (Getters.FromListToStruct) getter;
                var members = (java.util.List<Map.Entry<String, Getters.FromStruct>>) structGetter.getMembersGetters();
                var membersGetters = new ArrayList<ArrowGetterFromStruct>(members.size());
                for (Map.Entry<String, Getters.FromStruct> member : members) {
                    membersGetters.add(arrowGetter(member.getKey(), member.getValue()));
                }
                return new ArrowGetterFromList(new Field(
                        name, new FieldType(false, new ArrowType.Struct(), null),
                        Collections.singletonList(new Field(
                                "entries", new FieldType(false, new ArrowType.List(), null),
                                membersGetters.stream().map(member -> member.field).collect(Collectors.toList())
                        ))
                )) {
                    @Override
                    public ArrowWriterFromList writer(ValueVector valueVector) {
                        var structVector = (StructVector) valueVector;
                        var membersWriters = new ArrayList<ArrowWriterFromStruct>(members.size());
                        for (int i = 0; i < members.size(); i++) {
                            membersWriters.add(membersGetters.get(i).writer(structVector.getChildByOrdinal(i)));
                        }
                        return new ArrowWriterFromList() {
                            @Override
                            void setFromList(List list, int i) {
                                if (list == null) {
                                    for (int j = 0; j < members.size(); j++) {
                                        membersWriters.get(j).setFromStruct(null);
                                    }
                                } else {
                                    var struct = (Struct) structGetter.getStruct(list, i);
                                    structVector.setIndexDefined(structVector.getValueCount());
                                    for (int j = 0; j < members.size(); j++) {
                                        membersWriters.get(j).setFromStruct(struct);
                                    }
                                }
                                structVector.setValueCount(structVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            default:
                return null;
        }
    }

    private ArrowGetterFromStruct nonComplexArrowGetter(String name, Getters.FromStruct getter) {
        var tiType = getter.getTiType();
        switch (tiType.getTypeName()) {
            case String: {
                var stringGetter = (Getters.FromStructToString) getter;
                return new ArrowGetterFromStruct(field(name, new ArrowType.Binary())) {
                    @Override
                    public ArrowWriterFromStruct writer(ValueVector valueVector) {
                        var varBinaryVector = (VarBinaryVector) valueVector;
                        return new ArrowWriterFromStruct() {
                            @Override
                            void setFromStruct(Struct struct) {
                                if (struct == null) {
                                    varBinaryVector.setNull(varBinaryVector.getValueCount());
                                } else {
                                    varBinaryVector.set(
                                            varBinaryVector.getValueCount(), stringGetter.getString(struct).getBytes()
                                    );
                                }
                                varBinaryVector.setValueCount(varBinaryVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Int8: {
                var byteGetter = (Getters.FromStructToByte) getter;
                return new ArrowGetterFromStruct(field(name, new ArrowType.Int(8, true))) {
                    @Override
                    public ArrowWriterFromStruct writer(ValueVector valueVector) {
                        var tinyIntVector = (TinyIntVector) valueVector;
                        return new ArrowWriterFromStruct() {
                            @Override
                            void setFromStruct(Struct struct) {
                                if (struct == null) {
                                    tinyIntVector.setNull(tinyIntVector.getValueCount());
                                } else {
                                    tinyIntVector.set(tinyIntVector.getValueCount(), byteGetter.getByte(struct));
                                }
                                tinyIntVector.setValueCount(tinyIntVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Uint8: {
                var byteGetter = (Getters.FromStructToByte) getter;
                return new ArrowGetterFromStruct(field(name, new ArrowType.Int(8, false))) {
                    @Override
                    public ArrowWriterFromStruct writer(ValueVector valueVector) {
                        var uInt1Vector = (UInt1Vector) valueVector;
                        return new ArrowWriterFromStruct() {
                            @Override
                            void setFromStruct(Struct struct) {
                                if (struct == null) {
                                    uInt1Vector.setNull(uInt1Vector.getValueCount());
                                } else {
                                    uInt1Vector.set(uInt1Vector.getValueCount(), byteGetter.getByte(struct));
                                }
                                uInt1Vector.setValueCount(uInt1Vector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Int16: {
                var shortGetter = (Getters.FromStructToShort) getter;
                return new ArrowGetterFromStruct(field(name, new ArrowType.Int(16, true))) {
                    @Override
                    public ArrowWriterFromStruct writer(ValueVector valueVector) {
                        var smallIntVector = (SmallIntVector) valueVector;
                        return new ArrowWriterFromStruct() {
                            @Override
                            void setFromStruct(Struct struct) {
                                if (struct == null) {
                                    smallIntVector.setNull(smallIntVector.getValueCount());
                                } else {
                                    smallIntVector.set(smallIntVector.getValueCount(), shortGetter.getShort(struct));
                                }
                                smallIntVector.setValueCount(smallIntVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Uint16: {
                var shortGetter = (Getters.FromStructToShort) getter;
                return new ArrowGetterFromStruct(field(name, new ArrowType.Int(16, false))) {
                    @Override
                    public ArrowWriterFromStruct writer(ValueVector valueVector) {
                        var uInt2Vector = (UInt2Vector) valueVector;
                        return new ArrowWriterFromStruct() {
                            @Override
                            void setFromStruct(Struct struct) {
                                if (struct == null) {
                                    uInt2Vector.setNull(uInt2Vector.getValueCount());
                                } else {
                                    uInt2Vector.set(uInt2Vector.getValueCount(), shortGetter.getShort(struct));
                                }
                                uInt2Vector.setValueCount(uInt2Vector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Int32: {
                var intGetter = (Getters.FromStructToInt) getter;
                return new ArrowGetterFromStruct(field(name, new ArrowType.Int(32, true))) {
                    @Override
                    public ArrowWriterFromStruct writer(ValueVector valueVector) {
                        var intVector = (IntVector) valueVector;
                        return new ArrowWriterFromStruct() {
                            @Override
                            void setFromStruct(Struct struct) {
                                if (struct == null) {
                                    intVector.setNull(intVector.getValueCount());
                                } else {
                                    intVector.set(intVector.getValueCount(), intGetter.getInt(struct));
                                }
                                intVector.setValueCount(intVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Uint32: {
                var intGetter = (Getters.FromStructToInt) getter;
                return new ArrowGetterFromStruct(field(name, new ArrowType.Int(32, false))) {
                    @Override
                    public ArrowWriterFromStruct writer(ValueVector valueVector) {
                        var uInt4Vector = (UInt4Vector) valueVector;
                        return new ArrowWriterFromStruct() {
                            @Override
                            void setFromStruct(Struct struct) {
                                if (struct == null) {
                                    uInt4Vector.setNull(uInt4Vector.getValueCount());
                                } else {
                                    uInt4Vector.set(uInt4Vector.getValueCount(), intGetter.getInt(struct));
                                }
                                uInt4Vector.setValueCount(uInt4Vector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Interval:
            case Int64: {
                var longGetter = (Getters.FromStructToLong) getter;
                return new ArrowGetterFromStruct(field(name, new ArrowType.Int(64, true))) {
                    @Override
                    public ArrowWriterFromStruct writer(ValueVector valueVector) {
                        var bigIntVector = (BigIntVector) valueVector;
                        return new ArrowWriterFromStruct() {
                            @Override
                            void setFromStruct(Struct struct) {
                                if (struct == null) {
                                    bigIntVector.setNull(bigIntVector.getValueCount());
                                } else {
                                    bigIntVector.set(bigIntVector.getValueCount(), longGetter.getLong(struct));
                                }
                                bigIntVector.setValueCount(bigIntVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Uint64: {
                var longGetter = (Getters.FromStructToLong) getter;
                return new ArrowGetterFromStruct(field(name, new ArrowType.Int(64, false))) {
                    @Override
                    public ArrowWriterFromStruct writer(ValueVector valueVector) {
                        var uInt8Vector = (UInt8Vector) valueVector;
                        return new ArrowWriterFromStruct() {
                            @Override
                            void setFromStruct(Struct struct) {
                                if (struct == null) {
                                    uInt8Vector.setNull(uInt8Vector.getValueCount());
                                } else {
                                    uInt8Vector.set(uInt8Vector.getValueCount(), longGetter.getLong(struct));
                                }
                                uInt8Vector.setValueCount(uInt8Vector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Bool: {
                var booleanGetter = (Getters.FromStructToBoolean) getter;
                return new ArrowGetterFromStruct(field(name, new ArrowType.Bool())) {
                    @Override
                    public ArrowWriterFromStruct writer(ValueVector valueVector) {
                        var bitVector = (BitVector) valueVector;
                        return new ArrowWriterFromStruct() {
                            @Override
                            void setFromStruct(Struct struct) {
                                if (struct == null) {
                                    bitVector.setNull(bitVector.getValueCount());
                                } else {
                                    bitVector.set(bitVector.getValueCount(), booleanGetter.getBoolean(struct) ? 1 : 0);
                                }
                                bitVector.setValueCount(bitVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Float: {
                var floatGetter = (Getters.FromStructToFloat) getter;
                return new ArrowGetterFromStruct(field(name, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))) {
                    @Override
                    public ArrowWriterFromStruct writer(ValueVector valueVector) {
                        var float4Vector = (Float4Vector) valueVector;
                        return new ArrowWriterFromStruct() {
                            @Override
                            void setFromStruct(Struct struct) {
                                if (struct == null) {
                                    float4Vector.setNull(float4Vector.getValueCount());
                                } else {
                                    float4Vector.set(float4Vector.getValueCount(), floatGetter.getFloat(struct));
                                }
                                float4Vector.setValueCount(float4Vector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Double: {
                var doubleGetter = (Getters.FromStructToDouble) getter;
                return new ArrowGetterFromStruct(field(name, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))) {
                    @Override
                    public ArrowWriterFromStruct writer(ValueVector valueVector) {
                        var float8Vector = (Float8Vector) valueVector;
                        return new ArrowWriterFromStruct() {
                            @Override
                            void setFromStruct(Struct struct) {
                                if (struct == null) {
                                    float8Vector.setNull(float8Vector.getValueCount());
                                } else {
                                    float8Vector.set(float8Vector.getValueCount(), doubleGetter.getDouble(struct));
                                }
                                float8Vector.setValueCount(float8Vector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Date: {
                var intGetter = (Getters.FromStructToInt) getter;
                return new ArrowGetterFromStruct(field(name, new ArrowType.Date(DateUnit.DAY))) {
                    @Override
                    public ArrowWriterFromStruct writer(ValueVector valueVector) {
                        var dateDayVector = (DateDayVector) valueVector;
                        return new ArrowWriterFromStruct() {
                            @Override
                            void setFromStruct(Struct struct) {
                                if (struct == null) {
                                    dateDayVector.setNull(dateDayVector.getValueCount());
                                } else {
                                    dateDayVector.set(dateDayVector.getValueCount(), intGetter.getInt(struct));
                                }
                                dateDayVector.setValueCount(dateDayVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Datetime: {
                var longGetter = (Getters.FromStructToLong) getter;
                return new ArrowGetterFromStruct(field(name, new ArrowType.Date(DateUnit.MILLISECOND))) {
                    @Override
                    public ArrowWriterFromStruct writer(ValueVector valueVector) {
                        var dateMilliVector = (DateMilliVector) valueVector;
                        return new ArrowWriterFromStruct() {
                            @Override
                            void setFromStruct(Struct struct) {
                                if (struct == null) {
                                    dateMilliVector.setNull(dateMilliVector.getValueCount());
                                } else {
                                    dateMilliVector.set(dateMilliVector.getValueCount(), longGetter.getLong(struct));
                                }
                                dateMilliVector.setValueCount(dateMilliVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Timestamp: {
                var longGetter = (Getters.FromStructToLong) getter;
                return new ArrowGetterFromStruct(field(name, new ArrowType.Timestamp(TimeUnit.MICROSECOND, null))) {
                    @Override
                    public ArrowWriterFromStruct writer(ValueVector valueVector) {
                        var timeStampMicroVector = (TimeStampMicroVector) valueVector;
                        return new ArrowWriterFromStruct() {
                            @Override
                            void setFromStruct(Struct struct) {
                                if (struct == null) {
                                    timeStampMicroVector.setNull(timeStampMicroVector.getValueCount());
                                } else {
                                    timeStampMicroVector.set(timeStampMicroVector.getValueCount(), longGetter.getLong(struct));
                                }
                                timeStampMicroVector.setValueCount(timeStampMicroVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case List: {
                var listGetter = (Getters.FromStructToList) getter;
                var elementGetter = (Getters.FromList) listGetter.getElementGetter();
                var itemGetter = arrowGetter("item", elementGetter);
                return new ArrowGetterFromStruct(new Field(name, new FieldType(
                        false, new ArrowType.List(), null
                ), Collections.singletonList(itemGetter.field))) {
                    @Override
                    public ArrowWriterFromStruct writer(ValueVector valueVector) {
                        var listVector = (ListVector) valueVector;
                        var dataWriter = itemGetter.writer(listVector.getDataVector());
                        return new ArrowWriterFromStruct() {
                            @Override
                            public void setFromStruct(Struct struct) {
                                var list = struct == null ? null : (List) listGetter.getList(struct);
                                if (list != null) {
                                    int size = elementGetter.getSize(list);
                                    listVector.startNewValue(listVector.getValueCount());
                                    for (int i = 0; i < size; i++) {
                                        dataWriter.setFromList(list, i);
                                    }
                                    listVector.endValue(listVector.getValueCount(), size);
                                }
                                listVector.setValueCount(listVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Dict: {
                var dictGetter = (Getters.FromStructToDict) getter;
                var fromDictGetter = (Getters.FromDict) dictGetter.getGetter();
                var keyGetter = arrowGetter("key", (Getters.FromList) fromDictGetter.getKeyGetter());
                var valueGetter = arrowGetter("value", (Getters.FromList) fromDictGetter.getValueGetter());
                return new ArrowGetterFromStruct(new Field(
                        name, new FieldType(false, new ArrowType.Map(false), null),
                        Collections.singletonList(new Field(
                                "entries", new FieldType(false, new ArrowType.List(), null),
                                Arrays.asList(keyGetter.field, valueGetter.field)
                        ))
                )) {
                    @Override
                    public ArrowWriterFromStruct writer(ValueVector valueVector) {
                        var mapVector = (MapVector) valueVector;
                        var structVector = (StructVector) mapVector.getDataVector();
                        var keyWriter = keyGetter.writer(structVector.getChildByOrdinal(0));
                        var valueWriter = valueGetter.writer(structVector.getChildByOrdinal(1));
                        return new ArrowWriterFromStruct() {
                            @Override
                            public void setFromStruct(Struct struct) {
                                var dict = struct == null ? null : dictGetter.getDict(struct);
                                if (dict != null) {
                                    int size = fromDictGetter.getSize(dict);
                                    var keys = (List) fromDictGetter.getKeys(dict);
                                    var values = (List) fromDictGetter.getValues(dict);
                                    mapVector.startNewValue(mapVector.getValueCount());
                                    for (int i = 0; i < size; i++) {
                                        keyWriter.setFromList(keys, i);
                                        valueWriter.setFromList(values, i);
                                    }
                                    mapVector.endValue(mapVector.getValueCount(), size);
                                }
                                mapVector.setValueCount(mapVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            case Struct: {
                var structGetter = (Getters.FromStructToStruct) getter;
                var members = (java.util.List<Map.Entry<String, Getters.FromStruct>>) structGetter.getMembersGetters();
                var membersGetters = new ArrayList<ArrowGetterFromStruct>(members.size());
                for (Map.Entry<String, ? extends Getters.FromStruct> member : members) {
                    membersGetters.add(arrowGetter(member.getKey(), member.getValue()));
                }
                return new ArrowGetterFromStruct(new Field(
                        name, new FieldType(false, new ArrowType.Struct(), null),
                        Collections.singletonList(new Field(
                                "entries", new FieldType(false, new ArrowType.List(), null),
                                membersGetters.stream().map(member -> member.field).collect(Collectors.toList())
                        ))
                )) {
                    @Override
                    public ArrowWriterFromStruct writer(ValueVector valueVector) {
                        var structVector = (StructVector) valueVector;
                        var membersWriters = new ArrayList<ArrowWriterFromStruct>(members.size());
                        for (int i = 0; i < members.size(); i++) {
                            membersWriters.add(membersGetters.get(i).writer(structVector.getChildByOrdinal(i)));
                        }
                        return new ArrowWriterFromStruct() {
                            @Override
                            void setFromStruct(Struct row) {
                                if (row == null) {
                                    for (int i = 0; i < members.size(); i++) {
                                        membersWriters.get(i).setFromStruct(null);
                                    }
                                } else {
                                    var struct = (Struct) structGetter.getStruct(row);
                                    structVector.setIndexDefined(structVector.getValueCount());
                                    for (int i = 0; i < members.size(); i++) {
                                        membersWriters.get(i).setFromStruct(struct);
                                    }
                                }
                                structVector.setValueCount(structVector.getValueCount() + 1);
                            }
                        };
                    }
                };
            }
            default:
                return null;
        }
    }

    private final java.util.List<ArrowGetterFromStruct> fieldGetters;
    private final Schema schema;
    private final BufferAllocator allocator =
            ROOT_ALLOCATOR.newChildAllocator("toBatchIterator", 0, Long.MAX_VALUE);

    public ArrowTableRowsSerializer(
            java.util.List<? extends Map.Entry<String, ? extends Getters.FromStruct>> structsGetter
    ) {
        super(ERowsetFormat.RF_FORMAT);
        fieldGetters = structsGetter.stream().map(memberGetter -> arrowGetter(
                memberGetter.getKey(), memberGetter.getValue()
        )).collect(Collectors.toList());
        schema = new Schema(() -> fieldGetters.stream().map(getter -> getter.field).iterator());
    }

    @Override
    public void close() {
        allocator.close();
    }

    private static class ByteBufWritableByteChannel implements WritableByteChannel {
        private final ByteBuf buf;

        private ByteBufWritableByteChannel(ByteBuf buf) {
            this.buf = buf;
        }

        @Override
        public int write(ByteBuffer src) {
            int remaining = src.remaining();
            buf.writeBytes(src);
            return remaining - src.remaining();
        }

        @Override
        public boolean isOpen() {
            return buf.isWritable();
        }

        @Override
        public void close() {
        }
    }

    @Override
    protected void writeMeta(ByteBuf buf, ByteBuf serializedRows, int rowsCount) {
        try {
            var writeChannel = new WriteChannel(new ByteBufWritableByteChannel(buf));
            MessageSerializer.serialize(writeChannel, schema);
            writeChannel.write(serializedRows.nioBuffer());
            ArrowStreamWriter.writeEndOfStream(writeChannel, new IpcOption());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void writeRowsWithoutCount(
            ByteBuf buf, TRowsetDescriptor descriptor, java.util.List<Struct> rows, int[] idMapping
    ) {
        writeRows(buf, descriptor, rows, idMapping);
    }

    @Override
    protected void writeRows(ByteBuf buf, TRowsetDescriptor descriptor, java.util.List<Struct> rows, int[] idMapping) {
        try {
            var writeChannel = new WriteChannel(new ByteBufWritableByteChannel(buf));
            MessageSerializer.serialize(writeChannel, schema);
            var root = VectorSchemaRoot.create(schema, allocator);
            var unloader = new VectorUnloader(root);
            var writers = IntStream.range(0, fieldGetters.size()).mapToObj(column -> {
                var valueVector = root.getFieldVectors().get(column);
                if (valueVector instanceof FixedWidthVector) {
                    ((FixedWidthVector) valueVector).allocateNew(rows.size());
                } else {
                    valueVector.allocateNew();
                }
                return fieldGetters.get(column).writer(valueVector);
            }).collect(Collectors.toList());
            for (var row : rows) {
                for (var writer : writers) {
                    writer.setFromStruct(row);
                }
            }
            root.setRowCount(rows.size());
            try (var batch = unloader.getRecordBatch()) {
                MessageSerializer.serialize(writeChannel, batch);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
