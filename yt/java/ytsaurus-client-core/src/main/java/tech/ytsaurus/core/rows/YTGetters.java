package tech.ytsaurus.core.rows;

import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.yson.YsonConsumer;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Map;

public class YTGetters {
    public interface GetTiType {
        TiType getTiType();
    }

    public interface FromStruct<Struct> extends GetTiType {
        void getYson(Struct struct, YsonConsumer ysonConsumer);
    }

    public interface FromList<List> extends GetTiType {
        int getSize(List list);

        void getYson(List list, int i, YsonConsumer ysonConsumer);
    }

    public interface FromStructToYson<Struct> extends FromStruct<Struct> {
    }

    public interface FromListToYson<List> extends FromList<List> {
    }

    public interface FromDict<Dict, Keys, Values> extends GetTiType {
        FromList<Keys> getKeyGetter();

        FromList<Values> getValueGetter();

        int getSize(Dict dict);

        Keys getKeys(Dict dict);

        Values getValues(Dict dict);
    }

    public interface FromStructToNull<Struct> extends FromStruct<Struct> {
    }

    public interface FromListToNull<List> extends FromList<List> {
    }

    public interface FromStructToOptional<Struct> extends FromStruct<Struct> {
        FromStruct<Struct> getNotEmptyGetter();

        boolean isEmpty(Struct struct);
    }

    public interface FromListToOptional<List> extends FromList<List> {
        FromList<List> getNotEmptyGetter();

        boolean isEmpty(List list, int i);
    }

    public interface FromStructToString<Struct> extends FromStruct<Struct> {
        ByteBuffer getString(Struct struct);
    }

    public interface FromListToString<List> extends FromList<List> {
        ByteBuffer getString(List struct, int i);
    }

    public interface FromStructToByte<Struct> extends FromStruct<Struct> {
        byte getByte(Struct struct);
    }

    public interface FromListToByte<List> extends FromList<List> {
        byte getByte(List list, int i);
    }

    public interface FromStructToShort<Struct> extends FromStruct<Struct> {
        short getShort(Struct struct);
    }

    public interface FromListToShort<List> extends FromList<List> {
        short getShort(List list, int i);
    }

    public interface FromStructToInt<Struct> extends FromStruct<Struct> {
        int getInt(Struct struct);
    }

    public interface FromListToInt<List> extends FromList<List> {
        int getInt(List list, int i);
    }

    public interface FromStructToLong<Struct> extends FromStruct<Struct> {
        long getLong(Struct struct);
    }

    public interface FromListToLong<List> extends FromList<List> {
        long getLong(List list, int i);
    }

    public interface FromStructToBoolean<Struct> extends FromStruct<Struct> {
        boolean getBoolean(Struct struct);
    }

    public interface FromListToBoolean<List> extends FromList<List> {
        boolean getBoolean(List list, int i);
    }

    public interface FromStructToFloat<Struct> extends FromStruct<Struct> {
        float getFloat(Struct struct);
    }

    public interface FromListToFloat<List> extends FromList<List> {
        float getFloat(List list, int i);
    }

    public interface FromStructToDouble<Struct> extends FromStruct<Struct> {
        double getDouble(Struct struct);
    }

    public interface FromListToDouble<List> extends FromList<List> {
        double getDouble(List list, int i);
    }

    public interface FromStructToStruct<Struct, Value> extends FromStruct<Struct> {
        java.util.List<Map.Entry<String, FromStruct<Value>>> getMembersGetters();

        Value getStruct(Struct struct);
    }

    public interface FromListToStruct<List, Value> extends FromList<List> {
        java.util.List<Map.Entry<String, FromStruct<Value>>> getMembersGetters();

        Value getStruct(List list, int i);
    }

    public interface FromStructToList<Struct, List> extends FromStruct<Struct> {
        FromList<List> getElementGetter();

        List getList(Struct struct);
    }

    public interface FromListToList<List, Value> extends FromList<List> {
        FromList<Value> getElementGetter();

        Value getList(List list, int i);
    }

    public interface FromStructToDict<Struct, Dict, Keys, Values> extends FromStruct<Struct> {
        FromDict<Dict, Keys, Values> getGetter();

        Dict getDict(Struct struct);
    }

    public interface FromListToDict<List, Dict, Keys, Values> extends FromList<List> {
        FromDict<Dict, Keys, Values> getGetter();

        Dict getDict(List list, int i);
    }

    public interface FromStructToBigDecimal<Struct> extends FromStruct<Struct> {
        BigDecimal getBigDecimal(Struct struct);
    }

    public interface FromListToBigDecimal<List> extends FromList<List> {
        BigDecimal getBigDecimal(List list, int i);
    }
}
