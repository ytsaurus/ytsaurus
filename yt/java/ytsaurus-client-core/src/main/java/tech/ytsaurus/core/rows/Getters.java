package tech.ytsaurus.core.rows;

import tech.ytsaurus.typeinfo.*;
import tech.ytsaurus.ysontree.YTreeNode;

import java.util.Map;

public abstract class Getters<Struct, List, Dict> {
    public abstract java.util.List<? extends Map.Entry<String, ? extends FromStruct>> getSchema();

    public abstract class Getter {
        private Getter() {
        }

        public abstract TiType getTiType();
    }

    public abstract class FromStruct extends Getter {
        private FromStruct() {
        }

        public YTreeNode getYTreeNode(Struct struct) {
            throw new UnsupportedOperationException();
        }
    }

    public abstract class FromList extends Getter {
        private FromList() {
        }

        public abstract int getSize(List list);

        public YTreeNode getYTreeNode(List list, int i) {
            throw new UnsupportedOperationException();
        }
    }

    public abstract class FromDict extends Getter {
        private FromDict() {
        }

        public abstract FromList getKeyGetter();

        public abstract FromList getValueGetter();

        public abstract int getSize(Dict dict);

        public abstract List getKeys(Dict dict);

        public abstract List getValues(Dict dict);
    }

    public abstract class FromStructToOptional extends FromStruct {
        public abstract FromStruct getNotEmptyGetter();

        public abstract boolean isEmpty(Struct struct);
    }

    public abstract class FromListToOptional extends FromList {
        public abstract FromList getNotEmptyGetter();

        public abstract boolean isEmpty(List list, int i);
    }

    public abstract class FromStructToString extends FromStruct {
        public abstract String getString(Struct struct);
    }

    public abstract class FromListToString extends FromList {
        public abstract String getString(List struct, int i);
    }

    public abstract class FromStructToByte extends FromStruct {
        public abstract byte getByte(Struct struct);
    }

    public abstract class FromListToByte extends FromList {
        public abstract byte getByte(List list, int i);
    }

    public abstract class FromStructToShort extends FromStruct {
        public abstract short getShort(Struct struct);
    }

    public abstract class FromListToShort extends FromList {
        public abstract short getShort(List list, int i);
    }

    public abstract class FromStructToInt extends FromStruct {
        public abstract int getInt(Struct struct);
    }

    public abstract class FromListToInt extends FromList {
        public abstract int getInt(List list, int i);
    }

    public abstract class FromStructToLong extends FromStruct {
        public abstract long getLong(Struct struct);
    }

    public abstract class FromListToLong extends FromList {
        public abstract long getLong(List list, int i);
    }

    public abstract class FromStructToBoolean extends FromStruct {
        public abstract boolean getBoolean(Struct struct);
    }

    public abstract class FromListToBoolean extends FromList {
        public abstract boolean getBoolean(List list, int i);
    }

    public abstract class FromStructToFloat extends FromStruct {
        public abstract float getFloat(Struct struct);
    }

    public abstract class FromListToFloat extends FromList {
        public abstract float getFloat(List list, int i);
    }

    public abstract class FromStructToDouble extends FromStruct {
        public abstract double getDouble(Struct struct);
    }

    public abstract class FromListToDouble extends FromList {
        public abstract double getDouble(List list, int i);
    }

    public abstract class FromStructToStruct extends FromStruct {
        public abstract java.util.List<Map.Entry<String, FromStruct>> getMembersGetters();

        public abstract Struct getStruct(Struct struct);
    }

    public abstract class FromListToStruct extends FromList {
        public abstract java.util.List<Map.Entry<String, FromStruct>> getMembersGetters();

        public abstract Struct getStruct(List list, int i);
    }

    public abstract class FromStructToList extends FromStruct {
        public abstract FromList getElementGetter();

        public abstract List getList(Struct struct);
    }

    public abstract class FromListToList extends FromList {
        public abstract FromList getElementGetter();

        public abstract List getList(List list, int i);
    }

    public abstract class FromStructToDict extends FromStruct {
        public abstract FromDict getGetter();

        public abstract Dict getDict(Struct struct);
    }

    public abstract class FromListToDict extends FromList {
        public abstract FromDict getGetter();

        public abstract Dict getDict(List list, int i);
    }
}
