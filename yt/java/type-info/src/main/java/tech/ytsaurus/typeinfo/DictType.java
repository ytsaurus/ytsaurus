package tech.ytsaurus.typeinfo;

import java.util.Objects;

import tech.ytsaurus.yson.YsonConsumer;

public class DictType extends TiType {
    private final TiType key;
    private final TiType value;

    public DictType(TiType key, TiType value) {
        super(TypeName.Dict);
        this.key = key;
        this.value = value;
    }

    public TiType getKey() {
        return key;
    }

    public TiType getValue() {
        return value;
    }

    @Override
    public String toString() {
        return String.format("Dict<%s, %s>", key, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DictType dictType = (DictType) o;
        return key.equals(dictType.key) && value.equals(dictType.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(TypeName.Dict, key, value);
    }

    @Override
    public void serializeTo(YsonConsumer ysonConsumer) {
        ysonConsumer.onBeginMap();

        assert TypeName.Dict.wireNameBytes != null;
        YsonConsumer.onKeyedItem(ysonConsumer, KeyNames.TYPE_NAME);
        YsonConsumer.onString(ysonConsumer, TypeName.Dict.wireNameBytes);

        YsonConsumer.onKeyedItem(ysonConsumer, KeyNames.KEY);
        key.serializeTo(ysonConsumer);

        YsonConsumer.onKeyedItem(ysonConsumer, KeyNames.VALUE);
        value.serializeTo(ysonConsumer);

        ysonConsumer.onEndMap();
    }
}
