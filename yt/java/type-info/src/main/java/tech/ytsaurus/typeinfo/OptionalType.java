package tech.ytsaurus.typeinfo;

import java.util.Objects;

import tech.ytsaurus.yson.YsonConsumer;

public class OptionalType extends ItemizedType {
    OptionalType(TiType element) {
        super(TypeName.Optional, element);
    }

    @Override
    public String toString() {
        return String.format("Optional<%s>", item);
    }
}

abstract class ItemizedType extends TiType {
    final TiType item;

    ItemizedType(TypeName typeName, TiType item) {
        super(typeName);
        this.item = item;
    }

    public TiType getItem() {
        return item;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ItemizedType that = (ItemizedType) o;
        return item.equals(that.item);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeName, item);
    }

    @Override
    public void serializeTo(YsonConsumer ysonConsumer) {
        ysonConsumer.onBeginMap();

        assert typeName.wireNameBytes != null;
        YsonConsumer.onKeyedItem(ysonConsumer, KeyNames.TYPE_NAME);
        YsonConsumer.onString(ysonConsumer, typeName.wireNameBytes);

        YsonConsumer.onKeyedItem(ysonConsumer, KeyNames.ITEM);
        item.serializeTo(ysonConsumer);

        ysonConsumer.onEndMap();
    }
}
