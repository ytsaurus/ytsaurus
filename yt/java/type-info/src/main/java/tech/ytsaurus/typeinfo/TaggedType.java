package tech.ytsaurus.typeinfo;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.yson.YsonConsumer;

public class TaggedType extends TiType {
    final TiType item;
    final String tag;

    TaggedType(TiType item, String tag) {
        super(TypeName.Tagged);
        this.item = item;
        this.tag = tag;
    }

    public TiType getItem() {
        return item;
    }

    public String getTag() {
        return tag;
    }

    @Override
    public String toString() {
        return String.format("Tagged<%s, %s>", item, Escape.quote(tag));
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        } else if (o == null || o.getClass() != this.getClass()) {
            return false;
        }

        TaggedType that = (TaggedType) o;
        return this.tag.equals(that.tag) &&
                this.item.equals(that.item);
    }

    @Override
    public int hashCode() {
        return Objects.hash(TypeName.Tagged, item, tag);
    }

    @Override
    public void serializeTo(YsonConsumer ysonConsumer) {
        ysonConsumer.onBeginMap();

        assert TypeName.Tagged.wireNameBytes != null;
        YsonConsumer.onKeyedItem(ysonConsumer, KeyNames.TYPE_NAME);
        YsonConsumer.onString(ysonConsumer, TypeName.Tagged.wireNameBytes);

        YsonConsumer.onKeyedItem(ysonConsumer, KeyNames.ITEM);
        item.serializeTo(ysonConsumer);

        YsonConsumer.onKeyedItem(ysonConsumer, KeyNames.TAG);
        ysonConsumer.onString(tag);

        ysonConsumer.onEndMap();
    }
}
