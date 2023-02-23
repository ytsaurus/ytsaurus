package tech.ytsaurus.typeinfo;

import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.yson.YsonConsumer;

public class VariantType extends TiType {
    private final TiType underlying;

    VariantType(StructType underlying) {
        super(TypeName.Variant);
        this.underlying = underlying;
    }

    VariantType(TupleType underlying) {
        super(TypeName.Variant);
        this.underlying = underlying;
    }

    public TiType getUnderlying() {
        return underlying;
    }

    public static Builder overStructBuilder() {
        return new Builder();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Variant<");
        if (underlying.isStruct()) {
            underlying.asStruct().printMembers(sb);
        } else {
            underlying.asTuple().printElements(sb);
        }
        sb.append(">");
        return sb.toString();
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VariantType that = (VariantType) o;
        return underlying.equals(that.underlying);
    }

    @Override
    public int hashCode() {
        return Objects.hash(TypeName.Dict, underlying);
    }

    @Override
    public void serializeTo(YsonConsumer ysonConsumer) {
        ysonConsumer.onBeginMap();

        assert TypeName.Variant.wireNameBytes != null;
        YsonConsumer.onKeyedItem(ysonConsumer, KeyNames.TYPE_NAME);
        YsonConsumer.onString(ysonConsumer, TypeName.Variant.wireNameBytes);

        if (underlying.isStruct()) {
            underlying.asStruct().serializeMembers(ysonConsumer);
        } else {
            underlying.asTuple().serializeElements(ysonConsumer);
        }

        ysonConsumer.onEndMap();
    }

    public static class Builder extends MembersBuilder<Builder> {
        public VariantType build() {
            return new VariantType(new StructType(super.members));
        }

        @Override
        Builder self() {
            return this;
        }
    }
}
