package tech.ytsaurus.typeinfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.yson.YsonConsumer;

public class TupleType extends TiType {
    private final List<TiType> elements;

    public TupleType(TiType... elements) {
        super(TypeName.Tuple);
        this.elements = Collections.unmodifiableList(Arrays.asList(elements));
    }

    public TupleType(List<TiType> elements) {
        super(TypeName.Tuple);
        this.elements = Collections.unmodifiableList(new ArrayList<>(elements));
    }

    public List<TiType> getElements() {
        return elements;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Tuple<");
        printElements(sb);
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
        TupleType tupleType = (TupleType) o;
        return elements.equals(tupleType.elements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(elements);
    }

    @Override
    public void serializeTo(YsonConsumer ysonConsumer) {
        ysonConsumer.onBeginMap();

        assert TypeName.Tuple.wireNameBytes != null;
        YsonConsumer.onKeyedItem(ysonConsumer, KeyNames.TYPE_NAME);
        YsonConsumer.onString(ysonConsumer, TypeName.Tuple.wireNameBytes);

        serializeElements(ysonConsumer);

        ysonConsumer.onEndMap();
    }

    void printElements(StringBuilder sb) {
        boolean first = true;
        for (TiType type : elements) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(type);
        }
    }

    void serializeElements(YsonConsumer ysonConsumer) {
        YsonConsumer.onKeyedItem(ysonConsumer, KeyNames.ELEMENTS);
        ysonConsumer.onBeginList();
        for (TiType elementType : elements) {
            ysonConsumer.onListItem();
            ysonConsumer.onBeginMap();

            YsonConsumer.onKeyedItem(ysonConsumer, KeyNames.TYPE);
            elementType.serializeTo(ysonConsumer);

            ysonConsumer.onEndMap();
        }
        ysonConsumer.onEndList();
    }
}
