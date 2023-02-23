package tech.ytsaurus.typeinfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import tech.ytsaurus.yson.YsonConsumer;

public class StructType extends TiType {
    private final List<Member> members;

    StructType(List<Member> members) {
        super(TypeName.Struct);
        this.members = Collections.unmodifiableList(new ArrayList<>(members));
    }

    StructType(Member... members) {
        super(TypeName.Struct);
        this.members = Collections.unmodifiableList(Arrays.asList(members));
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<Member> getMembers() {
        return members;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Struct<");
        printMembers(sb);
        sb.append(">");
        return sb.toString();
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (o == this) {
            return true;
        } else if (o == null || o.getClass() != this.getClass()) {
            return false;
        }
        StructType that = (StructType) o;
        return members.equals(that.members);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeName, members);
    }

    @Override
    public void serializeTo(YsonConsumer ysonConsumer) {
        ysonConsumer.onBeginMap();

        assert TypeName.Struct.wireNameBytes != null;
        YsonConsumer.onKeyedItem(ysonConsumer, KeyNames.TYPE_NAME);
        YsonConsumer.onString(ysonConsumer, TypeName.Struct.wireNameBytes);

        serializeMembers(ysonConsumer);

        ysonConsumer.onEndMap();
    }

    void printMembers(StringBuilder sb) {
        boolean first = true;
        for (Member member : members) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(Escape.quote(member.getName()));
            sb.append(": ");
            sb.append(member.getType());
        }
    }

    void serializeMembers(YsonConsumer ysonConsumer) {
        YsonConsumer.onKeyedItem(ysonConsumer, KeyNames.MEMBERS);
        ysonConsumer.onBeginList();
        for (Member member : members) {
            ysonConsumer.onListItem();
            ysonConsumer.onBeginMap();

            YsonConsumer.onKeyedItem(ysonConsumer, KeyNames.NAME);
            ysonConsumer.onString(member.getName());

            YsonConsumer.onKeyedItem(ysonConsumer, KeyNames.TYPE);
            member.getType().serializeTo(ysonConsumer);

            ysonConsumer.onEndMap();
        }
        ysonConsumer.onEndList();
    }

    public static class Member {
        private final String name;
        private final TiType type;

        public Member(String name, TiType type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public TiType getType() {
            return type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Member member = (Member) o;
            return name.equals(member.name) && type.equals(member.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type);
        }
    }

    public static class Builder extends MembersBuilder<Builder> {
        public StructType build() {
            return new StructType(members);
        }

        @Override
        Builder self() {
            return this;
        }
    }
}

abstract class MembersBuilder<T extends MembersBuilder<T>> {
    final List<StructType.Member> members = new ArrayList<>();

    abstract T self();

    public T add(String name, TiType type) {
        members.add(new StructType.Member(name, type));
        return self();
    }
}
