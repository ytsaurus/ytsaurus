package tech.ytsaurus.skiff;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;

import static tech.ytsaurus.ysontree.YTree.listBuilder;
import static tech.ytsaurus.ysontree.YTree.mapBuilder;

public abstract class SkiffSchema {
    protected final WireType type;
    @Nullable
    private String name;

    SkiffSchema(WireType type) {
        this.type = type;
    }

    public WireType getWireType() {
        return type;
    }

    public SkiffSchema setName(String name) {
        this.name = name;
        return this;
    }

    public Optional<String> getName() {
        return Optional.ofNullable(name);
    }

    public abstract List<SkiffSchema> getChildren();

    public abstract boolean isListSchema();

    public abstract boolean isMapSchema();

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SkiffSchema that = (SkiffSchema) o;
        return Objects.equals(type, that.getWireType()) &&
                Objects.equals(name, that.name) &&
                Objects.equals(getChildren(), that.getChildren());
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name, getChildren());
    }

    public YTreeNode toYTree() {
        YTreeBuilder ytreeBuilder = mapBuilder()
                .key("wire_type").value(type.toString());
        if (name != null) {
            ytreeBuilder.key("name").value(name);
        }
        if (!getChildren().isEmpty()) {
            YTreeBuilder listBuilder = listBuilder();
            for (SkiffSchema child : getChildren()) {
                listBuilder.value(child.toYTree());
            }
            ytreeBuilder.key("children").value(listBuilder.endList().build());
        }
        return ytreeBuilder.endMap().build();
    }

    public static SimpleTypeSchema simpleType(WireType type) {
        return new SimpleTypeSchema(type);
    }

    public static ComplexSchema tuple(List<SkiffSchema> children) {
        return new ComplexSchema(WireType.TUPLE, children);
    }

    public static ComplexSchema variant8(List<SkiffSchema> children) {
        verifyNonEmptyChildren(children, WireType.VARIANT_8);
        return new ComplexSchema(WireType.VARIANT_8, children);
    }

    public static ComplexSchema variant16(List<SkiffSchema> children) {
        verifyNonEmptyChildren(children, WireType.VARIANT_16);
        return new ComplexSchema(WireType.VARIANT_16, children);
    }

    public static ComplexSchema repeatedVariant8(List<SkiffSchema> children) {
        verifyNonEmptyChildren(children, WireType.REPEATED_VARIANT_8);
        return new ComplexSchema(WireType.REPEATED_VARIANT_8, children);
    }

    public static ComplexSchema repeatedVariant16(List<SkiffSchema> children) {
        verifyNonEmptyChildren(children, WireType.REPEATED_VARIANT_16);
        return new ComplexSchema(WireType.REPEATED_VARIANT_16, children);
    }

    public static SimpleTypeSchema nothing() {
        return new SimpleTypeSchema(WireType.NOTHING);
    }

    private static void verifyNonEmptyChildren(List<SkiffSchema> children, WireType type) {
        if (children.isEmpty()) {
            throw new IllegalArgumentException("\"" + type + "\"must have at least one child");
        }
    }
}
