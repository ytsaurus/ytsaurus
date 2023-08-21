package tech.ytsaurus.skiff;

import java.util.List;

public class ComplexSchema extends SkiffSchema {
    private final List<SkiffSchema> children;
    private final boolean isListSchema;
    private final boolean isMapSchema;

    ComplexSchema(WireType type, List<SkiffSchema> children) {
        super(type);
        this.children = children;
        this.isListSchema = type == WireType.REPEATED_VARIANT_8 &&
                getChildren().size() == 1 &&
                getChildren().get(0).type != WireType.NOTHING;
        this.isMapSchema = type == WireType.REPEATED_VARIANT_8 &&
                getChildren().size() == 1 &&
                getChildren().get(0).type == WireType.TUPLE &&
                getChildren().get(0).getChildren().size() == 2 &&
                getChildren().get(0).getChildren().get(0).type != WireType.NOTHING &&
                getChildren().get(0).getChildren().get(1).type != WireType.NOTHING;
        if (type.isSimpleType()) {
            throw new IllegalArgumentException("TYPE must be complex");
        }
    }

    @Override
    public List<SkiffSchema> getChildren() {
        return children;
    }

    @Override
    public boolean isListSchema() {
        return isListSchema;
    }

    @Override
    public boolean isMapSchema() {
        return isMapSchema;
    }
}
