package tech.ytsaurus.skiff.schema;

import java.util.List;

public class ComplexSchema extends SkiffSchema {
    private final List<SkiffSchema> children;

    ComplexSchema(WireType type, List<SkiffSchema> children) {
        super(type);
        this.children = children;
        if (type.isSimpleType()) {
            throw new IllegalArgumentException("TYPE must be complex");
        }
    }

    @Override
    public List<SkiffSchema> getChildren() {
        return children;
    }
}
