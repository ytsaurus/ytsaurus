package tech.ytsaurus.skiff.schema;

import java.util.Collections;
import java.util.List;

public class SimpleTypeSchema extends SkiffSchema {
    SimpleTypeSchema(WireType type) {
        super(type);
        if (!type.isSimpleType()) {
            throw new IllegalArgumentException("TYPE must be simple");
        }
    }

    @Override
    public List<SkiffSchema> getChildren() {
        return Collections.emptyList();
    }
}
