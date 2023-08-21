package tech.ytsaurus.core.cypress;

import tech.ytsaurus.core.StringValueEnum;
import tech.ytsaurus.core.StringValueEnumResolver;

/**
 * Type of the cypress node.
 */
public enum CypressNodeType implements StringValueEnum {
    // Static nodes.
    STRING(300, "string_node"),
    INT64(301, "int64_node"),
    UINT64(306, "uint64_node"),
    DOUBLE(302, "double_node"),
    BOOLEAN(305, "boolean_node"),
    MAP(303, "map_node"),
    LIST(304, "list_node"),

    // Dynamic nodes.
    FILE(400, "file"),
    TABLE(401, "table"),
    LINK(417, "link"),
    DOCUMENT(421, "document"),
    REPLICATED_TABLE(425, "replicated_table"),

    // Tablet Manager stuff.
    TABLE_REPLICA(709, "table_replica");

    public static final StringValueEnumResolver<CypressNodeType> R = StringValueEnumResolver.of(CypressNodeType.class);

    private final int protoValue;
    private final String value;

    CypressNodeType(int protoValue, String value) {
        this.protoValue = protoValue;
        this.value = value;
    }

    @Override
    public String value() {
        return value;
    }

    public int protoValue() {
        return protoValue;
    }
}
