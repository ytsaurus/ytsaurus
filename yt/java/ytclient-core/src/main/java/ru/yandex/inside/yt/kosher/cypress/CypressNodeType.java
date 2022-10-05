package ru.yandex.inside.yt.kosher.cypress;

import ru.yandex.inside.yt.kosher.common.StringValueEnum;
import ru.yandex.inside.yt.kosher.common.StringValueEnumResolver;

/**
 * ENodeType analog
 *
 * @author sankear
 * @see <a href="https://a.yandex-team.ru/arc/trunk/arcadia/mapreduce/yt/interface/client_method_options.h?rev=3275008#L9">ENodeType</a>
 */
public enum CypressNodeType implements StringValueEnum {

    STRING("string_node"),
    INT64("int64_node"),
    UINT64("uint64_node"),
    DOUBLE("double_node"),
    BOOLEAN("boolean_node"),
    MAP("map_node"),
    LIST("list_node"),
    FILE("file"),
    TABLE("table"),
    DOCUMENT("document"),
    REPLICATED_TABLE("replicated_table"),
    TABLE_REPLICA("table_replica");

    public static final StringValueEnumResolver<CypressNodeType> R = StringValueEnumResolver.of(CypressNodeType.class);

    private final String value;

    CypressNodeType(String value) {
        this.value = value;
    }

    @Override
    public String value() {
        return value;
    }
}
