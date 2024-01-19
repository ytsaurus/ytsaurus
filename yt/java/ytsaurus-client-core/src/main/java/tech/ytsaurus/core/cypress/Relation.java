package tech.ytsaurus.core.cypress;

import tech.ytsaurus.core.StringValueEnum;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * Relation for table ranges.
 */
@NonNullApi
@NonNullFields
public enum Relation implements StringValueEnum {
    LESS("<"),
    LESS_OR_EQUAL("<="),
    GREATER(">"),
    GREATER_OR_EQUAL(">=");

    private final String value;

    Relation(String value) {
        this.value = value;
    }

    @Override
    public String value() {
        return value;
    }

    public static Relation fromValue(String value) {
        for (Relation relation : Relation.values()) {
            if (relation.value.equals(value)) {
                return relation;
            }
        }
        throw new IllegalArgumentException("Unknown relation value: " + value);
    }
}
