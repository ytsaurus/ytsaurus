package tech.ytsaurus.core.cypress;

import tech.ytsaurus.core.StringValueEnum;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

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
}
