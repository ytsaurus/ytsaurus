package tech.ytsaurus.client.operations;


import tech.ytsaurus.core.StringValueEnum;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * @author sankear
 */
@NonNullApi
@NonNullFields
public enum MergeMode implements StringValueEnum {
    UNORDERED("unordered"),
    ORDERED("ordered"),
    SORTED("sorted");

    private final String value;

    MergeMode(String value) {
        this.value = value;
    }

    @Override
    public String value() {
        return value;
    }

}
