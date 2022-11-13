package ru.yandex.yt.ytclient.operations;


import tech.ytsaurus.core.StringValueEnum;
/**
 * @author sankear
 */
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
