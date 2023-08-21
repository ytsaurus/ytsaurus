package tech.ytsaurus.core;


import java.util.HashMap;
import java.util.Map;

public class StringValueEnumResolver<T extends Enum<T> & StringValueEnum> {
    private final Class<T> enumClass;
    private final Map<String, T> valuesByName = new HashMap<>();

    public StringValueEnumResolver(Class<T> enumClass) {
        this.enumClass = enumClass;
        for (T value : enumClass.getEnumConstants()) {
            valuesByName.put(value.value(), value);
        }
    }

    public T fromName(String name) {
        if (name == null) {
            throw new IllegalArgumentException("value must not be null; for enum of type " + enumClass.getName());
        }
        if (valuesByName.containsKey(name)) {
            return valuesByName.get(name);
        }
        throw new IllegalArgumentException("no enum of type " + enumClass.getName() + " found with name " + name);
    }

    public static <T extends Enum<T> & StringValueEnum> StringValueEnumResolver<T> of(Class<T> enumClass) {
        return new StringValueEnumResolver<>(enumClass);
    }
}
