package ru.yandex.abi;

import java.util.function.Function;

public final class JavaApi {
    public String message() {
        return new PublicApi().message();
    }

    public static String apply(String value, Function<String, String> transform) {
        return transform.apply(value);
    }
}
