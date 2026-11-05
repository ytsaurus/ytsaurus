package ru.yandex.devtools.test;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ParameterizedClass
@MethodSource("parameters")
class ParameterizedClassInitializationFailedTest {

    @Parameter
    int value;

    static Stream<Arguments> parameters() {
        if (TestState.isTestOverTest()) {
            throw new IllegalStateException("Unable to provide class parameters");
        }
        return Stream.of(Arguments.of(1));
    }

    @Test
    void test() {
        assertEquals(1, value);
    }
}
