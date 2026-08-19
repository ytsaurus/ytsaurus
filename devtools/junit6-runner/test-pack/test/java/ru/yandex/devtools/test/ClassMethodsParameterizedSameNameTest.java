package ru.yandex.devtools.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Slf4j
@TestMethodOrder(MethodOrderer.MethodName.class)
class ClassMethodsParameterizedSameNameTest {

    @ParameterizedTest
    @MethodSource("values")
    @DisplayName("test value: {0}")
    void parameterizedTest(String name) {
        Assertions.assertNotNull(name);
    }

    @ParameterizedTest
    @MethodSource("values2")
    @DisplayName("test value: {0}")
    void parameterizedTestTest(int value) {
        Assertions.assertTrue(value > 0);
    }


    static Object[] values() {
        return new String[]{"v1", "v2"};
    }

    static Object[] values2() {
        return new Integer[]{3, 4};
    }
}
