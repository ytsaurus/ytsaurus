package ru.yandex.devtools.test;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.MethodName.class)
class ParameterizedMethodDuplicateTest {

    @DisplayName("Тест на проверку отображения и параметров")
    @ParameterizedTest(name = "check {0}")
    @ValueSource(ints = {10, 5, 66})
    void testDisplayAndParameters(int value) {
        assertTrue(value >= 0);
    }

    @DisplayName("Тест на проверку отображения и параметров")
    @ParameterizedTest(name = "check {0}")
    @ValueSource(ints = {10, 5, 66})
    void testDisplayAndParameterDuplicate(int value) {
        assertTrue(value >= 0);
    }
}
