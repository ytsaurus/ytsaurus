package ru.yandex.devtools.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@TestMethodOrder(MethodOrderer.MethodName.class)
class ClassMethodsParameterizedMethodDifferentClassTest {

    @ParameterizedTest
    @MethodSource("ru.yandex.devtools.test.ClassMethodsParameterizedMethodSameClassTest#methodParameters")
    void test(int value, String expect) {
        assertEquals(expect, String.valueOf(value));
    }

}
