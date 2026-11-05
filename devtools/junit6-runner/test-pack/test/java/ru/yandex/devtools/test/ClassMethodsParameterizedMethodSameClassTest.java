package ru.yandex.devtools.test;

import java.util.stream.IntStream;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@TestMethodOrder(MethodOrderer.MethodName.class)
class ClassMethodsParameterizedMethodSameClassTest {

    @ParameterizedTest
    @MethodSource("methodParameters")
    void test(int value, String expect) {
        assertEquals(expect, String.valueOf(value));
    }

    static Object[][] methodParameters() {
        return IntStream.range(0, 4)
                .mapToObj(i -> new Object[]{i, String.valueOf(i)})
                .toArray(Object[][]::new);
    }
}
