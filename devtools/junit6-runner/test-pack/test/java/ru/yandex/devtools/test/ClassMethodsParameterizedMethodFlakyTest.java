package ru.yandex.devtools.test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@TestMethodOrder(MethodOrderer.MethodName.class)
class ClassMethodsParameterizedMethodFlakyTest {

    private static final AtomicInteger INDEX = new AtomicInteger(4);

    @ParameterizedTest
    @MethodSource("flakyMethodParameters")
    void test(long value, String expect) {
        assertEquals(expect, String.valueOf(value));
    }

    static Object[][] flakyMethodParameters() {
        return LongStream.range(0, 4)
                .map(i -> TestState.isTestOverTest() ? INDEX.getAndIncrement() : i)
                .mapToObj(i -> new Object[]{i, String.valueOf(i)})
                .toArray(Object[][]::new);
    }
}
