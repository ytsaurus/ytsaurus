package ru.yandex.devtools.test;

import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullSource;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@TestMethodOrder(MethodOrderer.MethodName.class)
public class ClassMethodsParameterizedEmptyTest {

    @EmptySource
    @ParameterizedTest
    void testEmptyArg(List<String> names) {
        assertTrue(names.isEmpty());
    }

    @NullSource
    @ParameterizedTest
    void testNullArg(String names) {
        assertNull(names);
    }
}
