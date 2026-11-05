package ru.yandex.devtools.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
@TestMethodOrder(MethodOrderer.MethodName.class)
public class ClassMethodsParameterizedEnumTest {

    @EnumSource(E1.class)
    @ParameterizedTest
    void testEnumArg(E1 e1) {
        assertNotNull(e1);
    }

    enum E1 {
        v1, v2
    }
}
