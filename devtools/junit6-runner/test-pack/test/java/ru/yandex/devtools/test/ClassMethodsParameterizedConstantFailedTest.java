package ru.yandex.devtools.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Slf4j
@TestMethodOrder(MethodOrderer.MethodName.class)
class ClassMethodsParameterizedConstantFailedTest {

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void test(int value) {
        if (TestState.isTestOverTest()) {
            throw new RuntimeException();
        }
    }

}
