package ru.yandex.devtools.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestMethodOrder;

@Slf4j
@TestMethodOrder(MethodOrderer.MethodName.class)
class ClassMethodsRepeatFailedTest {

    @RepeatedTest(5)
    void test() {
        if (TestState.isTestOverTest()) {
            throw new RuntimeException();
        }
    }

}
