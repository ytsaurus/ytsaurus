package ru.yandex.devtools.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Slf4j
@TestMethodOrder(MethodOrderer.MethodName.class)
class ClassFailedTest {

    @BeforeAll
    static void initAll() {
        if (TestState.isTestOverTest()) {
            throw new RuntimeException();
        }
    }

    @Test
    void test() {
        log.info("Just a test");
    }
}
