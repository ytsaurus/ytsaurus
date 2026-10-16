package ru.yandex.devtools.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Slf4j
@TestMethodOrder(MethodOrderer.MethodName.class)
class ClassMethodDisplayTest {

    @DisplayName("Тест")
    @Test
    void test1() {
        log.info("Just a test 1");
    }

    @DisplayName("Тест")
    @Test
    void test2() {
        log.info("Just a test 2");
    }
}
