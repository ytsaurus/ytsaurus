package ru.yandex.devtools.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import ru.yandex.devtools.test.annotations.YaIgnore;

@Slf4j
@TestMethodOrder(MethodOrderer.MethodName.class)
class ClassMethodSingleYaIgnoredTest {

    @Test
    @YaIgnore("Flaky on macOS")
    void test() {
        log.info("Just a test");
    }
}
