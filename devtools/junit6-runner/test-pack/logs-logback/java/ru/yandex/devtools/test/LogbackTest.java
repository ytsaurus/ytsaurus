package ru.yandex.devtools.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class LogbackTest {

    @Test
    void test1() {
        log.info("test1");
    }

    @Test
    void test2() {
        log.info("test2");
    }
}
