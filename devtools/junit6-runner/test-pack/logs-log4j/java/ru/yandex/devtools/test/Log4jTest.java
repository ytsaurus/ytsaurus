package ru.yandex.devtools.test;

import lombok.extern.log4j.Log4j;
import org.junit.jupiter.api.Test;

@Log4j
public class Log4jTest {

    @Test
    void test1() {
        log.info("test1");
    }

    @Test
    void test2() {
        log.info("test2");
    }
}
