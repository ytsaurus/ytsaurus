package ru.yandex.devtools.test;

import lombok.extern.java.Log;
import org.junit.jupiter.api.Test;

@Log
public class JulTest {

    @Test
    void test1() {
        log.info("test1");
    }

    @Test
    void test2() {
        log.info("test2");
    }
}
