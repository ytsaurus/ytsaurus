package ru.yandex.devtools.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;

//CHECKSTYLE:OFF
@Slf4j
@TestMethodOrder(MethodOrderer.MethodName.class)
class ClassMethodsNoneTest {

    @BeforeAll
    static void beforeAll() {
        log.info("Before All");
    }


    @AfterAll
    static void afterAll() {
        log.info("After all");
    }
}
