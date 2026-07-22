package ru.yandex.devtools.test;

import java.util.Random;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestMethodOrder;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@TestMethodOrder(MethodOrderer.MethodName.class)
class ClassMethodsRepeatTest {

    private final Random random = new Random();
    private int value;

    @BeforeEach
    void init() {
        value = random.nextInt(1000);
    }

    @RepeatedTest(3)
    void test() {
        assertTrue(value >= 0);
    }

}
