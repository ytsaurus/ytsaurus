package ru.yandex.devtools.test;

import java.util.Random;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestMethodOrder;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@TestMethodOrder(MethodOrderer.MethodName.class)
class ClassMethodsRepeatDisplayTest {

    private final Random random = new Random();
    private int value;

    @BeforeEach
    void init() {
        value = random.nextInt(1000);
    }

    @DisplayName("Тест с повторами")
    @RepeatedTest(2)
    void testDisplay() {
        assertTrue(value >= 0);
    }

    @DisplayName("Тест с повторами и параметрами")
    @RepeatedTest(value = 2, name = "тест [{currentRepetition}]")
    void testDisplayAndParameters() {
        assertTrue(value >= 0);
    }

}
