package ru.yandex.devtools.test;

import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.MethodName.class)
class ClassTwoTest {

    private int value;

    @BeforeEach
    void init() {
        value =  new Random().nextInt(1000);
    }

    @Test
    void test1() {
        assertTrue(value >= 0);
    }

    @Test
    void test2() {
        assertTrue(value >= 0);
    }
}
