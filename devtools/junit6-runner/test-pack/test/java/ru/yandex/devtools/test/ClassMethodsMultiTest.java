package ru.yandex.devtools.test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@TestMethodOrder(MethodOrderer.MethodName.class)
class ClassMethodsMultiTest {

    private static final AtomicInteger INDEX = new AtomicInteger(4);

    private final Random random = new Random();
    private int value;

    @BeforeEach
    void init() {
        value = random.nextInt(1000);
    }

    @Test
    void test1() {
        assertTrue(value >= 0);
    }

    @Test
    void test2() {
        assertTrue(value >= 0);
    }

    @Test
    public void canonTest() {
        try {
            if (TestState.isYaMakeTest()) {
                Canonizer.canonize("test");
            }
        } catch (RuntimeException e) {
            new Exception("For " + Thread.currentThread().getName()).printStackTrace();
            throw e;
        }
    }

    @Test
    public void metricsTest() {
        if (TestState.isYaMakeTest()) {
            Metrics.set("a", 1);
            Metrics.set("b", 0);
        }
    }

    @Test
    public void linksTest() {
        if (TestState.isYaMakeTest()) {
            Links.set("link", "http://yandex.ru");
        }
    }

    @Test
    @Disabled("for demonstration purposes")
    public void skippedTest() {
        // not executed
    }

    @Test
    public void failedTest() {
        if (TestState.isTestOverTest()) {
            throw new RuntimeException();
        }
    }

    @ParameterizedTest
    @MethodSource("flakySource")
    public void flakyTest(long value) {
        assertTrue(value > 0);
    }

    static Object[] flakySource() {
        return new Object[]{Long.valueOf(TestState.isTestOverTest() ? INDEX.getAndIncrement() : 6655544888L)};
    }

}
