package ru.yandex.devtools.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@TestClassOrder(ClassOrderer.ClassName.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@ParameterizedClass
@ValueSource(strings = {"test1", "test2"})
class ParameterizedClassTest {

    @Parameter
    String strParam;

    @Test
    void test() {
        log.info("Just a test: {}", strParam);

        assertNotNull(strParam);
    }

    @DisplayName("Тест")
    @Test
    void test1() {
        log.info("Just a test 1: {}", strParam);

        assertNotNull(strParam);
    }

    @DisplayName("Тест")
    @Test
    void test2() {
        log.info("Just a test 2: {}", strParam);

        assertNotNull(strParam);
    }

    @DisplayName("Тест на проверку отображения")
    @ParameterizedTest
    @ValueSource(ints = {10, 5, 66})
    void testDisplay(int value) {
        assertTrue(value >= 0);

        assertNotNull(strParam);
    }

    @DisplayName("Тест на проверку отображения и параметров")
    @ParameterizedTest(name = "check [{0}]")
    @ValueSource(ints = {10, 5, 66})
    void testDisplayAndParameters(int value) {
        assertTrue(value >= 0);

        assertNotNull(strParam);
    }


    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void test(int value) {
        assertTrue(value >= 0);

        assertNotNull(strParam);
    }

    @Nested
    class SingleTest {
        @Test
        void test() {
            log.info("Just a test: {}", strParam);

            assertNotNull(strParam);
        }

    }

    @Nested
    class DisplayTest {
        @DisplayName("Тест")
        @Test
        void test1() {
            log.info("Just a test 1: {}", strParam);

            assertNotNull(strParam);
        }

        @DisplayName("Тест")
        @Test
        void test2() {
            log.info("Just a test 2: {}", strParam);

            assertNotNull(strParam);
        }
    }

    @Nested
    class ParameterizedConstantDisplayTest {
        @DisplayName("Тест на проверку отображения")
        @ParameterizedTest
        @ValueSource(ints = {10, 5, 66})
        void testDisplay(int value) {
            assertTrue(value >= 0);

            assertNotNull(strParam);
        }

        @DisplayName("Тест на проверку отображения и параметров")
        @ParameterizedTest(name = "check [{0}]")
        @ValueSource(ints = {10, 5, 66})
        void testDisplayAndParameters(int value) {
            assertTrue(value >= 0);

            assertNotNull(strParam);
        }
    }

    @Nested
    class ParameterizedConstantTest {

        @ParameterizedTest
        @ValueSource(ints = {1, 2, 3})
        void test(int value) {
            assertTrue(value >= 0);

            assertNotNull(strParam);
        }

    }
}
