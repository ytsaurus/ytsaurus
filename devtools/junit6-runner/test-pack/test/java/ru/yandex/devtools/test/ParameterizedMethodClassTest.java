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
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@TestClassOrder(ClassOrderer.ClassName.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@ParameterizedClass
@MethodSource("classParameters")
class ParameterizedMethodClassTest {

    @Parameter(0)
    String strParam;

    @Parameter(1)
    int intParam;

    @Test
    void test() {
        log.info("Just a test: {}, {}", strParam, intParam);

        assertNotNull(strParam);
        assertTrue(intParam > 0);
    }

    @DisplayName("Тест")
    @Test
    void test1() {
        log.info("Just a test 1: {}, {}", strParam, intParam);

        assertNotNull(strParam);
        assertTrue(intParam > 0);
    }

    @DisplayName("Тест")
    @Test
    void test2() {
        log.info("Just a test 2: {}, {}", strParam, intParam);

        assertNotNull(strParam);
        assertTrue(intParam > 0);
    }

    @DisplayName("Тест на проверку отображения")
    @ParameterizedTest
    @ValueSource(ints = {10, 5, 66})
    void testDisplay(int value) {
        assertTrue(value >= 0);

        assertNotNull(strParam);
        assertTrue(intParam > 0);
    }

    @DisplayName("Тест на проверку отображения и параметров")
    @ParameterizedTest(name = "check [{0}]")
    @ValueSource(ints = {10, 5, 66})
    void testDisplayAndParameters(int value) {
        assertTrue(value >= 0);

        assertNotNull(strParam);
        assertTrue(intParam > 0);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void test(int value) {
        assertTrue(value >= 0);

        assertNotNull(strParam);
        assertTrue(intParam > 0);
    }


    @Nested
    class SingleTest {
        @Test
        void test() {
            log.info("Just a test: {}, {}", strParam, intParam);

            assertNotNull(strParam);
            assertTrue(intParam > 0);
        }

    }

    @Nested
    class DisplayTest {
        @DisplayName("Тест")
        @Test
        void test1() {
            log.info("Just a test 1: {}, {}", strParam, intParam);

            assertNotNull(strParam);
            assertTrue(intParam > 0);
        }

        @DisplayName("Тест")
        @Test
        void test2() {
            log.info("Just a test 2: {}, {}", strParam, intParam);

            assertNotNull(strParam);
            assertTrue(intParam > 0);
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
            assertTrue(intParam > 0);
        }

        @DisplayName("Тест на проверку отображения и параметров")
        @ParameterizedTest(name = "check [{0}]")
        @ValueSource(ints = {10, 5, 66})
        void testDisplayAndParameters(int value) {
            assertTrue(value >= 0);

            assertNotNull(strParam);
            assertTrue(intParam > 0);
        }
    }

    @Nested
    class ParameterizedConstantTest {

        @ParameterizedTest
        @ValueSource(ints = {1, 2, 3})
        void test(int value) {
            assertTrue(value >= 0);

            assertNotNull(strParam);
            assertTrue(intParam > 0);
        }

    }

    static Object[][] classParameters() {
        return new Object[][]{
                {"test1", 1},
                {"test2", 2}
        };
    }

}
