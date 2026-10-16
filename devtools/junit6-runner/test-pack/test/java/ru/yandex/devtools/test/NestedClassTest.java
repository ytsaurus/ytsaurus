package ru.yandex.devtools.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@TestClassOrder(ClassOrderer.ClassName.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
class NestedClassTest {

    @Nested
    class SingleTest {
        @Test
        void test() {
            log.info("Just a test");
        }

    }

    @Nested
    class DisplayTest {
        @DisplayName("Тест")
        @Test
        void test1() {
            log.info("Just a test 1");
        }

        @DisplayName("Тест")
        @Test
        void test2() {
            log.info("Just a test 2");
        }
    }

    @Nested
    class ParameterizedConstantDisplayTest {
        @DisplayName("Тест на проверку отображения")
        @ParameterizedTest
        @ValueSource(ints = {10, 5, 66})
        void testDisplay(int value) {
            assertTrue(value >= 0);
        }

        @DisplayName("Тест на проверку отображения и параметров")
        @ParameterizedTest(name = "check [{0}]")
        @ValueSource(ints = {10, 5, 66})
        void testDisplayAndParameters(int value) {
            assertTrue(value >= 0);
        }
    }

    @Nested
    class ParameterizedConstantTest {

        @ParameterizedTest
        @ValueSource(ints = {1, 2, 3})
        void test(int value) {
            assertTrue(value >= 0);
        }

    }
}
